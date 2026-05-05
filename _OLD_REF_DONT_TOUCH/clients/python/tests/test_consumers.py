"""Construction-time tests for high-level consumers.

These tests cover validation, lease-policy parsing, and lease-freshness
detection. The full listen+commit loop sits behind the SQL extension and
is exercised by the demo / integration suite.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest

from ducklake import DuckLake
from ducklake_cdc import (
    BaseDDLTickSink,
    BaseDMLTickSink,
    DDLConsumer,
    DDLTickBatch,
    DMLConsumer,
    DMLTickBatch,
    StdoutDDLSink,
    StdoutDMLSink,
)
from ducklake_cdc.consumers.base import _AdaptiveSnapshotWindow, _lease_is_alive
from ducklake_cdc.lowlevel import ConsumerListEntry, DDLTickRow, DMLTickRow


class _FakeLake:
    """Stand-in :class:`ducklake.DuckLake` that never issues SQL.

    Construction is the only thing under test in this file, so we never
    need a working ``sql`` method.
    """

    alias = "lake"

    def sql(self, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("DuckLake.sql should not be called in these tests")


def _fake_lake() -> DuckLake:
    return cast(DuckLake, _FakeLake())


class _FakeClient:
    def __init__(self) -> None:
        self.list_calls = 0
        self.created = 0
        self.create_failures = 0

    def cdc_list_consumers(self) -> list[ConsumerListEntry]:
        self.list_calls += 1
        return []

    def cdc_dml_consumer_create(self, *_args: Any, **_kwargs: Any) -> None:
        if self.create_failures > 0:
            self.create_failures -= 1
            raise RuntimeError("transient setup failure")
        self.created += 1

    def cdc_consumer_force_release(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("force_release should not be called")

    def cdc_consumer_drop(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("drop should not be called")


class _ReplaceClient(_FakeClient):
    def __init__(self) -> None:
        super().__init__()
        self.force_releases = 0
        self.drops = 0

    def cdc_list_consumers(self) -> list[ConsumerListEntry]:
        raise AssertionError("replace setup should not list every consumer")

    def cdc_consumer_force_release(self, *_args: Any, **_kwargs: Any) -> None:
        self.force_releases += 1

    def cdc_consumer_drop(self, *_args: Any, **_kwargs: Any) -> None:
        self.drops += 1


class _DMLTickSink(BaseDMLTickSink):
    def write(self, batch: DMLTickBatch, ctx: Any) -> None:
        del batch, ctx


class _DDLTickSink(BaseDDLTickSink):
    def write(self, batch: DDLTickBatch, ctx: Any) -> None:
        del batch, ctx


def test_dml_consumer_requires_at_least_one_sink() -> None:
    with pytest.raises(ValueError, match="at least one sink"):
        DMLConsumer(_fake_lake(), "name", table="t", sinks=[])


def test_ddl_consumer_requires_at_least_one_sink() -> None:
    with pytest.raises(ValueError, match="at least one sink"):
        DDLConsumer(_fake_lake(), "name", sinks=[])


def test_dml_consumer_requires_non_empty_name() -> None:
    with pytest.raises(ValueError, match="non-empty name"):
        DMLConsumer(_fake_lake(), "", table="t", mode="changes", sinks=[StdoutDMLSink()])


def test_ddl_consumer_requires_non_empty_name() -> None:
    with pytest.raises(ValueError, match="non-empty name"):
        DDLConsumer(_fake_lake(), "", mode="changes", sinks=[StdoutDDLSink()])


def test_dml_consumer_defaults_to_tick_mode() -> None:
    consumer = DMLConsumer(_fake_lake(), "ticks", table="t", sinks=[_DMLTickSink()])

    assert consumer._mode == "ticks"


def test_ddl_consumer_defaults_to_tick_mode() -> None:
    consumer = DDLConsumer(_fake_lake(), "ticks", sinks=[_DDLTickSink()])

    assert consumer._mode == "ticks"


def test_consumer_rejects_sink_that_does_not_match_mode() -> None:
    with pytest.raises(TypeError, match="dml_ticks"):
        DMLConsumer(_fake_lake(), "n", table="t", sinks=[StdoutDMLSink()])
    with pytest.raises(TypeError, match="ddl_ticks"):
        DDLConsumer(_fake_lake(), "n", sinks=[StdoutDDLSink()])


def test_dml_tick_mode_builds_tick_batch() -> None:
    consumer = DMLConsumer(_fake_lake(), "n", table="t", sinks=[_DMLTickSink()])

    batch = consumer._build_batch(
        [
            DMLTickRow(
                consumer_name="n",
                start_snapshot=10,
                end_snapshot=12,
                snapshot_id=11,
                snapshot_time=datetime(2026, 1, 1, tzinfo=UTC),
                schema_version=4,
                table_ids=(42,),
            )
        ]
    )

    assert isinstance(batch, DMLTickBatch)
    assert batch.start_snapshot == 10
    assert batch.end_snapshot == 12
    assert batch.snapshot_ids == (11,)
    assert batch.ticks[0].table_ids == (42,)


def test_ddl_tick_mode_builds_tick_batch() -> None:
    consumer = DDLConsumer(_fake_lake(), "n", sinks=[_DDLTickSink()])

    batch = consumer._build_batch(
        [
            DDLTickRow(
                consumer_name="n",
                start_snapshot=2,
                end_snapshot=2,
                snapshot_id=2,
                snapshot_time=datetime(2026, 1, 1, tzinfo=UTC),
                schema_version=7,
            )
        ]
    )

    assert isinstance(batch, DDLTickBatch)
    assert batch.start_snapshot == 2
    assert batch.end_snapshot == 2
    assert batch.snapshot_ids == (2,)
    assert batch.ticks[0].schema_version == 7


def test_dml_consumer_rejects_unknown_lease_policy() -> None:
    with pytest.raises(ValueError, match="lease_policy"):
        DMLConsumer(
            _fake_lake(),
            "n",
            table="t",
            mode="changes",
            sinks=[StdoutDMLSink()],
            lease_policy="grab",  # type: ignore[arg-type]
        )


def test_dml_consumer_rejects_negative_lease_wait_timeout() -> None:
    with pytest.raises(ValueError, match="lease_wait_timeout"):
        DMLConsumer(
            _fake_lake(),
            "n",
            table="t",
            mode="changes",
            sinks=[StdoutDMLSink()],
            lease_wait_timeout=-1.0,
        )


def test_dml_consumer_requires_exactly_one_table_input() -> None:
    """The contract is one DML consumer = one table; the bind enforces
    exactly one of `table` / `table_id`. We mirror that in the
    constructor so the error surfaces synchronously before sinks open.
    """

    with pytest.raises(ValueError, match="exactly one of table"):
        DMLConsumer(_fake_lake(), "n", mode="changes", sinks=[StdoutDMLSink()])
    with pytest.raises(ValueError, match="exactly one of table"):
        DMLConsumer(
            _fake_lake(),
            "n",
            table="t",
            table_id=42,
            mode="changes",
            sinks=[StdoutDMLSink()],
        )


def test_consumer_enter_applies_retry_policy_to_setup() -> None:
    client = _FakeClient()
    client.create_failures = 1
    attempts = 0

    def retry(operation: Any) -> Any:
        nonlocal attempts
        while True:
            attempts += 1
            try:
                return operation()
            except RuntimeError:
                if attempts >= 2:
                    raise

    consumer = DMLConsumer(
        _fake_lake(),
        "retry-setup",
        table="demo_schema.events",
        mode="changes",
        sinks=[StdoutDMLSink()],
        client=cast(Any, client),
        retry=retry,
    )

    with consumer:
        pass

    assert attempts == 2
    assert client.list_calls == 1
    assert client.created == 1


def test_replace_setup_avoids_consumer_listing() -> None:
    client = _ReplaceClient()
    consumer = DMLConsumer(
        _fake_lake(),
        "replace-setup",
        table="demo_schema.events",
        mode="changes",
        on_exists="replace",
        sinks=[StdoutDMLSink()],
        client=cast(Any, client),
    )

    with consumer:
        pass

    assert client.force_releases == 1
    assert client.drops == 1
    assert client.created == 1


def test_adaptive_snapshot_window_starts_below_ceiling() -> None:
    window = _AdaptiveSnapshotWindow(100)

    assert window.current == 8


def test_adaptive_snapshot_window_grows_on_fast_full_window() -> None:
    window = _AdaptiveSnapshotWindow(100)

    window.observe_batch(row_count=100, snapshot_span=8, listen_elapsed_ms=20.0)

    assert window.current == 16


def test_adaptive_snapshot_window_shrinks_on_slow_listen() -> None:
    window = _AdaptiveSnapshotWindow(100)
    window.observe_batch(row_count=100, snapshot_span=8, listen_elapsed_ms=20.0)
    assert window.current == 16

    window.observe_batch(row_count=100, snapshot_span=16, listen_elapsed_ms=200.0)

    assert window.current == 8


def test_adaptive_snapshot_window_shrinks_on_very_large_batch() -> None:
    window = _AdaptiveSnapshotWindow(100)
    window.observe_batch(row_count=100, snapshot_span=8, listen_elapsed_ms=20.0)
    assert window.current == 16

    window.observe_batch(row_count=12_000, snapshot_span=1, listen_elapsed_ms=20.0)

    assert window.current == 8


def test_adaptive_snapshot_window_does_not_shrink_on_typical_dense_batch() -> None:
    window = _AdaptiveSnapshotWindow(100)
    window.observe_batch(row_count=100, snapshot_span=8, listen_elapsed_ms=20.0)
    assert window.current == 16

    window.observe_batch(row_count=900, snapshot_span=8, listen_elapsed_ms=20.0)

    assert window.current == 16


def test_adaptive_snapshot_window_shrinks_on_empty_listen() -> None:
    window = _AdaptiveSnapshotWindow(100)

    window.observe_empty()

    assert window.current == 4


def test_lease_is_alive_treats_missing_token_as_free() -> None:
    entry = ConsumerListEntry(
        consumer_name="x",
        consumer_kind="dml",
        consumer_id=1,
        owner_token=None,
    )

    assert _lease_is_alive(entry) is False


def test_lease_is_alive_treats_recent_heartbeat_as_held() -> None:
    from uuid import uuid4

    entry = ConsumerListEntry(
        consumer_name="x",
        consumer_kind="dml",
        consumer_id=1,
        owner_token=uuid4(),
        owner_heartbeat_at=datetime.now(UTC) - timedelta(seconds=1),
        lease_interval_seconds=10,
    )

    assert _lease_is_alive(entry) is True


def test_lease_is_alive_treats_stale_heartbeat_as_free() -> None:
    from uuid import uuid4

    entry = ConsumerListEntry(
        consumer_name="x",
        consumer_kind="dml",
        consumer_id=1,
        owner_token=uuid4(),
        owner_heartbeat_at=datetime.now(UTC) - timedelta(seconds=120),
        lease_interval_seconds=10,
    )

    assert _lease_is_alive(entry) is False
