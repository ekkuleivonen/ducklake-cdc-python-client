from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

import pytest

from ducklake import DuckLake
from ducklake_cdc import (
    BaseDMLTickSink,
    ConsumerSpawner,
    DMLConsumer,
    DMLTick,
    DMLTickBatch,
    SinkContext,
)


class _Lake:
    alias = "lake"

    def sql(self, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("DuckLake.sql should not be called")


class _App:
    def __init__(self) -> None:
        self.consumers: list[DMLConsumer] = []
        self.fail_on_add = False

    def add_consumer(self, consumer: DMLConsumer) -> None:
        if self.fail_on_add:
            raise ValueError("duplicate")
        self.consumers.append(consumer)


class _DMLTickSink(BaseDMLTickSink):
    def write(self, batch: DMLTickBatch, ctx: SinkContext) -> None:
        del batch, ctx


def _consumer(name: str) -> DMLConsumer:
    return DMLConsumer(
        cast(DuckLake, _Lake()),
        name,
        table="main.orders",
        sinks=[_DMLTickSink()],
    )


def _batch() -> DMLTickBatch:
    return DMLTickBatch(
        consumer_name="ticks",
        batch_id="ticks/1-2",
        start_snapshot=1,
        end_snapshot=2,
        snapshot_ids=(1, 2),
        received_at=datetime(2026, 1, 1, tzinfo=UTC),
        ticks=(
            DMLTick(
                snapshot_id=1,
                snapshot_time=datetime(2026, 1, 1, tzinfo=UTC),
                schema_version=1,
                table_ids=(42,),
            ),
            DMLTick(
                snapshot_id=2,
                snapshot_time=datetime(2026, 1, 1, tzinfo=UTC),
                schema_version=1,
                table_ids=(42,),
            ),
        ),
    )


def _ctx(batch: DMLTickBatch) -> SinkContext:
    return SinkContext(
        consumer_name=batch.consumer_name,
        batch_id=batch.batch_id,
        _heartbeat=lambda: None,
    )


def test_spawner_accepts_any_consumer_mode() -> None:
    app = _App()
    spawner = ConsumerSpawner(app=cast(Any, app), on_event=lambda _item: None)

    consumer = DMLConsumer(
        cast(DuckLake, _Lake()),
        "source",
        table="main.orders",
        sinks=[spawner],
    )

    assert consumer._mode == "ticks"


def test_spawner_adds_single_consumer_from_item_hook() -> None:
    app = _App()
    spawner = ConsumerSpawner(
        app=cast(Any, app),
        on_event=lambda item: _consumer(f"spawned-{item.snapshot_id}"),
    )
    batch = _batch()

    spawner.write(batch, _ctx(batch))

    assert [consumer.name for consumer in app.consumers] == ["spawned-1", "spawned-2"]


def test_spawner_adds_iterable_from_three_argument_hook() -> None:
    app = _App()
    seen: list[tuple[int, str, str]] = []

    def build(item: DMLTick, batch: DMLTickBatch, ctx: SinkContext) -> list[DMLConsumer]:
        seen.append((item.snapshot_id, batch.batch_id, ctx.consumer_name))
        return [_consumer(f"a-{item.snapshot_id}"), _consumer(f"b-{item.snapshot_id}")]

    spawner = ConsumerSpawner(app=cast(Any, app), on_event=build)
    batch = _batch()

    spawner.write(batch, _ctx(batch))

    assert seen == [(1, "ticks/1-2", "ticks"), (2, "ticks/1-2", "ticks")]
    assert [consumer.name for consumer in app.consumers] == [
        "a-1",
        "b-1",
        "a-2",
        "b-2",
    ]


def test_spawner_lets_app_duplicate_errors_raise() -> None:
    app = _App()
    app.fail_on_add = True
    spawner = ConsumerSpawner(app=cast(Any, app), on_event=lambda _item: _consumer("dup"))

    with pytest.raises(ValueError, match="duplicate"):
        spawner.write(_batch(), _ctx(_batch()))


def test_spawner_rejects_bad_hook_signature() -> None:
    def bad() -> None:
        return None

    with pytest.raises(TypeError, match="at least"):
        ConsumerSpawner(app=cast(Any, _App()), on_event=bad)
