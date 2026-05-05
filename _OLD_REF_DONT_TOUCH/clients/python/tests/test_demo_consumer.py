from __future__ import annotations

import sys
from argparse import Namespace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "demo"))

import consumer as demo_consumer  # noqa: E402
from analytics import DemoStats  # noqa: E402

from ducklake_cdc import DdlEventKind, DdlObjectKind, SchemaChange  # noqa: E402


class _FakeLake:
    def __init__(self) -> None:
        self.loaded_paths: list[Path] = []
        self.closed = False

    def load_extension(self, *, path: Path) -> None:
        self.loaded_paths.append(path)

    def close(self) -> None:
        self.closed = True


def test_consumer_name_for_table_uses_table_id_when_present() -> None:
    assert (
        demo_consumer._consumer_name_for_table(
            table_id=42,
            table_name="demo_schema_01.events_01",
            consumers_per_table=3,
            consumer_index=1,
        )
        == "demo__table_id_42__consumer_02"
    )


def test_created_table_hook_builds_dml_consumers(monkeypatch: Any) -> None:
    opened_lakes: list[_FakeLake] = []
    extension_path = Path("/tmp/ducklake_cdc.duckdb_extension")

    def open_lake(_args: Namespace) -> _FakeLake:
        lake = _FakeLake()
        opened_lakes.append(lake)
        return lake

    monkeypatch.setattr(demo_consumer, "_open_lake", open_lake)
    monkeypatch.setattr(demo_consumer, "resolve_cdc_extension_path", lambda: extension_path)
    consumer_lakes: list[Any] = []

    consumers = demo_consumer._dml_consumers_for_created_table(
        SchemaChange(
            event_kind=DdlEventKind.CREATED,
            object_kind=DdlObjectKind.TABLE,
            snapshot_id=7,
            snapshot_time=datetime(2026, 1, 1, tzinfo=UTC),
            schema_id=1,
            schema_name="demo_schema_01",
            object_id=42,
            object_name="events_01",
            details=None,
        ),
        args=_args(consumers_per_table=2),
        stats=DemoStats(started_ns=0),
        consumer_lakes=consumer_lakes,
        dashboard=None,
        quiet=True,
    )

    assert consumers is not None
    assert [consumer.name for consumer in consumers] == [
        "demo__table_id_42__consumer_01",
        "demo__table_id_42__consumer_02",
    ]
    assert [consumer._table_id for consumer in consumers] == [42, 42]
    assert [consumer._start_at for consumer in consumers] == [7, 7]
    assert [consumer._mode for consumer in consumers] == ["changes", "changes"]
    assert consumer_lakes == opened_lakes
    assert [lake.loaded_paths for lake in opened_lakes] == [[extension_path], [extension_path]]


def test_created_table_hook_ignores_non_table_events() -> None:
    consumers = demo_consumer._dml_consumers_for_created_table(
        SchemaChange(
            event_kind=DdlEventKind.CREATED,
            object_kind=DdlObjectKind.VIEW,
            snapshot_id=7,
            snapshot_time=datetime(2026, 1, 1, tzinfo=UTC),
            schema_id=1,
            schema_name="demo_schema_01",
            object_id=42,
            object_name="events_view",
            details=None,
        ),
        args=_args(),
        stats=DemoStats(started_ns=0),
        consumer_lakes=[],
        dashboard=None,
        quiet=True,
    )

    assert consumers is None


def _args(*, consumers_per_table: int = 1) -> Namespace:
    return Namespace(
        catalog=None,
        catalog_backend=None,
        storage=None,
        consumers_per_table=consumers_per_table,
        max_snapshots=100,
        fixed_max_snapshots=False,
    )
