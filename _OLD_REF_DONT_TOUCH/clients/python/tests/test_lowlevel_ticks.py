from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

from ducklake import DuckLake
from ducklake_cdc.lowlevel import CDCClient


class _Result:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def list(self) -> list[dict[str, Any]]:
        return self._rows


class _Lake:
    alias = "lake"

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.sql_calls: list[str] = []

    def sql(self, query: str) -> _Result:
        self.sql_calls.append(query)
        return _Result(self.rows)


def test_cdc_client_reads_dml_ticks() -> None:
    lake = _Lake(
        [
            {
                "consumer_name": "orders",
                "start_snapshot": 10,
                "end_snapshot": 12,
                "snapshot_id": 11,
                "snapshot_time": datetime(2026, 1, 1, tzinfo=UTC),
                "schema_version": 3,
                "table_ids": [42, 43],
            }
        ]
    )
    client = CDCClient(cast(DuckLake, lake))

    rows = client.cdc_dml_ticks_read("orders")

    assert "cdc_dml_ticks_read" in lake.sql_calls[0]
    assert rows[0].table_ids == (42, 43)


def test_cdc_client_listens_for_ddl_ticks() -> None:
    lake = _Lake(
        [
            {
                "consumer_name": "ddl",
                "start_snapshot": 10,
                "end_snapshot": 10,
                "snapshot_id": 10,
                "snapshot_time": datetime(2026, 1, 1, tzinfo=UTC),
                "schema_version": 4,
            }
        ]
    )
    client = CDCClient(cast(DuckLake, lake))

    rows = client.cdc_ddl_ticks_listen("ddl", timeout_ms=50, max_snapshots=2)

    assert "cdc_ddl_ticks_listen" in lake.sql_calls[0]
    assert rows[0].schema_version == 4
