"""DML consumer."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import UTC, datetime

from ducklake_client import DuckLake

from ducklake_cdc_client.client import CDCClient, ChangeRow, DMLTickRow
from ducklake_cdc_client.consumers.base import (
    _DEFAULT_LEASE_WAIT_TIMEOUT,
    ConsumerMode,
    LeasePolicy,
    OnExists,
    RetryPolicy,
    StartAt,
    _ConsumerBase,
)
from ducklake_cdc_client.sinks.base import SinkLike
from ducklake_cdc_client.types import Change, DMLBatch, DMLTick, DMLTickBatch


class DMLConsumer(_ConsumerBase):
    """Durable consumer for row-level DML changes."""

    _kind = "dml"

    def __init__(
        self,
        lake: DuckLake,
        name: str,
        *,
        table: str | None = None,
        table_id: int | None = None,
        change_types: Sequence[str] | None = None,
        start_at: StartAt = "now",
        mode: ConsumerMode = "ticks",
        on_exists: OnExists = "use",
        lease_policy: LeasePolicy = "wait",
        lease_wait_timeout: float = _DEFAULT_LEASE_WAIT_TIMEOUT,
        sinks: Sequence[SinkLike] = (),
        client: CDCClient | None = None,
        retry: RetryPolicy | None = None,
    ) -> None:
        super().__init__(
            lake,
            name,
            start_at=start_at,
            mode=mode,
            on_exists=on_exists,
            lease_policy=lease_policy,
            lease_wait_timeout=lease_wait_timeout,
            sinks=sinks,
            client=client,
            retry=retry,
        )
        if (table is None) == (table_id is None):
            raise ValueError(
                "DMLConsumer requires exactly one of table=... or table_id=... "
                "(DML consumers are pinned to a single table)."
            )
        self._table = table
        self._table_id = table_id
        self._change_types = list(change_types) if change_types else None

    def _create_consumer(self, client: CDCClient) -> None:
        client.cdc_dml_consumer_create(
            self._name,
            table_name=self._table,
            table_id=self._table_id,
            change_types=self._change_types,
            start_at="now",
        )

    def _listen_op(
        self, timeout_ms: int, max_snapshots: int
    ) -> Callable[[], list[ChangeRow] | list[DMLTickRow]]:
        client = self._require_client()
        name = self._name

        def operation() -> list[ChangeRow] | list[DMLTickRow]:
            if self._mode == "ticks":
                return client.cdc_dml_ticks_listen(
                    name,
                    timeout_ms=timeout_ms,
                    max_snapshots=max_snapshots,
                )
            return client.cdc_dml_changes_listen(
                name,
                timeout_ms=timeout_ms,
                max_snapshots=max_snapshots,
            )

        return operation

    def _read_op(
        self,
        max_snapshots: int,
        start_snapshot: int | None,
        end_snapshot: int | None,
    ) -> Callable[[], list[ChangeRow] | list[DMLTickRow]]:
        client = self._require_client()
        name = self._name

        def operation() -> list[ChangeRow] | list[DMLTickRow]:
            if self._mode == "ticks":
                return client.cdc_dml_ticks_read(
                    name,
                    max_snapshots=max_snapshots,
                    start_snapshot=start_snapshot,
                    end_snapshot=end_snapshot,
                )
            return client.cdc_dml_changes_read(
                name,
                max_snapshots=max_snapshots,
                start_snapshot=start_snapshot,
                end_snapshot=end_snapshot,
            )

        return operation

    def _build_batch(self, rows: list[ChangeRow] | list[DMLTickRow]) -> DMLBatch | DMLTickBatch:
        if self._mode == "ticks":
            return self._build_tick_batch(rows)  # type: ignore[arg-type]
        return self._build_change_batch(rows)  # type: ignore[arg-type]

    def _build_change_batch(self, rows: list[ChangeRow]) -> DMLBatch:
        start = min(
            row.start_snapshot if row.start_snapshot is not None else row.snapshot_id
            for row in rows
        )
        end = max(
            row.end_snapshot if row.end_snapshot is not None else row.snapshot_id
            for row in rows
        )
        snapshot_ids = tuple(sorted({row.snapshot_id for row in rows}))
        changes = tuple(
            Change(
                kind=row.change_type,
                snapshot_id=row.snapshot_id,
                table=row.table_name,
                table_id=row.table_id,
                rowid=row.rowid,
                snapshot_time=row.snapshot_time,
                values=row.values,
            )
            for row in rows
        )
        return DMLBatch(
            consumer_name=self._name,
            batch_id=DMLBatch.derive_batch_id(self._name, start, end),
            start_snapshot=start,
            end_snapshot=end,
            snapshot_ids=snapshot_ids,
            received_at=datetime.now(UTC),
            changes=changes,
            _commit=self._commit_snapshot,
        )

    def _build_tick_batch(self, rows: list[DMLTickRow]) -> DMLTickBatch:
        start = min(
            row.start_snapshot if row.start_snapshot is not None else row.snapshot_id
            for row in rows
        )
        end = max(
            row.end_snapshot if row.end_snapshot is not None else row.snapshot_id
            for row in rows
        )
        snapshot_ids = tuple(sorted({row.snapshot_id for row in rows}))
        ticks = tuple(
            DMLTick(
                snapshot_id=row.snapshot_id,
                snapshot_time=row.snapshot_time,
                schema_version=row.schema_version,
                table_ids=row.table_ids,
            )
            for row in rows
        )
        return DMLTickBatch(
            consumer_name=self._name,
            batch_id=DMLTickBatch.derive_batch_id(self._name, start, end),
            start_snapshot=start,
            end_snapshot=end,
            snapshot_ids=snapshot_ids,
            received_at=datetime.now(UTC),
            ticks=ticks,
            _commit=self._commit_snapshot,
        )
