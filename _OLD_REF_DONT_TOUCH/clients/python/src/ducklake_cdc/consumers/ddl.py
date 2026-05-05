"""DDL consumer."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import UTC, datetime

from ducklake import DuckLake
from ducklake_cdc.consumers.base import (
    _DEFAULT_LEASE_WAIT_TIMEOUT,
    ConsumerMode,
    LeasePolicy,
    OnExists,
    RetryPolicy,
    StartAt,
    _ConsumerBase,
)
from ducklake_cdc.lowlevel import CDCClient, DDLTickRow, SchemaChangeRow
from ducklake_cdc.sinks.base import DDLSink, DDLTickSink
from ducklake_cdc.types import DDLBatch, DDLTick, DDLTickBatch, SchemaChange


class DDLConsumer(_ConsumerBase):
    """Durable consumer for catalog/schema/table DDL events."""

    _kind = "ddl"

    def __init__(
        self,
        lake: DuckLake,
        name: str,
        *,
        schemas: Sequence[str] | None = None,
        tables: Sequence[str] | None = None,
        start_at: StartAt = "now",
        mode: ConsumerMode = "ticks",
        on_exists: OnExists = "use",
        lease_policy: LeasePolicy = "wait",
        lease_wait_timeout: float = _DEFAULT_LEASE_WAIT_TIMEOUT,
        sinks: Sequence[DDLSink | DDLTickSink] = (),
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
        self._schemas = list(schemas) if schemas else None
        self._tables = list(tables) if tables else None

    def _create_consumer(self, client: CDCClient) -> None:
        client.cdc_ddl_consumer_create(
            self._name,
            schemas=self._schemas,
            table_names=self._tables,
            start_at="now",
        )

    def _listen_op(
        self, timeout_ms: int, max_snapshots: int
    ) -> Callable[[], list[SchemaChangeRow] | list[DDLTickRow]]:
        client = self._require_client()
        name = self._name

        def operation() -> list[SchemaChangeRow] | list[DDLTickRow]:
            if self._mode == "ticks":
                return client.cdc_ddl_ticks_listen(
                    name,
                    timeout_ms=timeout_ms,
                    max_snapshots=max_snapshots,
                )
            return client.cdc_ddl_changes_listen(
                name,
                timeout_ms=timeout_ms,
                max_snapshots=max_snapshots,
            )

        return operation

    def _build_batch(
        self, rows: list[SchemaChangeRow] | list[DDLTickRow]
    ) -> DDLBatch | DDLTickBatch:
        if self._mode == "ticks":
            return self._build_tick_batch(rows)  # type: ignore[arg-type]
        return self._build_change_batch(rows)  # type: ignore[arg-type]

    def _build_change_batch(self, rows: list[SchemaChangeRow]) -> DDLBatch:
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
            SchemaChange(
                event_kind=row.event_kind,
                object_kind=row.object_kind,
                snapshot_id=row.snapshot_id,
                snapshot_time=row.snapshot_time,
                schema_id=row.schema_id,
                schema_name=row.schema_name,
                object_id=row.object_id,
                object_name=row.object_name,
                details=row.details,
            )
            for row in rows
        )
        return DDLBatch(
            consumer_name=self._name,
            batch_id=DDLBatch.derive_batch_id(self._name, start, end),
            start_snapshot=start,
            end_snapshot=end,
            snapshot_ids=snapshot_ids,
            received_at=datetime.now(UTC),
            changes=changes,
        )

    def _build_tick_batch(self, rows: list[DDLTickRow]) -> DDLTickBatch:
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
            DDLTick(
                snapshot_id=row.snapshot_id,
                snapshot_time=row.snapshot_time,
                schema_version=row.schema_version,
            )
            for row in rows
        )
        return DDLTickBatch(
            consumer_name=self._name,
            batch_id=DDLTickBatch.derive_batch_id(self._name, start, end),
            start_snapshot=start,
            end_snapshot=end,
            snapshot_ids=snapshot_ids,
            received_at=datetime.now(UTC),
            ticks=ticks,
        )
