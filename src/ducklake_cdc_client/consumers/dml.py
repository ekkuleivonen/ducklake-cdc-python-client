"""DML consumer."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from typing import Any

from ducklake_client import DuckLake

from ducklake_cdc_client.client import CDCClient, ChangeRow, DMLTickRow, SchemaDiffRow
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
        connection: Any | None = None,
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
            connection=connection,
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
            start_at=self._start_at,
        )

    def schema_diff(
        self,
        *,
        from_snapshot: int | None = None,
        to_snapshot: int | None = None,
        max_snapshots: int = 100,
    ) -> list[SchemaDiffRow]:
        """Return schema diff rows for this consumer's terminal boundary."""

        self._require_open()
        if from_snapshot is None or to_snapshot is None:
            window = self.window(max_snapshots=max_snapshots)
            boundary = window.terminal_at_snapshot
            if boundary is None:
                raise RuntimeError(
                    f"consumer {self._name!r} has no schema boundary to diff"
                )
            from_snapshot = boundary
            to_snapshot = boundary
        schema_name, table_name = self._schema_diff_table()
        return self._require_client().schema.diff(
            schema_name,
            table_name,
            from_snapshot,
            to_snapshot,
        )

    def successor(
        self,
        name: str,
        *,
        start_at: StartAt | None = None,
        max_snapshots: int = 100,
        on_exists: OnExists = "error",
        lease_policy: LeasePolicy | None = None,
        lease_wait_timeout: float | None = None,
        sinks: Sequence[SinkLike] = (),
    ) -> DMLConsumer:
        """Create a successor consumer object positioned at this schema boundary."""

        self._require_open()
        if start_at is None:
            window = self.window(max_snapshots=max_snapshots)
            if not window.terminal or window.terminal_at_snapshot is None:
                raise RuntimeError(
                    f"consumer {self._name!r} is not stopped at a schema boundary"
                )
            start_at = window.terminal_at_snapshot
        return DMLConsumer(
            self._lake,
            name,
            table=self._table,
            table_id=self._table_id,
            change_types=self._change_types,
            start_at=start_at,
            mode=self._mode,
            on_exists=on_exists,
            lease_policy=lease_policy or self._lease_policy,
            lease_wait_timeout=(
                self._lease_wait_timeout
                if lease_wait_timeout is None
                else lease_wait_timeout
            ),
            sinks=sinks,
            connection=self._connection_override,
            retry=self._retry_policy,
        )

    def _schema_diff_table(self) -> tuple[str, str]:
        table_name = self._table
        if table_name is None:
            entry = self._lookup_consumer(self._require_client())
            table_name = entry.table_name if entry is not None else None
        if table_name is None:
            raise RuntimeError(
                "schema_diff() requires a table name; table_id consumers need "
                "an existing consumer list entry with table_name"
            )
        parts = table_name.split(".", 1)
        if len(parts) == 2:
            return parts[0], parts[1]
        return self._require_client().catalog, table_name

    def _listen_op(
        self,
        timeout_ms: int,
        max_snapshots: int,
        poll_min_ms: int | None,
        coalesce: bool | None,
    ) -> Callable[[], list[ChangeRow] | list[DMLTickRow]]:
        client = self._require_client()
        name = self._name

        def operation() -> list[ChangeRow] | list[DMLTickRow]:
            if self._mode == "ticks":
                return client.cdc_dml_ticks_listen(
                    name,
                    timeout_ms=timeout_ms,
                    max_snapshots=max_snapshots,
                    poll_min_ms=poll_min_ms,
                    coalesce=coalesce,
                )
            return client.cdc_dml_changes_listen(
                name,
                timeout_ms=timeout_ms,
                max_snapshots=max_snapshots,
                poll_min_ms=poll_min_ms,
                coalesce=coalesce,
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
            _commit_within=self._commit_snapshot_within,
            _connection=self._connection,
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
            _commit_within=self._commit_snapshot_within,
            _connection=self._connection,
        )
