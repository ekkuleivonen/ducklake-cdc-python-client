"""Map sink combinators."""

from __future__ import annotations

from collections.abc import Callable

from ducklake_cdc.sinks._inner import inner_name
from ducklake_cdc.sinks.base import BaseDDLSink, BaseDMLSink, DDLSink, DMLSink
from ducklake_cdc.types import Change, DDLBatch, DMLBatch, SchemaChange, SinkContext


class MapDMLSink(BaseDMLSink):
    """Transform each :class:`Change` then forward to ``sink``."""

    def __init__(
        self,
        fn: Callable[[Change], Change],
        sink: DMLSink,
        *,
        name: str | None = None,
    ) -> None:
        self._fn = fn
        self._inner = sink
        self.name = name or f"map({inner_name(sink)})"
        self.require_ack = getattr(sink, "require_ack", True)

    def open(self) -> None:
        self._inner.open()

    def close(self) -> None:
        self._inner.close()

    def write(self, batch: DMLBatch, ctx: SinkContext) -> None:
        mapped = tuple(self._fn(change) for change in batch.changes)
        new_batch = DMLBatch(
            consumer_name=batch.consumer_name,
            batch_id=batch.batch_id,
            start_snapshot=batch.start_snapshot,
            end_snapshot=batch.end_snapshot,
            snapshot_ids=batch.snapshot_ids,
            received_at=batch.received_at,
            changes=mapped,
        )
        self._inner.write(new_batch, ctx)


class MapDDLSink(BaseDDLSink):
    """Transform each :class:`SchemaChange` then forward to ``sink``."""

    def __init__(
        self,
        fn: Callable[[SchemaChange], SchemaChange],
        sink: DDLSink,
        *,
        name: str | None = None,
    ) -> None:
        self._fn = fn
        self._inner = sink
        self.name = name or f"map({inner_name(sink)})"
        self.require_ack = getattr(sink, "require_ack", True)

    def open(self) -> None:
        self._inner.open()

    def close(self) -> None:
        self._inner.close()

    def write(self, batch: DDLBatch, ctx: SinkContext) -> None:
        mapped = tuple(self._fn(event) for event in batch.changes)
        new_batch = DDLBatch(
            consumer_name=batch.consumer_name,
            batch_id=batch.batch_id,
            start_snapshot=batch.start_snapshot,
            end_snapshot=batch.end_snapshot,
            snapshot_ids=batch.snapshot_ids,
            received_at=batch.received_at,
            changes=mapped,
        )
        self._inner.write(new_batch, ctx)
