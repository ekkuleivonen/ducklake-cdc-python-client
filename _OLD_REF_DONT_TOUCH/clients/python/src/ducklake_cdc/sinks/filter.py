"""Filter sink combinators."""

from __future__ import annotations

from collections.abc import Callable

from ducklake_cdc.sinks._inner import inner_name
from ducklake_cdc.sinks.base import BaseDDLSink, BaseDMLSink, DDLSink, DMLSink
from ducklake_cdc.types import Change, DDLBatch, DMLBatch, SchemaChange, SinkContext


class FilterDMLSink(BaseDMLSink):
    """Forward only :class:`Change`s where ``predicate`` returns truthy."""

    def __init__(
        self,
        predicate: Callable[[Change], bool],
        sink: DMLSink,
        *,
        name: str | None = None,
    ) -> None:
        self._predicate = predicate
        self._inner = sink
        self.name = name or f"filter({inner_name(sink)})"
        self.require_ack = getattr(sink, "require_ack", True)

    def open(self) -> None:
        self._inner.open()

    def close(self) -> None:
        self._inner.close()

    def write(self, batch: DMLBatch, ctx: SinkContext) -> None:
        kept = tuple(change for change in batch.changes if self._predicate(change))
        new_batch = DMLBatch(
            consumer_name=batch.consumer_name,
            batch_id=batch.batch_id,
            start_snapshot=batch.start_snapshot,
            end_snapshot=batch.end_snapshot,
            snapshot_ids=batch.snapshot_ids,
            received_at=batch.received_at,
            changes=kept,
        )
        self._inner.write(new_batch, ctx)


class FilterDDLSink(BaseDDLSink):
    """Forward only :class:`SchemaChange`s where ``predicate`` returns truthy."""

    def __init__(
        self,
        predicate: Callable[[SchemaChange], bool],
        sink: DDLSink,
        *,
        name: str | None = None,
    ) -> None:
        self._predicate = predicate
        self._inner = sink
        self.name = name or f"filter({inner_name(sink)})"
        self.require_ack = getattr(sink, "require_ack", True)

    def open(self) -> None:
        self._inner.open()

    def close(self) -> None:
        self._inner.close()

    def write(self, batch: DDLBatch, ctx: SinkContext) -> None:
        kept = tuple(event for event in batch.changes if self._predicate(event))
        new_batch = DDLBatch(
            consumer_name=batch.consumer_name,
            batch_id=batch.batch_id,
            start_snapshot=batch.start_snapshot,
            end_snapshot=batch.end_snapshot,
            snapshot_ids=batch.snapshot_ids,
            received_at=batch.received_at,
            changes=kept,
        )
        self._inner.write(new_batch, ctx)
