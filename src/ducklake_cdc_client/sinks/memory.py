"""In-memory sinks."""

from __future__ import annotations

from collections.abc import Iterator

from ducklake_cdc_client.sinks._batch import replace_changes
from ducklake_cdc_client.sinks.base import BaseDDLSink, BaseDMLSink
from ducklake_cdc_client.types import Change, DDLBatch, DMLBatch, SchemaChange, SinkContext


class MemoryDMLSink(BaseDMLSink):
    """Capture DML batches in memory for tests and notebooks."""

    name = "memory"
    require_ack = True

    def __init__(self, *, max_changes: int | None = None) -> None:
        if max_changes is not None and max_changes <= 0:
            raise ValueError("max_changes must be positive when provided")
        self._max_changes = max_changes
        self._batches: list[DMLBatch] = []
        self._changes: list[Change] = []

    @property
    def batches(self) -> list[DMLBatch]:
        return list(self._batches)

    @property
    def changes(self) -> list[Change]:
        return list(self._changes)

    def __iter__(self) -> Iterator[Change]:
        return iter(self._changes)

    def __len__(self) -> int:
        return len(self._changes)

    def write(self, batch: DMLBatch, ctx: SinkContext) -> None:
        self._batches.append(batch)
        self._changes.extend(batch.changes)
        self._enforce_cap()

    def reset(self) -> None:
        self._batches.clear()
        self._changes.clear()

    def _enforce_cap(self) -> None:
        cap = self._max_changes
        if cap is None or len(self._changes) <= cap:
            return
        excess = len(self._changes) - cap
        del self._changes[:excess]
        while self._batches and excess > 0:
            head = self._batches[0]
            count = len(head)
            if count <= excess:
                self._batches.pop(0)
                excess -= count
            else:
                self._batches[0] = replace_changes(head, head.changes[excess:])
                excess = 0


class MemoryDDLSink(BaseDDLSink):
    """Capture DDL batches in memory for tests and notebooks."""

    name = "memory"
    require_ack = True

    def __init__(self, *, max_changes: int | None = None) -> None:
        if max_changes is not None and max_changes <= 0:
            raise ValueError("max_changes must be positive when provided")
        self._max_changes = max_changes
        self._batches: list[DDLBatch] = []
        self._changes: list[SchemaChange] = []

    @property
    def batches(self) -> list[DDLBatch]:
        return list(self._batches)

    @property
    def changes(self) -> list[SchemaChange]:
        return list(self._changes)

    def __iter__(self) -> Iterator[SchemaChange]:
        return iter(self._changes)

    def __len__(self) -> int:
        return len(self._changes)

    def write(self, batch: DDLBatch, ctx: SinkContext) -> None:
        self._batches.append(batch)
        self._changes.extend(batch.changes)
        self._enforce_cap()

    def reset(self) -> None:
        self._batches.clear()
        self._changes.clear()

    def _enforce_cap(self) -> None:
        cap = self._max_changes
        if cap is None or len(self._changes) <= cap:
            return
        excess = len(self._changes) - cap
        del self._changes[:excess]
        while self._batches and excess > 0:
            head = self._batches[0]
            count = len(head)
            if count <= excess:
                self._batches.pop(0)
                excess -= count
            else:
                self._batches[0] = replace_changes(head, head.changes[excess:])
                excess = 0
