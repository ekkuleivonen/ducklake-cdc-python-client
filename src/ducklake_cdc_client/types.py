"""Data shapes and sink protocols for the high-level CDC client.

Sinks are the only output path from a consumer. A consumer reads a window of
changes (DML rows or DDL events), packages them into a batch, and hands the
batch to each attached sink. The sink decides what to do with it.

Implicit ack/nack is the contract: a sink that returns from ``write`` without
raising acknowledges the batch. A sink that raises nacks it, and the consumer
will retry the same batch (no commit happens). Required sinks gate the commit;
optional sinks (``require_ack=False``) do not.

DML and DDL batches are kept structurally identical but typed separately. A
type checker will catch a :class:`DDLSink` attached to a :class:`DMLConsumer`
at construction time, not at first event.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ducklake_cdc_client.enums import ChangeType, DdlEventKind, DdlObjectKind


@dataclass(frozen=True)
class Change:
    """A single DML change row delivered to a sink.

    The ``kind`` mirrors the SQL extension's emission: ``insert``, ``delete``,
    plus the two halves of an update (``update_preimage`` and
    ``update_postimage``). Pre/post images are kept distinct because they
    carry different ``produced_ns`` semantics — collapsing them loses
    end-to-end latency fidelity.

    ``table`` is the *current* qualified name of the consumer's pinned
    table (e.g. ``"main.orders"``); the SQL extension chases renames so
    callers don't need to. ``table_id`` is the stable identity. DML
    consumers are pinned to a single table by contract, so every
    :class:`Change` in a given :class:`DMLBatch` carries the same
    ``(table, table_id)`` pair.

    For idempotency, sinks should treat ``(snapshot_id, table_id, rowid,
    kind)`` as the stable identity of a change. ``rowid`` is per-table
    and may be reused after compaction, which is why ``snapshot_id`` and
    ``kind`` are part of the key.
    """

    kind: ChangeType
    snapshot_id: int
    table: str | None
    table_id: int | None
    rowid: int | None
    snapshot_time: datetime | None
    values: Mapping[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind.value,
            "snapshot_id": self.snapshot_id,
            "table": self.table,
            "table_id": self.table_id,
            "rowid": self.rowid,
            "snapshot_time": (
                self.snapshot_time.isoformat() if self.snapshot_time is not None else None
            ),
            "values": dict(self.values),
        }


@dataclass(frozen=True)
class SchemaChange:
    """A single DDL event delivered to a sink.

    Mirrors the column shape of ``cdc_ddl_changes_listen`` / ``read``:
    ``event_kind`` is one of ``created`` / ``altered`` / ``dropped`` /
    ``renamed``; ``object_kind`` is ``schema`` / ``table`` / ``view``.
    ``details`` is the JSON text payload emitted by the extension and is
    left unparsed so sinks can decide how to interpret it.

    The stable identity for idempotency is
    ``(snapshot_id, object_kind, object_id, event_kind)``.
    """

    event_kind: DdlEventKind
    object_kind: DdlObjectKind
    snapshot_id: int
    snapshot_time: datetime | None
    schema_id: int | None
    schema_name: str | None
    object_id: int | None
    object_name: str | None
    details: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_kind": self.event_kind.value,
            "object_kind": self.object_kind.value,
            "snapshot_id": self.snapshot_id,
            "snapshot_time": (
                self.snapshot_time.isoformat() if self.snapshot_time is not None else None
            ),
            "schema_id": self.schema_id,
            "schema_name": self.schema_name,
            "object_id": self.object_id,
            "object_name": self.object_name,
            "details": self.details,
        }


@dataclass(frozen=True)
class DMLTick:
    """A cheap DML notification for one snapshot.

    Tick mode reports that one or more subscribed tables were touched in a
    snapshot without materializing row payloads. ``table_ids`` is sorted and
    contains the subscribed table identities touched by this snapshot.
    """

    snapshot_id: int
    snapshot_time: datetime | None
    schema_version: int
    table_ids: tuple[int, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "snapshot_time": (
                self.snapshot_time.isoformat() if self.snapshot_time is not None else None
            ),
            "schema_version": self.schema_version,
            "table_ids": list(self.table_ids),
        }


@dataclass(frozen=True)
class DDLTick:
    """A cheap DDL notification for one snapshot.

    DDL ticks intentionally carry only snapshot metadata. Use
    ``DDLConsumer(mode="changes")`` when the sink needs object identities or
    expanded DDL payloads.
    """

    snapshot_id: int
    snapshot_time: datetime | None
    schema_version: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "snapshot_time": (
                self.snapshot_time.isoformat() if self.snapshot_time is not None else None
            ),
            "schema_version": self.schema_version,
        }


@dataclass(frozen=True)
class SinkAck:
    """Result of an explicit ``batch.ack()`` / ``batch.nack()`` call.

    Implicit ack/nack (return / raise from ``write``) is the headline
    contract; this dataclass exists for sinks that want to record an
    acknowledgement without raising.
    """

    sink: str
    batch_id: str
    ok: bool = True
    detail: str | None = None


class _BatchMixin:
    """Shared ack/nack helpers for :class:`DMLBatch` and :class:`DDLBatch`.

    The methods are pure value constructors — they return a :class:`SinkAck`
    record that callers can log, return, or hand back to the consumer.
    Implicit ack-on-return / nack-on-raise is still the headline contract;
    these helpers exist for the rare "fail this without raising" case.
    """

    batch_id: str

    def ack(self, sink: str, detail: str | None = None) -> SinkAck:
        return SinkAck(sink=sink, batch_id=self.batch_id, ok=True, detail=detail)

    def nack(self, sink: str, detail: str | None = None) -> SinkAck:
        return SinkAck(sink=sink, batch_id=self.batch_id, ok=False, detail=detail)


@dataclass(frozen=True)
class DMLBatch(_BatchMixin):
    """A pre-commit window of DML changes delivered to sinks.

    The consumer commits to the extension only after every required sink
    returns from ``write`` without raising. ``batch_id`` is stable across
    retries: a batch with the same ``(consumer_name, start_snapshot,
    end_snapshot)`` tuple has the same ``batch_id``. Sinks may use
    ``batch_id`` for crash-recovery dedup.
    """

    consumer_name: str
    batch_id: str
    start_snapshot: int
    end_snapshot: int
    snapshot_ids: tuple[int, ...]
    received_at: datetime
    changes: tuple[Change, ...]

    def __iter__(self) -> Iterator[Change]:
        return iter(self.changes)

    def __len__(self) -> int:
        return len(self.changes)

    @staticmethod
    def derive_batch_id(consumer_name: str, start_snapshot: int, end_snapshot: int) -> str:
        return f"{consumer_name}/{start_snapshot}-{end_snapshot}"


@dataclass(frozen=True)
class DDLBatch(_BatchMixin):
    """A pre-commit window of DDL events delivered to sinks.

    Structurally identical to :class:`DMLBatch` but parameterized over
    :class:`SchemaChange`. ``batch_id`` is stable across retries: a batch
    with the same ``(consumer_name, start_snapshot, end_snapshot)`` tuple
    has the same ``batch_id``.
    """

    consumer_name: str
    batch_id: str
    start_snapshot: int
    end_snapshot: int
    snapshot_ids: tuple[int, ...]
    received_at: datetime
    changes: tuple[SchemaChange, ...]

    def __iter__(self) -> Iterator[SchemaChange]:
        return iter(self.changes)

    def __len__(self) -> int:
        return len(self.changes)

    @staticmethod
    def derive_batch_id(consumer_name: str, start_snapshot: int, end_snapshot: int) -> str:
        return f"{consumer_name}/{start_snapshot}-{end_snapshot}"


@dataclass(frozen=True)
class DMLTickBatch(_BatchMixin):
    """A pre-commit window of DML ticks delivered to tick sinks."""

    consumer_name: str
    batch_id: str
    start_snapshot: int
    end_snapshot: int
    snapshot_ids: tuple[int, ...]
    received_at: datetime
    ticks: tuple[DMLTick, ...]

    def __iter__(self) -> Iterator[DMLTick]:
        return iter(self.ticks)

    def __len__(self) -> int:
        return len(self.ticks)

    @staticmethod
    def derive_batch_id(consumer_name: str, start_snapshot: int, end_snapshot: int) -> str:
        return f"{consumer_name}/{start_snapshot}-{end_snapshot}"


@dataclass(frozen=True)
class DDLTickBatch(_BatchMixin):
    """A pre-commit window of DDL ticks delivered to tick sinks."""

    consumer_name: str
    batch_id: str
    start_snapshot: int
    end_snapshot: int
    snapshot_ids: tuple[int, ...]
    received_at: datetime
    ticks: tuple[DDLTick, ...]

    def __iter__(self) -> Iterator[DDLTick]:
        return iter(self.ticks)

    def __len__(self) -> int:
        return len(self.ticks)

    @staticmethod
    def derive_batch_id(consumer_name: str, start_snapshot: int, end_snapshot: int) -> str:
        return f"{consumer_name}/{start_snapshot}-{end_snapshot}"


HeartbeatFn = Callable[[], None]


@dataclass(frozen=True)
class SinkContext:
    """Per-batch context handed to a sink's ``write`` call.

    ``heartbeat()`` lets a slow sink keep the consumer's lease alive without
    exposing heartbeat as ordinary public API. Sinks that finish a batch
    quickly do not need to call it.
    """

    consumer_name: str
    batch_id: str
    _heartbeat: HeartbeatFn

    def heartbeat(self) -> None:
        self._heartbeat()

