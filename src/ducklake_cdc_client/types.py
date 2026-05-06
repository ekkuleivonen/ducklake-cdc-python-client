"""Data shapes for the high-level CDC client.

Consumers read a window of changes (DML rows or DDL events) and package them
into a batch. Iterator users process the batch and call ``batch.commit()``.
Sink users let ``consumer.run()`` deliver and commit batches for them.

Implicit ack/nack is the contract: a sink that returns from ``write`` without
raising acknowledges the batch. A sink that raises nacks it, and the consumer
will retry the same batch (no commit happens). Required sinks gate the commit;
optional sinks (``required=False``) do not.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from types import TracebackType
from typing import Any, Self

from ducklake_cdc_client.enums import ChangeType, DdlEventKind, DdlObjectKind

CommitFn = Callable[[int], None]
CommitWithinFn = Callable[[Any, int], None]


def _unbound_commit(snapshot: int) -> None:
    raise RuntimeError("batch is not bound to an active consumer")


def _unbound_commit_within(conn: Any, snapshot: int) -> None:
    raise RuntimeError("batch is not bound to an active consumer")


class BatchTransaction:
    """Atomic transaction wrapping sink work + the cursor advance.

    Returned from :meth:`DMLBatch.transaction` (and tick variants).
    Use as a context manager::

        for batch in consumer.batches(stop_event=stop):
            with batch.transaction() as tx:
                tx.executemany("INSERT INTO sink VALUES (...)", rows)

    Lifecycle:

    - ``__enter__`` issues ``BEGIN`` on the consumer's connection.
    - Normal ``__exit__`` issues ``cdc_commit`` then ``COMMIT`` on the
      same connection. The two are atomic from the catalog's
      perspective: a crash between them is impossible because they
      share one BEGIN.
    - Exceptional ``__exit__`` issues ``ROLLBACK`` and re-raises.
      Nothing is committed -- the next listen call replays the same
      batch -> exactly-once.

    The transaction owns the consumer's connection for its lifetime;
    do not run other queries on ``consumer.connection`` until the
    ``with`` block exits.
    """

    __slots__ = ("_conn", "_end_snapshot", "_commit_within", "_state")

    def __init__(
        self,
        conn: Any,
        end_snapshot: int,
        commit_within: CommitWithinFn,
    ) -> None:
        self._conn = conn
        self._end_snapshot = end_snapshot
        self._commit_within = commit_within
        # one of "fresh", "open", "closed"
        self._state = "fresh"

    @property
    def connection(self) -> Any:
        """The consumer's underlying DuckDB connection.

        Use ``execute`` / ``executemany`` on the transaction object for
        most cases; the raw connection is exposed for callers that need
        DuckDB APIs not proxied here (Arrow ingestion, prepared
        statements, etc.). Anything you run on this connection inside
        the ``with`` block is part of the same BEGIN.
        """
        return self._conn

    def execute(self, sql: str, parameters: Any = None) -> Any:
        if parameters is None:
            return self._conn.execute(sql)
        return self._conn.execute(sql, parameters)

    def executemany(self, sql: str, parameters: Iterable[Any]) -> Any:
        return self._conn.executemany(sql, parameters)

    def __enter__(self) -> Self:
        if self._state != "fresh":
            raise RuntimeError("BatchTransaction is single-use")
        self._conn.execute("BEGIN")
        self._state = "open"
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._state = "closed"
        if exc_type is not None:
            try:
                self._conn.execute("ROLLBACK")
            except Exception:
                # Don't mask the in-flight exception; surface the
                # rollback failure as a chained log instead.
                pass
            return None
        try:
            self._commit_within(self._conn, self._end_snapshot)
            self._conn.execute("COMMIT")
        except Exception:
            try:
                self._conn.execute("ROLLBACK")
            except Exception:
                pass
            raise
        return None


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
class DMLBatch:
    """A pre-commit window of DML changes.

    Hand the batch to a sink and finish with one of two patterns:

    - ``with batch.transaction() as tx:`` -- atomic sink-write +
      cdc_commit. The recommended path for non-idempotent sinks (plain
      INSERTs, external writes, anything where replay would duplicate).
      ``ROLLBACK`` on exception, ``cdc_commit`` + ``COMMIT`` on
      success, both on the consumer's lease-holding connection.
    - ``batch.commit()`` -- bare cursor advance, no transaction. Use
      this only for sinks that are idempotent on
      ``(snapshot_id, table_id, rowid, kind)`` (writing to an OLAP
      sink with that as a primary key, posting to an idempotent HTTP
      endpoint, etc.).

    ``batch_id`` is stable across retries: a batch with the same
    ``(consumer_name, start_snapshot, end_snapshot)`` tuple has the
    same ``batch_id``.
    """

    consumer_name: str
    batch_id: str
    start_snapshot: int
    end_snapshot: int
    snapshot_ids: tuple[int, ...]
    received_at: datetime
    changes: tuple[Change, ...]
    _commit: CommitFn = field(default=_unbound_commit, repr=False, compare=False)
    _commit_within: CommitWithinFn = field(
        default=_unbound_commit_within, repr=False, compare=False
    )
    _connection: Any = field(default=None, repr=False, compare=False)

    def __iter__(self) -> Iterator[Change]:
        return iter(self.changes)

    def __len__(self) -> int:
        return len(self.changes)

    @staticmethod
    def derive_batch_id(consumer_name: str, start_snapshot: int, end_snapshot: int) -> str:
        return f"{consumer_name}/{start_snapshot}-{end_snapshot}"

    def transaction(self) -> BatchTransaction:
        """Atomic sink-write + cursor advance on the consumer's connection.

        See :class:`BatchTransaction` for the lifecycle. Use this
        whenever your sink isn't idempotent::

            for batch in consumer.batches(stop_event=stop):
                with batch.transaction() as tx:
                    tx.executemany(
                        "INSERT INTO sink VALUES (...)",
                        rows_for(batch),
                    )
        """
        if self._connection is None:
            raise RuntimeError(
                "batch.transaction() requires the batch to be created by an "
                "open consumer; this batch has no bound connection"
            )
        return BatchTransaction(self._connection, self.end_snapshot, self._commit_within)

    def commit(self) -> None:
        """Advance the consumer cursor to ``end_snapshot`` (no transaction).

        Idempotent-sink path. Runs ``cdc_commit`` on the consumer's
        connection without wrapping it in a BEGIN -- there's no sink
        write to keep atomic with it because your sink either dedupes
        on replay or doesn't care about exactly-once.

        For non-idempotent sinks, use :meth:`transaction` instead.
        """
        self._commit(self.end_snapshot)


@dataclass(frozen=True)
class DDLBatch:
    """A pre-commit window of DDL events.

    Structurally identical to :class:`DMLBatch` but parameterized over
    :class:`SchemaChange`. See :class:`DMLBatch` for the
    ``transaction()`` / ``commit()`` lifecycle.
    """

    consumer_name: str
    batch_id: str
    start_snapshot: int
    end_snapshot: int
    snapshot_ids: tuple[int, ...]
    received_at: datetime
    changes: tuple[SchemaChange, ...]
    _commit: CommitFn = field(default=_unbound_commit, repr=False, compare=False)
    _commit_within: CommitWithinFn = field(
        default=_unbound_commit_within, repr=False, compare=False
    )
    _connection: Any = field(default=None, repr=False, compare=False)

    def __iter__(self) -> Iterator[SchemaChange]:
        return iter(self.changes)

    def __len__(self) -> int:
        return len(self.changes)

    @staticmethod
    def derive_batch_id(consumer_name: str, start_snapshot: int, end_snapshot: int) -> str:
        return f"{consumer_name}/{start_snapshot}-{end_snapshot}"

    def transaction(self) -> BatchTransaction:
        """See :meth:`DMLBatch.transaction`."""
        if self._connection is None:
            raise RuntimeError(
                "batch.transaction() requires the batch to be created by an "
                "open consumer; this batch has no bound connection"
            )
        return BatchTransaction(self._connection, self.end_snapshot, self._commit_within)

    def commit(self) -> None:
        """See :meth:`DMLBatch.commit`."""
        self._commit(self.end_snapshot)


@dataclass(frozen=True)
class DMLTickBatch:
    """A pre-commit window of DML ticks."""

    consumer_name: str
    batch_id: str
    start_snapshot: int
    end_snapshot: int
    snapshot_ids: tuple[int, ...]
    received_at: datetime
    ticks: tuple[DMLTick, ...]
    _commit: CommitFn = field(default=_unbound_commit, repr=False, compare=False)
    _commit_within: CommitWithinFn = field(
        default=_unbound_commit_within, repr=False, compare=False
    )
    _connection: Any = field(default=None, repr=False, compare=False)

    def __iter__(self) -> Iterator[DMLTick]:
        return iter(self.ticks)

    def __len__(self) -> int:
        return len(self.ticks)

    @staticmethod
    def derive_batch_id(consumer_name: str, start_snapshot: int, end_snapshot: int) -> str:
        return f"{consumer_name}/{start_snapshot}-{end_snapshot}"

    def transaction(self) -> BatchTransaction:
        """See :meth:`DMLBatch.transaction`."""
        if self._connection is None:
            raise RuntimeError(
                "batch.transaction() requires the batch to be created by an "
                "open consumer; this batch has no bound connection"
            )
        return BatchTransaction(self._connection, self.end_snapshot, self._commit_within)

    def commit(self) -> None:
        """See :meth:`DMLBatch.commit`."""
        self._commit(self.end_snapshot)


@dataclass(frozen=True)
class DDLTickBatch:
    """A pre-commit window of DDL ticks."""

    consumer_name: str
    batch_id: str
    start_snapshot: int
    end_snapshot: int
    snapshot_ids: tuple[int, ...]
    received_at: datetime
    ticks: tuple[DDLTick, ...]
    _commit: CommitFn = field(default=_unbound_commit, repr=False, compare=False)
    _commit_within: CommitWithinFn = field(
        default=_unbound_commit_within, repr=False, compare=False
    )
    _connection: Any = field(default=None, repr=False, compare=False)

    def __iter__(self) -> Iterator[DDLTick]:
        return iter(self.ticks)

    def __len__(self) -> int:
        return len(self.ticks)

    @staticmethod
    def derive_batch_id(consumer_name: str, start_snapshot: int, end_snapshot: int) -> str:
        return f"{consumer_name}/{start_snapshot}-{end_snapshot}"

    def transaction(self) -> BatchTransaction:
        """See :meth:`DMLBatch.transaction`."""
        if self._connection is None:
            raise RuntimeError(
                "batch.transaction() requires the batch to be created by an "
                "open consumer; this batch has no bound connection"
            )
        return BatchTransaction(self._connection, self.end_snapshot, self._commit_within)

    def commit(self) -> None:
        """See :meth:`DMLBatch.commit`."""
        self._commit(self.end_snapshot)


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

