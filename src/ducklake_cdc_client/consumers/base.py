"""Shared consumer lifecycle and helpers."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, datetime
from types import TracebackType
from typing import Any, Literal, Self, TypeVar

from ducklake_client import DuckLake, DuckLakeQueryError

from ducklake_cdc_client.client import CDCClient, ConsumerListEntry, ConsumerWindow
from ducklake_cdc_client.client.client import _connection_for_lake
from ducklake_cdc_client.client.sql import table_function_sql
from ducklake_cdc_client.retry import retry_on_transient
from ducklake_cdc_client.sinks.base import (
    Sink,
    as_sink,
    close_sink,
    open_sink,
    sink_name,
    sink_required,
    write_sink,
)
from ducklake_cdc_client.types import SinkContext

T = TypeVar("T")

OnExists = Literal["error", "use", "replace"]
LeasePolicy = Literal["wait", "takeover", "error"]
ConsumerMode = Literal["ticks", "changes"]
StartAt = str | int

RetryPolicy = Callable[[Callable[[], object]], object]

_LOG = logging.getLogger(__name__)
_DEFAULT_LEASE_WAIT_TIMEOUT = 30.0
_LEASE_WAIT_POLL_INTERVAL = 0.5
_LEASE_FRESHNESS_GRACE_SECONDS = 5.0
_ADAPTIVE_INITIAL_MAX_SNAPSHOTS = 8
_ADAPTIVE_TARGET_ROWS = 2048
_ADAPTIVE_TARGET_LISTEN_MS = 150.0
_ADAPTIVE_FAST_LISTEN_MS = 75.0
_ADAPTIVE_LARGE_BATCH_ROWS = 8192


class _AdaptiveSnapshotWindow:
    def __init__(self, ceiling: int) -> None:
        self.ceiling = max(1, ceiling)
        self.current = min(self.ceiling, _ADAPTIVE_INITIAL_MAX_SNAPSHOTS)

    def observe_empty(self) -> None:
        self.current = max(1, self.current // 2)

    def observe_batch(
        self,
        *,
        row_count: int,
        snapshot_span: int,
        listen_elapsed_ms: float,
    ) -> None:
        if (
            listen_elapsed_ms >= _ADAPTIVE_TARGET_LISTEN_MS
            or row_count >= _ADAPTIVE_LARGE_BATCH_ROWS
        ):
            self.current = max(1, self.current // 2)
            return

        if (
            snapshot_span >= self.current
            and row_count <= (_ADAPTIVE_TARGET_ROWS // 2)
            and listen_elapsed_ms <= _ADAPTIVE_FAST_LISTEN_MS
        ):
            self.current = min(self.ceiling, max(self.current + 1, self.current * 2))


class _PinnedConnectionLake:
    """Adapter that makes a raw DuckDB connection look like a DuckLake.

    :class:`CDCClient` resolves its connection through ``lake.connection``
    (or ``lake.raw_connection()``), so any object exposing a
    ``connection`` attribute satisfies it. Passing one of these lets the
    caller pin every CDC call (``cdc_dml_consumer_create``, the
    ``cdc_dml_changes_listen`` long-poll, ``cdc_commit``, heartbeats,
    consumer listing) to a specific connection -- which is the only way
    to keep the consumer's lease (anchored on
    ``(db_pointer, connection_id)``) consistent with a transactional
    commit the caller drives via :meth:`DMLBatch.commit(conn=...)`.
    """

    __slots__ = ("connection", "alias")

    def __init__(self, connection: Any, *, alias: str) -> None:
        self.connection = connection
        self.alias = alias


class _ConsumerBase:
    """Shared lifecycle and run loop for DML and DDL consumers."""

    _kind: str

    def __init__(
        self,
        lake: DuckLake,
        name: str,
        *,
        start_at: StartAt = "now",
        mode: ConsumerMode = "ticks",
        on_exists: OnExists = "use",
        lease_policy: LeasePolicy = "wait",
        lease_wait_timeout: float = _DEFAULT_LEASE_WAIT_TIMEOUT,
        sinks: Sequence[Any] = (),
        client: CDCClient | None = None,
        connection: Any | None = None,
        retry: RetryPolicy | None = None,
    ) -> None:
        if not name:
            raise ValueError(f"{type(self).__name__} requires a non-empty name")
        if lease_policy not in ("wait", "takeover", "error"):
            raise ValueError(
                f"lease_policy must be 'wait', 'takeover', or 'error'; "
                f"got {lease_policy!r}"
            )
        if lease_wait_timeout < 0:
            raise ValueError("lease_wait_timeout must be >= 0")
        if mode not in ("ticks", "changes"):
            raise ValueError(f"mode must be 'ticks' or 'changes'; got {mode!r}")
        if client is not None and connection is not None:
            raise ValueError(
                "pass at most one of `client` or `connection`; supplying both "
                "is ambiguous (the explicit `client` already pins its own "
                "connection)"
            )

        self._lake = lake
        self._name = name
        self._start_at = start_at
        self._mode: ConsumerMode = mode
        self._on_exists = on_exists
        self._lease_policy: LeasePolicy = lease_policy
        self._lease_wait_timeout = lease_wait_timeout
        self._sinks: list[Sink] = [as_sink(item) for item in sinks]
        self._client = client
        self._connection_override = connection
        # The connection used for every CDC call (listen, cdc_commit,
        # heartbeat) and exposed via ``consumer.connection``. Resolved
        # lazily on ``__enter__``; a derived one is closed on
        # ``__exit__``, an override is left to the caller.
        self._connection: Any = None
        self._derived_connection: Any = None
        # Default to ``retry_on_transient`` so the H-022 first-bootstrap
        # mutex race and SQLite ``database is locked`` bursts don't leak
        # to callers. Pass ``retry=no_retry`` to opt out.
        self._retry_policy: RetryPolicy = retry if retry is not None else retry_on_transient
        self._opened = False

    def _effective_retry(self, retry: RetryPolicy | None) -> RetryPolicy:
        """Use per-call ``retry`` override when provided, else the consumer default."""

        return self._retry_policy if retry is None else retry

    @property
    def name(self) -> str:
        return self._name

    @property
    def client(self) -> CDCClient:
        if self._client is None:
            raise RuntimeError(
                f"{type(self).__name__}.client is only available inside a "
                "`with` block"
            )
        return self._client

    @property
    def connection(self) -> Any:
        """The DuckDB connection backing every CDC call this consumer makes.

        Pinned by the consumer at ``__enter__`` time -- either an
        explicit override passed via ``connection=`` (caller owns
        lifecycle) or one derived from ``lake.connection.cursor()``
        (consumer owns lifecycle, closes on ``__exit__``).

        This is the connection the extension's lease lives on, so it's
        also the only connection from which ``cdc_commit`` will succeed
        without CDC_BUSY. :meth:`DMLBatch.transaction` uses it
        automatically -- the property is exposed for callers that need
        DuckDB APIs not proxied through ``BatchTransaction``.
        """

        if not self._opened:
            raise RuntimeError(
                f"{type(self).__name__}.connection is only available "
                "inside a `with` block"
            )
        return self._connection

    def __enter__(self) -> Self:
        if self._client is None:
            # Always pin every CDC call to one connection so the
            # extension's lease (anchored on
            # ``(db_pointer, connection_id)``) and the cursor advance
            # in ``DMLBatch.transaction`` agree on which connection
            # holds the lease. Without this, ``cdc_commit`` from a
            # different connection raises CDC_BUSY.
            if self._connection_override is not None:
                self._connection = self._connection_override
            else:
                self._connection = self._derive_connection()
                self._derived_connection = self._connection
            lake_alias = getattr(self._lake, "alias", "lake")
            self._client = CDCClient(
                _PinnedConnectionLake(self._connection, alias=lake_alias),
                install_extension=False,
            )
        else:
            # An explicit ``client`` override owns its own connection;
            # mirror it onto ``self._connection`` so consumer.connection
            # and BatchTransaction still resolve to the lease holder.
            self._connection = _connection_for_lake(self._client.lake)
        try:
            self._retry(self._setup_and_apply_lease_policy)
            self._open_sinks()
            self._opened = True
        except BaseException:
            self._close_sinks_quietly()
            self._close_derived_connection()
            raise
        return self

    def open(self) -> Self:
        """Open this consumer and return it.

        This is the explicit-method form of ``with DMLConsumer(...) as consumer``.
        It is useful when an application needs to swap in a successor consumer
        during a long-running loop.
        """

        return self.__enter__()

    def close(self) -> None:
        """Close this consumer and release resources held by the wrapper."""

        self.__exit__(None, None, None)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._opened = False
        self._close_sinks_quietly()
        self._close_derived_connection()

    def _derive_connection(self) -> Any:
        """Open a fresh DuckDB connection on the lake.

        ``lake.connection.cursor()`` returns a sibling connection in
        duckdb-python: same database, independent transaction state and
        independent ``connection_id`` (and therefore independent CDC
        lease identity). That's exactly what we need for a
        consumer-owned connection.
        """

        base = getattr(self._lake, "connection", None)
        if base is None:
            raise TypeError(
                f"{type(self).__name__} requires `lake.connection` to "
                "derive a dedicated consumer connection (or pass "
                "`connection=` / `client=` explicitly)"
            )
        cursor_factory = getattr(base, "cursor", None)
        if not callable(cursor_factory):
            raise TypeError(
                f"{type(self).__name__} requires `lake.connection.cursor()` "
                "to derive a dedicated consumer connection"
            )
        return cursor_factory()

    def _close_derived_connection(self) -> None:
        if self._derived_connection is None:
            return
        connection = self._derived_connection
        self._derived_connection = None
        close = getattr(connection, "close", None)
        if not callable(close):
            return
        try:
            close()
        except Exception:
            _LOG.exception("error closing derived consumer connection")

    def run(
        self,
        *,
        infinite: bool = True,
        max_batches: int = 0,
        timeout_ms: int = 1_000,
        max_snapshots: int = 100,
        poll_min_ms: int | None = None,
        coalesce: bool | None = None,
        idle_timeout: float = 0.0,
        stop_event: threading.Event | None = None,
        retry: RetryPolicy | None = None,
    ) -> int:
        self._require_open()
        if not self._sinks:
            raise RuntimeError(
                f"{type(self).__name__}.run() requires at least one sink; "
                "use batches() for manual iteration"
            )
        delivered = 0
        for batch in self.batches(
            infinite=infinite,
            max_batches=max_batches,
            timeout_ms=timeout_ms,
            max_snapshots=max_snapshots,
            poll_min_ms=poll_min_ms,
            coalesce=coalesce,
            idle_timeout=idle_timeout,
            stop_event=stop_event,
            retry=retry,
        ):
            self._deliver(batch)
            batch.commit()
            delivered += 1
        return delivered

    def listen(
        self,
        *,
        timeout_ms: int = 1_000,
        max_snapshots: int = 100,
        poll_min_ms: int | None = None,
        coalesce: bool | None = None,
        retry: RetryPolicy | None = None,
    ) -> Any | None:
        """Listen for one batch. Returns ``None`` when no rows are available.

        ``retry`` overrides the consumer's constructor retry policy for this listen only.
        """

        self._require_open()
        rows = self._effective_retry(retry)(
            self._listen_op(timeout_ms, max_snapshots, poll_min_ms, coalesce)
        )
        if not rows:
            return None
        return self._build_batch(rows)

    def read(
        self,
        *,
        max_snapshots: int = 100,
        start_snapshot: int | None = None,
        end_snapshot: int | None = None,
        retry: RetryPolicy | None = None,
    ) -> Any | None:
        """Read one non-blocking batch. Returns ``None`` when no rows are available.

        ``retry`` overrides the consumer's constructor retry policy for this read only.
        """

        self._require_open()
        rows = self._effective_retry(retry)(
            self._read_op(max_snapshots, start_snapshot, end_snapshot)
        )
        if not rows:
            return None
        return self._build_batch(rows)

    def window(self, *, max_snapshots: int = 100) -> ConsumerWindow:
        """Return the consumer's next durable window without materializing rows."""

        self._require_open()
        client = self._require_client()
        return self._retry(lambda: client.cdc_window(self._name, max_snapshots=max_snapshots))

    def batches(
        self,
        *,
        infinite: bool = True,
        max_batches: int = 0,
        timeout_ms: int = 1_000,
        max_snapshots: int = 100,
        poll_min_ms: int | None = None,
        coalesce: bool | None = None,
        idle_timeout: float = 0.0,
        stop_event: threading.Event | None = None,
        drain_event: threading.Event | None = None,
        drain_idle_timeout: float = 2.0,
        retry: RetryPolicy | None = None,
    ) -> Iterator[Any]:
        """Yield batches from the consumer.

        The caller owns the commit boundary. Call ``batch.commit()``
        after successfully processing a yielded batch (or use
        ``batch.transaction()`` for atomic sink-write + commit).

        Shutdown signaling has two flavors:

        - ``stop_event`` is the panic button: when it fires, this
          generator returns *immediately* the next time it checks --
          before the next listen call and right after a listen
          returns. Use it for "operator hit Ctrl-C, abandon any
          in-flight work."
        - ``drain_event`` is the polite request: when it fires, the
          generator keeps polling listen so the consumer drains any
          batches the upstream has committed, then returns when listen
          has been idle for ``drain_idle_timeout`` seconds. Use it for
          "the producer has stopped, let the cursor catch up, then
          we'll join the thread." Combined with ``stop_event``, drain
          becomes a bounded wait: the runner sets ``drain_event``,
          waits a few seconds, then sets ``stop_event`` to force exit
          on any laggard.

        ``idle_timeout`` is the older, drain-event-less knob: when
        ``drain_event`` is ``None``, an idle window of >=
        ``idle_timeout`` seconds also returns. Kept for one-shot
        "consume what's available, then exit" callers. When
        ``drain_event`` is provided, ``drain_idle_timeout`` takes
        precedence (only after ``drain_event`` fires).

        ``retry`` overrides the consumer's constructor ``retry`` policy for
        listen calls in this iterator only (e.g. :func:`no_retry` during
        shutdown drains).
        """

        self._require_open()
        yielded = 0
        last_activity = time.monotonic()
        adaptive_window = (
            _AdaptiveSnapshotWindow(max_snapshots) if self._kind == "dml" else None
        )

        while True:
            if stop_event is not None and stop_event.is_set():
                return
            listen_max_snapshots = (
                adaptive_window.current
                if adaptive_window is not None
                else max_snapshots
            )
            listen_started = time.perf_counter()
            rows = self._effective_retry(retry)(
                self._listen_op(timeout_ms, listen_max_snapshots, poll_min_ms, coalesce)
            )
            listen_elapsed_ms = (time.perf_counter() - listen_started) * 1_000.0
            if not rows:
                if adaptive_window is not None:
                    adaptive_window.observe_empty()
                if stop_event is not None and stop_event.is_set():
                    return
                if not infinite:
                    return
                idle_for = time.monotonic() - last_activity
                # Drain mode: caller has signaled "no more upstream
                # writes are coming; exit once you've been idle long
                # enough to be confident the cursor is caught up."
                if drain_event is not None and drain_event.is_set():
                    if idle_for >= drain_idle_timeout:
                        return
                # Legacy idle_timeout for callers not using drain_event.
                # Kept so existing ``batches(idle_timeout=N)`` users
                # don't have to migrate.
                elif idle_timeout > 0 and idle_for >= idle_timeout:
                    return
                continue

            last_activity = time.monotonic()
            batch = self._build_batch(rows)
            if adaptive_window is not None:
                adaptive_window.observe_batch(
                    row_count=len(rows),
                    snapshot_span=max(1, batch.end_snapshot - batch.start_snapshot + 1),
                    listen_elapsed_ms=listen_elapsed_ms,
                )
            yield batch
            yielded += 1

            if not infinite:
                return
            if max_batches > 0 and yielded >= max_batches:
                return

    def _setup_consumer(self) -> None:
        client = self._require_client()
        name = self._name

        if self._on_exists == "replace":
            self._drop_consumer_if_exists(client)
            self._create_and_position(client)
            return

        try:
            self._create_and_position(client)
            return
        except DuckLakeQueryError as exc:
            if not _is_duplicate_consumer_error(exc, name):
                raise

        if self._on_exists == "error":
            raise RuntimeError(f"consumer {name!r} already exists") from None

        if self._on_exists == "use":
            return

        raise AssertionError(f"unsupported on_exists policy: {self._on_exists!r}")

    def _setup_and_apply_lease_policy(self) -> None:
        self._setup_consumer()
        if self._on_exists == "replace":
            return
        self._apply_lease_policy()

    def _create_and_position(self, client: CDCClient) -> None:
        self._create_consumer(client)

    def _apply_lease_policy(self) -> None:
        client = self._require_client()
        entry = self._lookup_consumer(client)
        if entry is None or not _lease_is_alive(entry):
            return

        if self._lease_policy == "error":
            raise RuntimeError(
                f"consumer {self._name!r} is leased by {entry.owner_token} "
                "and lease_policy='error' was requested"
            )

        if self._lease_policy == "takeover":
            client.cdc_consumer_force_release(self._name)
            return

        deadline = time.monotonic() + self._lease_wait_timeout
        while True:
            entry = self._lookup_consumer(client)
            if entry is None or not _lease_is_alive(entry):
                return
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"timed out after {self._lease_wait_timeout:.1f}s waiting "
                    f"for consumer {self._name!r} lease (held by "
                    f"{entry.owner_token})"
                )
            time.sleep(_LEASE_WAIT_POLL_INTERVAL)

    def _drop_consumer_if_exists(self, client: CDCClient) -> None:
        def ignore_missing(operation: Callable[[], None]) -> None:
            try:
                operation()
            except DuckLakeQueryError as exc:
                if not _is_missing_consumer_error(exc, self._name):
                    raise

        ignore_missing(lambda: client.cdc_consumer_force_release(self._name))
        ignore_missing(lambda: client.cdc_consumer_drop(self._name))

    def _lookup_consumer(self, client: CDCClient) -> ConsumerListEntry | None:
        for entry in client.cdc_list_consumers():
            if entry.consumer_name == self._name:
                return entry
        return None

    def _open_sinks(self) -> None:
        opened: list[Any] = []
        try:
            for sink in self._sinks:
                open_sink(sink)
                opened.append(sink)
        except BaseException:
            for sink in reversed(opened):
                try:
                    close_sink(sink)
                except Exception:
                    _LOG.exception("error closing sink %r during rollback", sink_name(sink))
            raise

    def _close_sinks_quietly(self) -> None:
        for sink in reversed(self._sinks):
            try:
                close_sink(sink)
            except Exception:
                _LOG.exception("error closing sink %r", sink_name(sink))

    def _deliver(self, batch: Any) -> None:
        ctx = SinkContext(
            consumer_name=self._name,
            batch_id=batch.batch_id,
            _heartbeat=self._heartbeat,
        )
        for sink in self._sinks:
            try:
                write_sink(sink, batch, ctx)
            except Exception as exc:
                if sink_required(sink):
                    raise
                _LOG.warning(
                    "optional sink %r raised on batch %s: %s",
                    sink_name(sink),
                    batch.batch_id,
                    exc,
                )

    def _heartbeat(self) -> None:
        client = self._client
        if client is None:
            return
        self._retry(lambda: client.cdc_consumer_heartbeat(self._name))

    def _commit_op(self, snapshot: int) -> Callable[[], object]:
        client = self._require_client()
        name = self._name

        def operation() -> object:
            return client.cdc_commit(name, snapshot)

        return operation

    def _commit_within_op(self, conn: Any, snapshot: int) -> Callable[[], object]:
        """Build a no-arg op that runs ``cdc_commit`` on the caller's connection.

        Used by :meth:`DMLBatch.commit(conn=...)` so the cursor advance
        joins whatever transaction the caller has open on ``conn``.
        Going through :func:`table_function_sql` keeps the rendered SQL
        identical to what :meth:`CDCClient.cdc_commit` emits, so any
        future SQL-level changes (escaping rules, named-arg binding,
        etc.) apply to both paths automatically.
        """

        catalog = self._require_client().catalog
        name = self._name
        sql = table_function_sql("cdc_commit", catalog, name, int(snapshot))

        def operation() -> object:
            cursor = conn.execute(sql)
            fetchone = getattr(cursor, "fetchone", None)
            if callable(fetchone):
                fetchone()
            return None

        return operation

    def commit(self, batch: Any) -> None:
        self._commit_snapshot(batch.end_snapshot)

    def _commit_snapshot(self, snapshot: int) -> None:
        self._retry(self._commit_op(snapshot))

    def _commit_snapshot_within(self, conn: Any, snapshot: int) -> None:
        # Same retry policy as the consumer's own connection path. A
        # transient lock retry inside the caller's BEGIN is safe: the
        # second attempt either succeeds or raises non-transiently, and
        # the caller's exception handler can ROLLBACK and replay the
        # whole batch from listen() either way.
        self._retry(self._commit_within_op(conn, snapshot))

    def _retry(self, operation: Callable[[], T]) -> T:
        return self._retry_policy(operation)  # type: ignore[return-value]

    def _require_open(self) -> None:
        if not self._opened:
            raise RuntimeError(
                f"{type(self).__name__}.run() must be called inside a "
                "`with consumer:` block"
            )

    def _require_client(self) -> CDCClient:
        if self._client is None:
            raise RuntimeError(
                f"{type(self).__name__} client is not initialized; use "
                "`with consumer:`"
            )
        return self._client

    def _create_consumer(self, client: CDCClient) -> None:
        raise NotImplementedError

    def _listen_op(
        self,
        timeout_ms: int,
        max_snapshots: int,
        poll_min_ms: int | None,
        coalesce: bool | None,
    ) -> Callable[[], list[Any]]:
        raise NotImplementedError

    def _read_op(
        self,
        max_snapshots: int,
        start_snapshot: int | None,
        end_snapshot: int | None,
    ) -> Callable[[], list[Any]]:
        raise NotImplementedError

    def _build_batch(self, rows: list[Any]) -> Any:
        raise NotImplementedError

def _lease_is_alive(entry: ConsumerListEntry) -> bool:
    if entry.owner_token is None:
        return False
    if entry.owner_heartbeat_at is None:
        return True

    interval = entry.lease_interval_seconds or 0
    grace = max(_LEASE_FRESHNESS_GRACE_SECONDS, float(interval) * 0.5)
    cutoff_age = float(interval) + grace
    now = datetime.now(UTC)
    heartbeat = entry.owner_heartbeat_at
    if heartbeat.tzinfo is None:
        heartbeat = heartbeat.replace(tzinfo=UTC)
    age = (now - heartbeat).total_seconds()
    return age <= cutoff_age


def _is_duplicate_consumer_error(exc: BaseException, consumer_name: str) -> bool:
    return _consumer_error_contains(exc, consumer_name, "already exists")


def _is_missing_consumer_error(exc: BaseException, consumer_name: str) -> bool:
    return _consumer_error_contains(exc, consumer_name, "does not exist")


def _consumer_error_contains(exc: BaseException, consumer_name: str, needle: str) -> bool:
    current: BaseException | None = exc
    quoted_name = f"consumer '{consumer_name}'"
    while current is not None:
        message = str(current)
        if quoted_name in message and needle in message:
            return True
        current = current.__cause__
    return False
