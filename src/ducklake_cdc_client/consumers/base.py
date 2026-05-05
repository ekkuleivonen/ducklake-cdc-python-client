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

from ducklake_cdc_client.client import CDCClient, ConsumerListEntry
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

        self._lake = lake
        self._name = name
        self._start_at = start_at
        self._mode: ConsumerMode = mode
        self._on_exists = on_exists
        self._lease_policy: LeasePolicy = lease_policy
        self._lease_wait_timeout = lease_wait_timeout
        self._sinks: list[Sink] = [as_sink(item) for item in sinks]
        self._client = client
        self._retry_policy = retry
        self._opened = False

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

    def __enter__(self) -> Self:
        if self._client is None:
            self._client = CDCClient(self._lake)
        try:
            self._retry(self._setup_and_apply_lease_policy)
            self._open_sinks()
            self._opened = True
        except BaseException:
            self._close_sinks_quietly()
            raise
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._opened = False
        self._close_sinks_quietly()

    def run(
        self,
        *,
        infinite: bool = True,
        max_batches: int = 0,
        timeout_ms: int = 1_000,
        max_snapshots: int = 100,
        idle_timeout: float = 0.0,
        stop_event: threading.Event | None = None,
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
            idle_timeout=idle_timeout,
            stop_event=stop_event,
        ):
            self._deliver(batch)
            batch.commit()
            delivered += 1
        return delivered

    def listen(self, *, timeout_ms: int = 1_000, max_snapshots: int = 100) -> Any | None:
        """Listen for one batch. Returns ``None`` when no rows are available."""

        self._require_open()
        rows = self._retry(self._listen_op(timeout_ms, max_snapshots))
        if not rows:
            return None
        return self._build_batch(rows)

    def read(
        self,
        *,
        max_snapshots: int = 100,
        start_snapshot: int | None = None,
        end_snapshot: int | None = None,
    ) -> Any | None:
        """Read one non-blocking batch. Returns ``None`` when no rows are available."""

        self._require_open()
        rows = self._retry(self._read_op(max_snapshots, start_snapshot, end_snapshot))
        if not rows:
            return None
        return self._build_batch(rows)

    def batches(
        self,
        *,
        infinite: bool = True,
        max_batches: int = 0,
        timeout_ms: int = 1_000,
        max_snapshots: int = 100,
        idle_timeout: float = 0.0,
        stop_event: threading.Event | None = None,
    ) -> Iterator[Any]:
        """Yield batches from the consumer.

        The caller owns the commit boundary. Call ``batch.commit()`` after
        successfully processing a yielded batch.
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
            rows = self._retry(self._listen_op(timeout_ms, listen_max_snapshots))
            listen_elapsed_ms = (time.perf_counter() - listen_started) * 1_000.0
            if not rows:
                if adaptive_window is not None:
                    adaptive_window.observe_empty()
                if stop_event is not None and stop_event.is_set():
                    return
                if not infinite:
                    return
                if idle_timeout > 0 and (time.monotonic() - last_activity) >= idle_timeout:
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
        if self._start_at != "now":
            client.cdc_consumer_reset(self._name, to_snapshot=self._start_at)

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

    def commit(self, batch: Any) -> None:
        self._commit_snapshot(batch.end_snapshot)

    def _commit_snapshot(self, snapshot: int) -> None:
        self._retry(self._commit_op(snapshot))

    def _retry(self, operation: Callable[[], T]) -> T:
        if self._retry_policy is None:
            return operation()
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

    def _listen_op(self, timeout_ms: int, max_snapshots: int) -> Callable[[], list[Any]]:
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
