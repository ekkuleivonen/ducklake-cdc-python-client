"""Transient-error retry helpers for the ducklake-cdc client.

These mirror the empirical retry recipe documented in H-022 of
``ducklake-cdc-extension/docs/hazard-log.md``. Two transient failure
modes are known to surface around the first cdc_* call against a fresh
catalog in a process:

1. ``database is locked`` from SQLite-backed catalogs while another
   connection on the same file is writing the metadata schema. The
   ``META_BUSY_TIMEOUT`` ATTACH option already absorbs short bursts;
   anything longer becomes a transient error here.

2. ``thread::join failed: <errno>`` from DuckDB's parallel pipeline
   teardown when an exception races with worker join on the H-022
   first-bootstrap path. macOS surfaces both ``Resource deadlock
   avoided`` (EDEADLK) and ``Invalid argument`` (EINVAL) for the same
   underlying race.

Both are retryable on the same connection most of the time: the second
attempt finds the catalog already bootstrapped and avoids the racy
first-time mutex re-entry. There is a residual ~5-10% on inline-DuckDB
catalogs where the first failure poisons the connection's worker pool
and same-connection retries also fail; the durable recovery for that
shape is to recreate the ``DuckLake`` instance (which the high-level
consumers do automatically when they're re-entered into a new
``with`` block).

Default exposure: high-level consumers use :func:`retry_on_transient`
unless the caller passes ``retry=...`` explicitly (including per-call
``retry`` on :meth:`DMLConsumer.listen`, :meth:`DMLConsumer.read`, and
:meth:`DMLConsumer.batches`). Callers that prefer
no retry (e.g. tests asserting an exception propagates) can opt out
with ``retry=no_retry``.

Related mitigation for the first-bootstrap mutex race: run
:func:`ducklake_cdc_client.prewarm` (or use :class:`~ducklake_cdc_client.client.client.CDCClient`
with default ``prewarm_connection=True``) on each DuckDB handle after ``LOAD ducklake_cdc``.
See ``ducklake-cdc-extension/docs/hazard-log.md`` (H-022).
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")

#: Floor on retry sleep. Conservative — H-022 races are sub-millisecond
#: in practice so anything > 0 is enough to let the racing pipeline
#: finish teardown.
_DEFAULT_BASE_SLEEP_S = 0.2

#: Cap on attempts. Three is enough for the documented bootstrap race
#: (the second attempt always finds a warm catalog) with one spare for
#: the unrelated SQLite-busy case.
_DEFAULT_MAX_ATTEMPTS = 5


def is_transient_error(exc: BaseException) -> bool:
    """True if ``exc`` is a known transient that benefits from a retry.

    Walks ``__cause__`` so wrapped DuckLake/DuckDB exceptions are
    recognised even after the python client repackages them.
    """

    current: BaseException | None = exc
    while current is not None:
        message = str(current).lower()
        if "database is locked" in message:
            return True
        if "thread::join failed" in message:
            # Matches both ``Resource deadlock avoided`` (EDEADLK) and
            # ``Invalid argument`` (EINVAL). Both are pthread_join
            # surfaces of the H-022 first-bootstrap mutex race.
            return True
        current = current.__cause__
    return False


def retry_on_transient(
    operation: Callable[[], T],
    *,
    max_attempts: int = _DEFAULT_MAX_ATTEMPTS,
    base_sleep_s: float = _DEFAULT_BASE_SLEEP_S,
) -> T:
    """Run ``operation`` and retry on known transient errors.

    Retries up to ``max_attempts`` times with a short fixed sleep
    between tries. Non-transient exceptions propagate on the first
    occurrence. The last attempt re-raises whatever the operation
    threw so the caller still sees the underlying error if the race
    keeps firing.
    """

    if max_attempts < 1:
        raise ValueError("max_attempts must be >= 1")
    last_exc: BaseException | None = None
    for attempt in range(max_attempts):
        try:
            return operation()
        except BaseException as exc:  # noqa: BLE001 - filter via predicate
            if not is_transient_error(exc):
                raise
            last_exc = exc
            if attempt + 1 >= max_attempts:
                break
            time.sleep(base_sleep_s)
    assert last_exc is not None  # loop only exits via raise/return/break
    raise last_exc


def no_retry(operation: Callable[[], T]) -> T:
    """Pass-through ``RetryPolicy`` for callers opting out of retries."""

    return operation()


__all__ = [
    "is_transient_error",
    "no_retry",
    "retry_on_transient",
]
