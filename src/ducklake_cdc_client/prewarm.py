"""Cheap CDC scalar used to mitigate first-call bootstrap races (H-022).

Run :func:`prewarm` on any DuckDB connection (or cursor) that will execute
``cdc_*`` table functions, **after** ``LOAD ducklake_cdc``. Derived cursors need
their own prewarm if they reach CDC before the parent connection has been
warmed.

See ``ducklake-cdc-extension/docs/hazard-log.md`` (H-022) and
:func:`ducklake_cdc_client.retry.retry_on_transient`.
"""

from __future__ import annotations

from typing import Any


def prewarm(connection: Any) -> str:
    """Execute ``SELECT cdc_version()`` on ``connection`` and return the version string.

    No-op aside from running the scalar — intended ordering is: attach DuckLake,
    ``LOAD ducklake_cdc``, then :func:`prewarm` before other ``cdc_*`` calls on
    this handle.
    """

    try:
        cursor = connection.execute("SELECT cdc_version()")
        fetchone = getattr(cursor, "fetchone", None)
        if callable(fetchone):
            row = fetchone()
        else:
            row = None
    except Exception as exc:
        raise RuntimeError(
            "cdc prewarm failed (is ducklake_cdc LOADed on this connection?)"
        ) from exc

    if row is None or len(row) < 1:
        raise RuntimeError("cdc_version() returned no value")
    return str(row[0])
