"""Fanout sink combinators."""

from __future__ import annotations

import logging

from ducklake_cdc.sinks._inner import close_all, inner_name, open_all
from ducklake_cdc.sinks.base import BaseDDLSink, BaseDMLSink, DDLSink, DMLSink
from ducklake_cdc.types import DDLBatch, DMLBatch, SinkContext

_LOG = logging.getLogger(__name__)


class FanoutDMLSink(BaseDMLSink):
    """Broadcast each batch to every inner sink."""

    def __init__(
        self,
        *sinks: DMLSink,
        name: str | None = None,
    ) -> None:
        if not sinks:
            raise ValueError("FanoutDMLSink requires at least one inner sink")
        self._inner: tuple[DMLSink, ...] = sinks
        self.name = name or f"fanout({','.join(inner_name(s) for s in sinks)})"
        self.require_ack = any(getattr(s, "require_ack", True) for s in sinks)

    def open(self) -> None:
        open_all(self._inner)

    def close(self) -> None:
        close_all(self._inner)

    def write(self, batch: DMLBatch, ctx: SinkContext) -> None:
        for sink in self._inner:
            try:
                sink.write(batch, ctx)
            except Exception as exc:
                if getattr(sink, "require_ack", True):
                    raise
                _LOG.warning(
                    "optional fanout sink %r raised on batch %s: %s",
                    inner_name(sink),
                    batch.batch_id,
                    exc,
                )


class FanoutDDLSink(BaseDDLSink):
    """Broadcast each DDL batch to every inner sink."""

    def __init__(
        self,
        *sinks: DDLSink,
        name: str | None = None,
    ) -> None:
        if not sinks:
            raise ValueError("FanoutDDLSink requires at least one inner sink")
        self._inner: tuple[DDLSink, ...] = sinks
        self.name = name or f"fanout({','.join(inner_name(s) for s in sinks)})"
        self.require_ack = any(getattr(s, "require_ack", True) for s in sinks)

    def open(self) -> None:
        open_all(self._inner)

    def close(self) -> None:
        close_all(self._inner)

    def write(self, batch: DDLBatch, ctx: SinkContext) -> None:
        for sink in self._inner:
            try:
                sink.write(batch, ctx)
            except Exception as exc:
                if getattr(sink, "require_ack", True):
                    raise
                _LOG.warning(
                    "optional fanout sink %r raised on batch %s: %s",
                    inner_name(sink),
                    batch.batch_id,
                    exc,
                )
