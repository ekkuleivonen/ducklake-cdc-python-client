"""Stdout sinks."""

from __future__ import annotations

import sys
from typing import IO

from ducklake_cdc_client.sinks._json import emit, emit_commit, emit_window
from ducklake_cdc_client.sinks.base import BaseDDLSink, BaseDMLSink
from ducklake_cdc_client.types import DDLBatch, DMLBatch, SinkContext


class StdoutDMLSink(BaseDMLSink):
    """Emit each DML batch as JSON lines to stdout."""

    name = "stdout"
    require_ack = True

    def __init__(self, *, stream: IO[str] | None = None) -> None:
        self._stream = stream if stream is not None else sys.stdout

    def write(self, batch: DMLBatch, ctx: SinkContext) -> None:
        emit_window(self._stream, batch, len(batch))
        for change in batch:
            payload = change.to_dict()
            payload["type"] = "change"
            emit(self._stream, payload)
        emit_commit(self._stream, batch)


class StdoutDDLSink(BaseDDLSink):
    """Emit each DDL batch as JSON lines to stdout."""

    name = "stdout"
    require_ack = True

    def __init__(self, *, stream: IO[str] | None = None) -> None:
        self._stream = stream if stream is not None else sys.stdout

    def write(self, batch: DDLBatch, ctx: SinkContext) -> None:
        emit_window(self._stream, batch, len(batch))
        for event in batch:
            payload = event.to_dict()
            payload["type"] = "schema_change"
            emit(self._stream, payload)
        emit_commit(self._stream, batch)
