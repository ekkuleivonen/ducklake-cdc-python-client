"""Stdout sinks."""

from __future__ import annotations

import sys
from typing import IO

from ducklake_cdc_client.sinks._json import emit, emit_commit, emit_window
from ducklake_cdc_client.sinks.base import Batch
from ducklake_cdc_client.types import Change, SchemaChange, SinkContext


class StdoutSink:
    """Emit each batch as JSON lines."""

    name = "stdout"
    required = True

    def __init__(self, *, stream: IO[str] | None = None) -> None:
        self._stream = stream if stream is not None else sys.stdout

    def write(self, batch: Batch, ctx: SinkContext) -> None:
        emit_window(self._stream, batch, len(batch))
        for item in batch:
            payload = item.to_dict()
            if isinstance(item, Change):
                payload["type"] = "change"
            elif isinstance(item, SchemaChange):
                payload["type"] = "schema_change"
            else:
                payload["type"] = "tick"
            emit(self._stream, payload)
        emit_commit(self._stream, batch)
