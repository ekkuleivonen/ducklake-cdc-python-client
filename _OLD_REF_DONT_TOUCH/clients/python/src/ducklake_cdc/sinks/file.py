"""File sinks."""

from __future__ import annotations

from contextlib import suppress
from pathlib import Path
from typing import IO

from ducklake_cdc.sinks._json import emit, emit_commit, emit_window
from ducklake_cdc.sinks.base import BaseDDLSink, BaseDMLSink
from ducklake_cdc.types import DDLBatch, DMLBatch, SinkContext


class _FileSinkBase:
    name = "file"
    require_ack = True

    def __init__(self, path: str | Path, *, mode: str = "a", encoding: str = "utf-8") -> None:
        if "b" in mode:
            raise ValueError(
                "FileDMLSink/FileDDLSink write text JSON Lines; pass a text mode"
            )
        self._path = Path(path)
        self._mode = mode
        self._encoding = encoding
        self._fh: IO[str] | None = None

    def open(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self._path.open(self._mode, encoding=self._encoding)

    def close(self) -> None:
        if self._fh is not None:
            with suppress(Exception):
                self._fh.flush()
            self._fh.close()
            self._fh = None

    def _require_fh(self) -> IO[str]:
        if self._fh is None:
            raise RuntimeError(
                f"{type(self).__name__} is not open; use it inside a `with "
                "consumer:` block"
            )
        return self._fh


class FileDMLSink(_FileSinkBase, BaseDMLSink):
    """Append each DML batch as JSON lines to ``path``."""

    name = "file"

    def write(self, batch: DMLBatch, ctx: SinkContext) -> None:
        fh = self._require_fh()
        emit_window(fh, batch, len(batch))
        for change in batch:
            payload = change.to_dict()
            payload["type"] = "change"
            emit(fh, payload)
        emit_commit(fh, batch)


class FileDDLSink(_FileSinkBase, BaseDDLSink):
    """Append each DDL batch as JSON lines to ``path``."""

    name = "file"

    def write(self, batch: DDLBatch, ctx: SinkContext) -> None:
        fh = self._require_fh()
        emit_window(fh, batch, len(batch))
        for event in batch:
            payload = event.to_dict()
            payload["type"] = "schema_change"
            emit(fh, payload)
        emit_commit(fh, batch)
