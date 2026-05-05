"""JSON line helpers shared by built-in sinks."""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import IO, Any

from ducklake_cdc_client.types import DDLBatch, DMLBatch


def emit_window(stream: IO[str], batch: DMLBatch | DDLBatch, change_count: int) -> None:
    emit(
        stream,
        {
            "type": "window",
            "consumer": batch.consumer_name,
            "batch_id": batch.batch_id,
            "start_snapshot": batch.start_snapshot,
            "end_snapshot": batch.end_snapshot,
            "snapshot_ids": list(batch.snapshot_ids),
            "received_at": batch.received_at.isoformat(),
            "change_count": change_count,
        },
    )


def emit_commit(stream: IO[str], batch: DMLBatch | DDLBatch) -> None:
    emit(
        stream,
        {
            "type": "commit",
            "consumer": batch.consumer_name,
            "batch_id": batch.batch_id,
            "snapshot": batch.end_snapshot,
        },
    )


def emit(stream: IO[str], payload: dict[str, Any]) -> None:
    line = json.dumps(payload, default=_json_default, sort_keys=True)
    stream.write(line + "\n")
    stream.flush()


def _json_default(value: object) -> str:
    if isinstance(value, datetime | date):
        return value.isoformat()
    return str(value)
