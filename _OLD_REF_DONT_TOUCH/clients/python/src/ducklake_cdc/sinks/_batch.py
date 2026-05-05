"""Batch rebuilding helpers for sinks."""

from __future__ import annotations

from typing import Any

from ducklake_cdc.types import DDLBatch, DMLBatch


def replace_changes(batch: Any, changes: tuple[Any, ...]) -> Any:
    """Rebuild a batch with a smaller ``changes`` tuple."""

    if isinstance(batch, DMLBatch):
        return DMLBatch(
            consumer_name=batch.consumer_name,
            batch_id=batch.batch_id,
            start_snapshot=batch.start_snapshot,
            end_snapshot=batch.end_snapshot,
            snapshot_ids=batch.snapshot_ids,
            received_at=batch.received_at,
            changes=changes,
        )
    if isinstance(batch, DDLBatch):
        return DDLBatch(
            consumer_name=batch.consumer_name,
            batch_id=batch.batch_id,
            start_snapshot=batch.start_snapshot,
            end_snapshot=batch.end_snapshot,
            snapshot_ids=batch.snapshot_ids,
            received_at=batch.received_at,
            changes=changes,
        )
    raise TypeError(f"unsupported batch type: {type(batch).__name__}")
