"""Built-in sinks for the high-level CDC client."""

from ducklake_cdc_client.sinks.base import (
    Batch,
    Sink,
    SinkLike,
    sink,
)
from ducklake_cdc_client.sinks.stdout import StdoutSink

__all__ = [
    "Batch",
    "Sink",
    "SinkLike",
    "StdoutSink",
    "sink",
]
