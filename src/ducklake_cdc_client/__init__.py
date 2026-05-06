"""Python client for the ducklake-cdc DuckDB extension.

The headline surface is consumers plus small sink helpers:

    from ducklake_cdc_client import DMLConsumer

    with DMLConsumer(lake, "orders", table="public.orders", mode="changes") as c:
        for batch in c.batches():
            process(batch)
            batch.commit()

Any callable accepting ``batch`` or ``batch, ctx`` can be used as a sink. Sink
objects can optionally expose ``open`` / ``close`` lifecycle hooks and a
``required`` flag; pass sinks when you want ``consumer.run()`` to own the
delivery loop.

The 1:1 mirror of the SQL extension surface lives at
``ducklake_cdc_client.client.CDCClient``. Reach for it when you need raw access
to the extension's table functions; use the high-level consumers for batch
iteration or sink-driven delivery.
"""

from ducklake_cdc_client._version import __version__
from ducklake_cdc_client.app import CDCApp, ConsumerHealth
from ducklake_cdc_client.client import CDCClient, SchemaDiffRow
from ducklake_cdc_client.consumers import DDLConsumer, DMLConsumer, RetryPolicy
from ducklake_cdc_client.enums import (
    ChangeType,
    DdlEventKind,
    DdlObjectKind,
    DiagnosticSeverity,
    EventCategory,
    ScopeKind,
    SubscriptionStatus,
)
from ducklake_cdc_client.prewarm import prewarm
from ducklake_cdc_client.retry import is_transient_error, no_retry, retry_on_transient
from ducklake_cdc_client.sinks import Batch, Sink, SinkLike, StdoutSink, sink
from ducklake_cdc_client.types import (
    BatchTransaction,
    Change,
    DDLBatch,
    DDLTick,
    DDLTickBatch,
    DMLBatch,
    DMLTick,
    DMLTickBatch,
    SchemaChange,
    SinkContext,
)

__all__ = [
    "Batch",
    "BatchTransaction",
    "CDCApp",
    "CDCClient",
    "Change",
    "ChangeType",
    "ConsumerHealth",
    "DDLBatch",
    "DDLConsumer",
    "DDLTick",
    "DDLTickBatch",
    "DdlEventKind",
    "DdlObjectKind",
    "DiagnosticSeverity",
    "DMLBatch",
    "DMLConsumer",
    "DMLTick",
    "DMLTickBatch",
    "EventCategory",
    "RetryPolicy",
    "SchemaChange",
    "SchemaDiffRow",
    "ScopeKind",
    "Sink",
    "SinkContext",
    "SinkLike",
    "StdoutSink",
    "SubscriptionStatus",
    "__version__",
    "is_transient_error",
    "no_retry",
    "prewarm",
    "retry_on_transient",
    "sink",
]
