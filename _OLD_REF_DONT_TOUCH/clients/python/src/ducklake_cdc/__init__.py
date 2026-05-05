"""Python client for the ducklake-cdc DuckDB extension.

The headline surface is the high-level consumers plus sink protocols:

    from ducklake_cdc import DMLConsumer, StdoutDMLSink

    with DMLConsumer(
        lake, "orders", table="public.orders", mode="changes", sinks=[StdoutDMLSink()]
    ) as c:
        c.run()

DDL events flow through the parallel :class:`DDLConsumer` / :class:`DDLSink`
surface. Built-in sinks (``Stdout*``, ``File*``, ``Memory*``, ``Callable*``)
and combinators (``Map*``, ``Filter*``, ``Fanout*``) are dependency-light;
network/IO sinks live in separate distributions.

The low-level 1:1 mirror of the SQL extension surface lives at
``ducklake_cdc.lowlevel.CDCClient``. Reach for it when you need raw access
to the extension's table functions; use the high-level consumers for the
listen + deliver + commit loop.
"""

from ducklake_cdc._version import __version__
from ducklake_cdc.app import CDCApp, ConsumerHealth
from ducklake_cdc.consumers import DDLConsumer, DMLConsumer
from ducklake_cdc.enums import (
    ChangeType,
    DdlEventKind,
    DdlObjectKind,
    DiagnosticSeverity,
    EventCategory,
    ScopeKind,
    SubscriptionStatus,
)
from ducklake_cdc.sinks import (
    BaseDDLSink,
    BaseDDLTickSink,
    BaseDMLSink,
    BaseDMLTickSink,
    CallableDDLSink,
    CallableDMLSink,
    ConsumerSpawner,
    DDLSink,
    DDLTickSink,
    DMLSink,
    DMLTickSink,
    FanoutDDLSink,
    FanoutDMLSink,
    FileDDLSink,
    FileDMLSink,
    FilterDDLSink,
    FilterDMLSink,
    MapDDLSink,
    MapDMLSink,
    MemoryDDLSink,
    MemoryDMLSink,
    SinkBatchKind,
    StdoutDDLSink,
    StdoutDMLSink,
)
from ducklake_cdc.types import (
    Change,
    DDLBatch,
    DDLTick,
    DDLTickBatch,
    DMLBatch,
    DMLTick,
    DMLTickBatch,
    SchemaChange,
    SinkAck,
    SinkContext,
)

__all__ = [
    "BaseDDLSink",
    "BaseDDLTickSink",
    "BaseDMLSink",
    "BaseDMLTickSink",
    "CallableDDLSink",
    "CallableDMLSink",
    "CDCApp",
    "Change",
    "ChangeType",
    "ConsumerHealth",
    "ConsumerSpawner",
    "DDLBatch",
    "DDLConsumer",
    "DDLTick",
    "DDLTickBatch",
    "DDLTickSink",
    "DDLSink",
    "DdlEventKind",
    "DdlObjectKind",
    "DiagnosticSeverity",
    "DMLBatch",
    "DMLConsumer",
    "DMLTick",
    "DMLTickBatch",
    "DMLTickSink",
    "DMLSink",
    "EventCategory",
    "FanoutDDLSink",
    "FanoutDMLSink",
    "FileDDLSink",
    "FileDMLSink",
    "FilterDDLSink",
    "FilterDMLSink",
    "MapDDLSink",
    "MapDMLSink",
    "MemoryDDLSink",
    "MemoryDMLSink",
    "SchemaChange",
    "SinkBatchKind",
    "ScopeKind",
    "SinkAck",
    "SinkContext",
    "StdoutDDLSink",
    "StdoutDMLSink",
    "SubscriptionStatus",
    "__version__",
]
