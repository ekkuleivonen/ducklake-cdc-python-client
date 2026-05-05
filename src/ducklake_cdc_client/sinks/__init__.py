"""Built-in sinks for the high-level CDC client."""

from ducklake_cdc_client.sinks.base import (
    BaseDDLSink,
    BaseDDLTickSink,
    BaseDMLSink,
    BaseDMLTickSink,
    DDLSink,
    DDLTickSink,
    DMLSink,
    DMLTickSink,
    SinkBatchKind,
)
from ducklake_cdc_client.sinks.callable import CallableDDLSink, CallableDMLSink
from ducklake_cdc_client.sinks.fanout import FanoutDDLSink, FanoutDMLSink
from ducklake_cdc_client.sinks.file import FileDDLSink, FileDMLSink
from ducklake_cdc_client.sinks.filter import FilterDDLSink, FilterDMLSink
from ducklake_cdc_client.sinks.map import MapDDLSink, MapDMLSink
from ducklake_cdc_client.sinks.memory import MemoryDDLSink, MemoryDMLSink
from ducklake_cdc_client.sinks.stdout import StdoutDDLSink, StdoutDMLSink

__all__ = [
    "BaseDDLSink",
    "BaseDDLTickSink",
    "BaseDMLSink",
    "BaseDMLTickSink",
    "CallableDDLSink",
    "CallableDMLSink",
    "ConsumerSpawner",
    "DDLSink",
    "DDLTickSink",
    "DMLSink",
    "DMLTickSink",
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
    "SinkBatchKind",
    "StdoutDDLSink",
    "StdoutDMLSink",
]


def __getattr__(name: str) -> object:
    if name == "ConsumerSpawner":
        from ducklake_cdc_client.sinks.consumer_spawner import ConsumerSpawner

        return ConsumerSpawner
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
