"""Compatibility exports for sink combinators.

New code should import from :mod:`ducklake_cdc_client.sinks`.
"""

from ducklake_cdc_client.sinks import (
    FanoutDDLSink,
    FanoutDMLSink,
    FilterDDLSink,
    FilterDMLSink,
    MapDDLSink,
    MapDMLSink,
)

__all__ = [
    "FanoutDDLSink",
    "FanoutDMLSink",
    "FilterDDLSink",
    "FilterDMLSink",
    "MapDDLSink",
    "MapDMLSink",
]
