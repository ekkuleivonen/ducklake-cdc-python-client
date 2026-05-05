"""Compatibility exports for sink combinators.

New code should import from :mod:`ducklake_cdc.sinks`.
"""

from ducklake_cdc.sinks import (
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
