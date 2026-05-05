"""Compatibility exports for high-level consumers.

New code should import from :mod:`ducklake_cdc.consumers`.
"""

from ducklake_cdc.consumers import (
    ConsumerMode,
    DDLConsumer,
    DMLConsumer,
    LeasePolicy,
    OnExists,
    RetryPolicy,
    StartAt,
    _AdaptiveSnapshotWindow,
    _ConsumerBase,
    _lease_is_alive,
)

__all__ = [
    "ConsumerMode",
    "DDLConsumer",
    "DMLConsumer",
    "LeasePolicy",
    "OnExists",
    "RetryPolicy",
    "StartAt",
    "_AdaptiveSnapshotWindow",
    "_ConsumerBase",
    "_lease_is_alive",
]
