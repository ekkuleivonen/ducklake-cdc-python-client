"""Compatibility exports for consumer spawner sinks.

New code should import from :mod:`ducklake_cdc.sinks`.
"""

from ducklake_cdc.sinks import ConsumerSpawner

__all__ = ["ConsumerSpawner"]
