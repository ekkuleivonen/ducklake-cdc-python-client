"""Compatibility exports for consumer spawner sinks.

New code should import from :mod:`ducklake_cdc_client.sinks`.
"""

from ducklake_cdc_client.sinks import ConsumerSpawner

__all__ = ["ConsumerSpawner"]
