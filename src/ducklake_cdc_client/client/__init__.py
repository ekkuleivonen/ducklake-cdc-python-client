"""Client primitives for the ducklake-cdc extension."""

from ducklake_cdc_client.client.client import (
    CDCClient,
    ChangeRow,
    ConsumerCommit,
    ConsumerListEntry,
    ConsumerWindow,
    DDLTickRow,
    DMLTickRow,
    SchemaChangeRow,
)

__all__ = [
    "CDCClient",
    "ChangeRow",
    "ConsumerCommit",
    "ConsumerListEntry",
    "ConsumerWindow",
    "DDLTickRow",
    "DMLTickRow",
    "SchemaChangeRow",
]
