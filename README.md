# ducklake-cdc-client

Python client helpers for the `ducklake_cdc` DuckDB community extension.

## Install

```sh
pip install ducklake-cdc-client
```

The package uses [`ducklake-client`](https://pypi.org/project/ducklake-client/) for DuckLake
connections. `CDCClient` installs and loads the DuckDB community extension on first use:

```sql
INSTALL ducklake_cdc FROM community;
LOAD ducklake_cdc;
```

## Example

```python
from ducklake_client import DiskStorage, DuckDBCatalog, DuckLake
from ducklake_cdc_client import DMLConsumer, StdoutDMLSink

with DuckLake(
    catalog=DuckDBCatalog("metadata.ducklake"),
    storage=DiskStorage("data"),
) as lake:
    with DMLConsumer(
        lake,
        "orders-consumer",
        table="main.orders",
        mode="changes",
        sinks=[StdoutDMLSink()],
    ) as consumer:
        consumer.run(infinite=False)
```
