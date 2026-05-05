# ducklake-cdc-client

Python client helpers for the `ducklake_cdc` DuckDB community extension.

The package gives you two layers:

- `CDCClient`: a direct Python wrapper over the extension's SQL table functions.
- `DMLConsumer` / `DDLConsumer`: durable consumers that yield batches and commit only after
  your code has processed them.

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

## Batch iteration

```python
from ducklake_client import DiskStorage, DuckDBCatalog, DuckLake
from ducklake_cdc_client import DMLConsumer

with DuckLake(
    catalog=DuckDBCatalog("metadata.ducklake"),
    storage=DiskStorage("data"),
) as lake:
    with DMLConsumer(
        lake,
        "orders-consumer",
        table="main.orders",
        mode="changes",
    ) as consumer:
        for batch in consumer.batches(infinite=False):
            for change in batch:
                print(change.to_dict())
            batch.commit()
```

`batch.commit()` advances the durable consumer cursor. If processing raises before that call,
the same batch can be read again on the next run.

## Sink-driven usage

If you prefer a push style, pass sinks and let `consumer.run()` deliver and commit for you.

```python
from ducklake_client import DiskStorage, DuckDBCatalog, DuckLake
from ducklake_cdc_client import DMLConsumer, StdoutSink

with DuckLake(
    catalog=DuckDBCatalog("metadata.ducklake"),
    storage=DiskStorage("data"),
) as lake:
    with DMLConsumer(
        lake,
        "orders-consumer",
        table="main.orders",
        mode="changes",
        sinks=[StdoutSink()],
    ) as consumer:
        consumer.run(infinite=False)
```

## Demo

Run the local demo:

```sh
uv run python demo.py
```

The demo creates a local DuckLake catalog under `.demo/`, inserts one row into `main.orders`,
prints the emitted CDC change batch, and commits it.
