from __future__ import annotations

import shutil
import threading
import time
from pathlib import Path

from ducklake_client import ColumnDef, DiskStorage, DuckLake, SqliteCatalog

from ducklake_cdc_client import DMLConsumer

DEMO_DIR = Path(".demo")

def main() -> None:
    lake = DuckLake(
        catalog=SqliteCatalog(DEMO_DIR / "metadata.sqlite"),
        storage=DiskStorage(DEMO_DIR / "data"),
    )
    consumer = DMLConsumer(
        lake,
        "orders-consumer",
        table="main.orders",
        mode="changes",
    )
    with lake, consumer:
        for batch in consumer.batches():
            for change in batch:
                res = change.to_dict()
                values = res.get('values', {})
                text = f"{res['snapshot_id']} {res['kind']} {res['table']} {values.get('description', '')}"
                print(text)
            batch.commit()

def producer() -> None:
    lake = DuckLake(
        catalog=SqliteCatalog(DEMO_DIR / "metadata.sqlite"),
        storage=DiskStorage(DEMO_DIR / "data"),
    )
    with lake:
        for idx in range(1, 101):
            lake.connection.execute("INSERT INTO lake.main.orders VALUES (?, ?)", [idx, f"order {idx}"])
            time.sleep(0.25)

if __name__ == "__main__":
    shutil.rmtree(DEMO_DIR, ignore_errors=True)
    DEMO_DIR.mkdir()
    with DuckLake(
        catalog=SqliteCatalog(DEMO_DIR / "metadata.sqlite"),
        storage=DiskStorage(DEMO_DIR / "data"),
    ) as lake:
        lake.table.create(
            "orders",
            id=ColumnDef("BIGINT"),
            description=ColumnDef("VARCHAR"),
        )
    threading.Thread(target=producer).start()
    main()
