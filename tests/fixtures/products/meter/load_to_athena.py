"""Load the smart_metering raw seed CSVs into S3 + register Athena external tables.

Runnable in shape but NOT executed in CI: it needs live AWS credentials
(``AWS_PROFILE`` / instance role) with write access to the data-lake bucket and
permission to run Athena DDL. Run after ``python gen_data.py``:

    python data_products/meter/load_to_athena.py

For each raw table it:
  1. reads the generated CSV from data/,
  2. writes it as parquet to s3://utility-datalake/meter/raw/<table>/,
  3. creates (or replaces) the Athena external table over that prefix.
"""
from __future__ import annotations

import os

import awswrangler as wr  # noqa: F401  (requires AWS creds at runtime)
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(HERE, "data")

DATABASE = "meter_raw"
S3_BASE = "s3://utility-datalake/meter/raw"

# raw table -> (csv file, Athena column DDL)
RAW_TABLES = {
    "interval_reads": {
        "csv": "interval_reads.csv",
        "columns": {
            "meter_id": "bigint",
            "read_ts": "timestamp",
            "kwh": "double",
            "read_date": "date",
        },
        "partition_by": "read_date",
    },
}


def _ddl(table: str, columns: dict[str, str], path: str) -> str:
    cols = ",\n  ".join(f"{name} {dtype}" for name, dtype in columns.items())
    return (
        f"CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.{table} (\n  {cols}\n)\n"
        f"STORED AS PARQUET\n"
        f"LOCATION '{path}'"
    )


def main() -> None:
    for table, spec in RAW_TABLES.items():
        df = pd.read_csv(os.path.join(DATA_DIR, spec["csv"]))
        path = f"{S3_BASE}/{table}/"

        wr.s3.to_parquet(
            df=df,
            path=path,
            dataset=True,
            mode="overwrite",
            partition_cols=[spec["partition_by"]],
        )

        ddl = _ddl(table, spec["columns"], path)
        wr.athena.start_query_execution(
            sql=ddl,
            database=DATABASE,
            wait=True,
        )
        print(f"loaded {len(df)} rows -> {path} and registered {DATABASE}.{table}")


if __name__ == "__main__":
    main()
