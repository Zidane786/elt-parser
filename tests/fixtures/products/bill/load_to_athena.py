"""Load the billing RAW-layer fixture CSVs into Athena (S3 + Glue catalog).

REQUIRES AWS CREDENTIALS (env / instance profile) and the optional `aws`
extras installed:

    uv pip install -e '.[aws]'
    python data_products/bill/load_to_athena.py

For each raw table this:
  1. reads the seed CSV from data/,
  2. writes parquet to s3://utility-datalake/bill/raw/<table>/,
  3. creates the corresponding Athena external table.

Runnable in shape; not executed in CI (no creds there).
"""
from __future__ import annotations

import os

import pandas as pd

try:
    import awswrangler as wr
except ImportError as exc:  # pragma: no cover - aws extra not installed
    raise SystemExit("install the 'aws' extra: uv pip install -e '.[aws]'") from exc

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(HERE, "data")

DATABASE = "bill_raw"
S3_PREFIX = "s3://utility-datalake/bill/raw"

# Raw tables to publish: csv seed -> athena external table name.
RAW_TABLES = {
    "invoices": "billing_pg.invoices.csv",
}


def main() -> None:
    wr.catalog.create_database(name=DATABASE, exist_ok=True)

    for table, csv_name in RAW_TABLES.items():
        df = pd.read_csv(os.path.join(DATA_DIR, csv_name))
        path = f"{S3_PREFIX}/{table}/"

        # Write parquet and (re)create the Athena external table in one call.
        wr.s3.to_parquet(
            df=df,
            path=path,
            dataset=True,
            mode="overwrite",
            database=DATABASE,
            table=table,
        )
        print(f"loaded {len(df)} rows -> {path} ({DATABASE}.{table})")


if __name__ == "__main__":
    main()
