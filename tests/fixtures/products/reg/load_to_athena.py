"""Load the regulatory_reporting raw seed CSVs into S3 + register Athena tables.

Runnable in shape, NOT executed here: it requires live AWS credentials (S3 write
+ Athena/Glue catalog access). Run after `python gen_data.py`:

    AWS_PROFILE=... python load_to_athena.py

For each raw table it:
  1. reads the seed CSV from ./data/<table>.csv,
  2. writes it as parquet to s3://utility-datalake/reg/raw/<table>/,
  3. creates the Athena external table over that S3 prefix.
"""
from __future__ import annotations

import os

import awswrangler as wr  # requires the [load] extra: awswrangler, boto3, pandas
import pandas as pd

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
S3_BASE = "s3://utility-datalake/reg/raw"
RAW_DATABASE = "reg_raw"

# Athena column types per raw table (mirrors registry schema for reg_raw).
RAW_TABLES = {
    "reporting_calendar": {
        "csv": "reporting_calendar.csv",
        "columns": {
            "period": "string",
            "period_type": "string",
            "start_date": "date",
            "end_date": "date",
        },
        "partition": None,
    },
}


def load_table(table: str, spec: dict) -> None:
    csv_path = os.path.join(DATA_DIR, spec["csv"])
    df = pd.read_csv(csv_path)
    path = f"{S3_BASE}/{table}/"

    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="overwrite",
        database=RAW_DATABASE,
        table=table,
        partition_cols=[spec["partition"]] if spec.get("partition") else None,
    )

    cols_sql = ",\n  ".join(
        f"`{c}` {t}" for c, t in spec["columns"].items()
        if c != spec.get("partition")
    )
    part_sql = ""
    if spec.get("partition"):
        ptype = spec["columns"][spec["partition"]]
        part_sql = f"\nPARTITIONED BY (`{spec['partition']}` {ptype})"

    ddl = (
        f"CREATE EXTERNAL TABLE IF NOT EXISTS {RAW_DATABASE}.{table} (\n  {cols_sql}\n){part_sql}\n"
        f"STORED AS PARQUET\nLOCATION '{path}'"
    )
    wr.athena.start_query_execution(sql=ddl, database=RAW_DATABASE, wait=True)
    print(f"loaded {RAW_DATABASE}.{table} -> {path}")


def main() -> None:
    for table, spec in RAW_TABLES.items():
        load_table(table, spec)


if __name__ == "__main__":
    main()
