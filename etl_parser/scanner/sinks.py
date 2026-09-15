"""Declarative table of I/O call patterns.

Each entry says whether a call reads, writes, or executes SQL, which argument carries the
dataset name, path, or SQL text, and which engine and dialect the call site implies.
The table is data so teams can extend it without touching the workers.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

Direction = Literal["read", "write", "sql"]


@dataclass(frozen=True)
class SinkSpec:
    callee: str
    """Dotted suffix matched against the fully qualified callee, e.g. ``read.parquet``."""
    direction: Direction
    arg: int | str
    """Positional index or keyword name of the dataset / path / SQL argument."""
    scheme: str
    """``table`` (engine decides), ``s3``, ``file``, ``sql``."""
    engine: str
    dialect: str | None = None
    language: str = "python"
    alt_arg: str | None = None
    """Keyword fallback when ``arg`` is positional but the call used a keyword."""
    schema_kw: str | None = None
    """Keyword that carries the schema/database (pandas ``to_sql(schema=...)``)."""


SINKS: list[SinkSpec] = [
    # --- PySpark ---------------------------------------------------------
    SinkSpec("spark.table", "read", 0, "table", "spark", "spark", "pyspark", alt_arg="tableName"),
    SinkSpec("read.table", "read", 0, "table", "spark", "spark", "pyspark", alt_arg="tableName"),
    SinkSpec("read.parquet", "read", 0, "path", "spark", "spark", "pyspark", alt_arg="path"),
    SinkSpec("read.csv", "read", 0, "path", "spark", "spark", "pyspark", alt_arg="path"),
    SinkSpec("read.json", "read", 0, "path", "spark", "spark", "pyspark", alt_arg="path"),
    SinkSpec("read.orc", "read", 0, "path", "spark", "spark", "pyspark", alt_arg="path"),
    SinkSpec("read.text", "read", 0, "path", "spark", "spark", "pyspark", alt_arg="path"),
    SinkSpec("load", "read", 0, "path", "spark", "spark", "pyspark", alt_arg="path"),
    SinkSpec("spark.sql", "sql", 0, "sql", "spark", "spark", "pyspark", alt_arg="sqlQuery"),
    SinkSpec("write.saveAsTable", "write", 0, "table", "spark", "spark", "pyspark", alt_arg="name"),
    SinkSpec("saveAsTable", "write", 0, "table", "spark", "spark", "pyspark", alt_arg="name"),
    SinkSpec("insertInto", "write", 0, "table", "spark", "spark", "pyspark", alt_arg="tableName"),
    SinkSpec("write.parquet", "write", 0, "path", "spark", "spark", "pyspark", alt_arg="path"),
    SinkSpec("write.csv", "write", 0, "path", "spark", "spark", "pyspark", alt_arg="path"),
    SinkSpec("write.json", "write", 0, "path", "spark", "spark", "pyspark", alt_arg="path"),
    SinkSpec("write.orc", "write", 0, "path", "spark", "spark", "pyspark", alt_arg="path"),
    SinkSpec("save", "write", 0, "path", "spark", "spark", "pyspark", alt_arg="path"),
    # AWS Glue DynamicFrame
    SinkSpec(
        "create_dynamic_frame.from_catalog",
        "read",
        "table_name",
        "table",
        "spark",
        "spark",
        "pyspark",
        schema_kw="database",
    ),
    SinkSpec(
        "write_dynamic_frame.from_catalog",
        "write",
        "table_name",
        "table",
        "spark",
        "spark",
        "pyspark",
        schema_kw="database",
    ),
    # --- pandas ------------------------------------------------------------
    SinkSpec("pd.read_sql", "sql", 0, "sql", "pandas", None, alt_arg="sql"),
    SinkSpec("pd.read_sql_query", "sql", 0, "sql", "pandas", None, alt_arg="sql"),
    SinkSpec(
        "pd.read_sql_table",
        "read",
        0,
        "table",
        "pandas",
        None,
        alt_arg="table_name",
        schema_kw="schema",
    ),
    SinkSpec("pd.read_csv", "read", 0, "path", "pandas", None, alt_arg="filepath_or_buffer"),
    SinkSpec("pd.read_parquet", "read", 0, "path", "pandas", None, alt_arg="path"),
    SinkSpec("pd.read_json", "read", 0, "path", "pandas", None, alt_arg="path_or_buf"),
    SinkSpec("pd.read_excel", "read", 0, "path", "pandas", None, alt_arg="io"),
    SinkSpec("to_sql", "write", 0, "table", "pandas", None, alt_arg="name", schema_kw="schema"),
    SinkSpec("to_csv", "write", 0, "path", "pandas", None, alt_arg="path_or_buf"),
    SinkSpec("to_parquet", "write", 0, "path", "pandas", None, alt_arg="path"),
    SinkSpec("to_json", "write", 0, "path", "pandas", None, alt_arg="path_or_buf"),
    # --- polars ------------------------------------------------------------
    SinkSpec("pl.read_parquet", "read", 0, "path", "polars", None, alt_arg="source"),
    SinkSpec("pl.scan_parquet", "read", 0, "path", "polars", None, alt_arg="source"),
    SinkSpec("pl.read_csv", "read", 0, "path", "polars", None, alt_arg="source"),
    SinkSpec("pl.scan_csv", "read", 0, "path", "polars", None, alt_arg="source"),
    SinkSpec("pl.read_database", "sql", 0, "sql", "polars", None, alt_arg="query"),
    SinkSpec("write_parquet", "write", 0, "path", "polars", None, alt_arg="file"),
    SinkSpec("sink_parquet", "write", 0, "path", "polars", None, alt_arg="path"),
    SinkSpec("write_csv", "write", 0, "path", "polars", None, alt_arg="file"),
    # --- boto3 / Athena / S3 ------------------------------------------------
    SinkSpec("start_query_execution", "sql", "QueryString", "sql", "athena", "trino"),
    SinkSpec("get_object", "read", "Key", "s3_object", "s3", None, schema_kw="Bucket"),
    SinkSpec("download_file", "read", "Key", "s3_object", "s3", None, schema_kw="Bucket"),
    SinkSpec("put_object", "write", "Key", "s3_object", "s3", None, schema_kw="Bucket"),
    SinkSpec("upload_file", "write", "Key", "s3_object", "s3", None, schema_kw="Bucket"),
    SinkSpec("upload_fileobj", "write", "Key", "s3_object", "s3", None, schema_kw="Bucket"),
    # --- awswrangler ---------------------------------------------------------
    SinkSpec("wr.athena.read_sql_query", "sql", 0, "sql", "athena", "trino", alt_arg="sql"),
    SinkSpec(
        "wr.athena.read_sql_table",
        "read",
        0,
        "table",
        "athena",
        "trino",
        alt_arg="table",
        schema_kw="database",
    ),
    SinkSpec("wr.s3.read_parquet", "read", 0, "path", "athena", None, alt_arg="path"),
    SinkSpec("wr.s3.read_csv", "read", 0, "path", "athena", None, alt_arg="path"),
    SinkSpec("wr.s3.to_parquet", "write", "path", "path", "athena", None),
    SinkSpec("wr.s3.to_csv", "write", "path", "path", "athena", None),
    # --- DB-API / SQLAlchemy / PyAthena --------------------------------------
    SinkSpec("cursor.execute", "sql", 0, "sql", "unknown", None, alt_arg="operation"),
    SinkSpec("cur.execute", "sql", 0, "sql", "unknown", None, alt_arg="operation"),
    SinkSpec("execute", "sql", 0, "sql", "unknown", None),
    SinkSpec("executemany", "sql", 0, "sql", "unknown", None),
    SinkSpec("text", "sql", 0, "sql", "unknown", None),
]

# Engine implied by a connection URL / client prefix seen in the same file.
URL_ENGINE_HINTS = {
    "postgresql": "postgres",
    "postgres": "postgres",
    "psycopg2": "postgres",
    "psycopg": "postgres",
    "mysql": "mysql",
    "pymysql": "mysql",
    "awsathena": "athena",
    "pyathena": "athena",
    "trino": "athena",
    "presto": "athena",
    "sqlite": "sqlite",
    "redshift": "redshift",
    "snowflake": "snowflake",
}

ENGINE_DIALECT = {
    "athena": "trino",
    "trino": "trino",
    "spark": "spark",
    "postgres": "postgres",
    "mysql": "mysql",
    "sqlite": "sqlite",
    "redshift": "redshift",
    "snowflake": "snowflake",
}


def match_sink(qualified_callee: str) -> SinkSpec | None:
    """Longest-suffix match of a dotted callee such as ``spark.read.parquet``.

    Earlier entries win ties, so ``write.saveAsTable`` is preferred over bare ``saveAsTable``.
    """
    best: SinkSpec | None = None
    for spec in SINKS:
        if qualified_callee == spec.callee or qualified_callee.endswith("." + spec.callee):
            if best is None or len(spec.callee) > len(best.callee):
                best = spec
    return best
