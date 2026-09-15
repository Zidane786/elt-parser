from etl_parser.scanner.sinks import match_sink


def test_spark_patterns():
    assert match_sink("spark.table").direction == "read"
    assert match_sink("spark.read.parquet").scheme == "path"
    s = match_sink("joined.write.mode.partitionBy.saveAsTable")
    assert s.direction == "write" and s.callee == "saveAsTable"
    assert match_sink("spark.sql").direction == "sql" and match_sink("spark.sql").dialect == "spark"


def test_pandas_boto_wrangler_dbapi():
    assert match_sink("pd.read_sql").direction == "sql"
    assert match_sink("invoices.to_sql").schema_kw == "schema"
    a = match_sink("athena.start_query_execution")
    assert a.direction == "sql" and a.arg == "QueryString" and a.dialect == "trino"
    assert match_sink("s3.put_object").schema_kw == "Bucket"
    assert match_sink("wr.athena.read_sql_query").engine == "athena"
    assert match_sink("cursor.execute").direction == "sql"


def test_unknown_returns_none_and_longest_suffix_wins():
    assert match_sink("df.show") is None
    assert match_sink("something.read.table").callee == "read.table"
