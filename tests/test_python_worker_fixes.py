"""Regression tests for the Python/Spark worker fixes of the 2026-09-19 review (WP-C)."""

import pytest

from etl_parser.pipeline import scan
from etl_parser.workers.sql import DictSchemaProvider


def run(tmp_path, text, schema=None, name="job.py"):
    """Scan one Python file and return the resulting lineage document."""
    path = tmp_path / name
    path.write_text(text)
    return scan(path, schema=DictSchemaProvider(schema) if schema else None).document


def origin(edge):
    """Return an edge's direct sources as ``(dataset_id, column)`` pairs."""
    return {(r.dataset_id, r.name) for r in edge.sources}


READ = 'df = spark.table("shop.orders").select("id")\n'


@pytest.mark.parametrize(
    "write",
    [
        'df.write.mode("overwrite").parquet("s3://b/o/")',
        'df.write.option("compression", "gzip").csv("s3://b/o/")',
        'df.write.partitionBy("id").json("s3://b/o/")',
        'df.write.format("parquet").option("path", "s3://b/o/").save()',
    ],
)
def test_chained_spark_writers_resolve_outputs_and_column_edges(tmp_path, write):
    doc = run(tmp_path, READ + write)
    assert doc.jobs[0].outputs == ["s3://b/o/"]
    edge = next(e for e in doc.column_edges if e.target.dataset_id == "s3://b/o/")
    assert edge.target.name == "id"
    assert origin(edge) == {("glue://shop/orders", "id")}
    assert [e.source for e in doc.table_edges] == ["glue://shop/orders"]


def test_spark_read_accepts_keyword_path(tmp_path):
    doc = run(
        tmp_path,
        'df = spark.read.parquet(path="s3://b/in/")\ndf.write.saveAsTable("db.t")',
    )
    assert doc.jobs[0].inputs == ["s3://b/in/"]
    assert not [u for u in doc.unresolved if u.kind == "dynamic_path"]


def test_deeply_nested_expression_is_reported_not_raised(tmp_path):
    chain = " + ".join(f"F.col('c{i}')" for i in range(400))
    doc = run(
        tmp_path,
        'from pyspark.sql import functions as F\ndf = spark.table("a.b")\n'
        f'df.withColumn("total", {chain}).write.saveAsTable("a.t")\n',
    )
    assert [j.id for j in doc.jobs] == ["job"]
    assert any(u.kind == "unsupported_syntax" for u in doc.unresolved)


def test_analyze_file_outside_index_root_is_not_an_error(tmp_path):
    from etl_parser.scanner.repo import RepoScanner
    from etl_parser.workers.python import PythonWorker

    (tmp_path / "repo").mkdir()
    (tmp_path / "repo" / "kept.py").write_text("x = 1\n")
    outside = tmp_path / "outside.py"
    outside.write_text('spark.table("a.b").write.saveAsTable("a.t")\n')
    index = RepoScanner(tmp_path / "repo").scan()
    result = PythonWorker(index=index).analyze_file(outside)
    assert [j.source_file for j in result.jobs] == [str(outside)]
    assert result.jobs[0].inputs == ["glue://a/b"]
