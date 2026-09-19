"""Regression tests for the Python/Spark worker fixes of the 2026-09-19 review (WP-C)."""

import zipfile

import pytest

from etl_parser.pipeline import scan
from etl_parser.scanner.repo import RepoScanner
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
    from etl_parser.workers.python import PythonWorker

    (tmp_path / "repo").mkdir()
    (tmp_path / "repo" / "kept.py").write_text("x = 1\n")
    outside = tmp_path / "outside.py"
    outside.write_text('spark.table("a.b").write.saveAsTable("a.t")\n')
    index = RepoScanner(tmp_path / "repo").scan()
    result = PythonWorker(index=index).analyze_file(outside)
    assert [j.source_file for j in result.jobs] == [str(outside)]
    assert result.jobs[0].inputs == ["glue://a/b"]


FAKE_NAMES = {"{{?}}", "*", ""}


def names_in(doc):
    """Return every column name that appears on either end of a column edge."""
    refs = [r for e in doc.column_edges for r in [*e.sources, *e.indirect_sources]]
    return {e.target.name for e in doc.column_edges} | {r.name for r in refs}


def test_unresolved_column_name_is_a_diagnostic_not_an_edge(tmp_path):
    doc = run(
        tmp_path,
        'from pyspark.sql import functions as F\ndf = spark.table("a.b")\n'
        'df.withColumn(name_var, F.lit(1)).write.saveAsTable("a.t")\n',
    )
    assert not names_in(doc) & FAKE_NAMES
    assert any(u.kind == "unknown_column" for u in doc.unresolved)


def test_select_star_expression_keeps_known_columns_not_a_star_column(tmp_path):
    doc = run(
        tmp_path,
        'df = spark.table("a.b")\ndf.selectExpr("*").write.saveAsTable("a.t")\n',
        schema={"a": {"b": ["id", "amount"]}},
    )
    assert not names_in(doc) & FAKE_NAMES
    assert {e.target.name for e in doc.column_edges} == {"id", "amount"}


def test_positional_union_with_open_right_frame_reports_unknown_column(tmp_path):
    doc = run(
        tmp_path,
        'a = spark.table("x.a").select("id")\nb = spark.table("x.b")\n'
        'a.union(b).write.saveAsTable("x.t")\n',
    )
    assert not names_in(doc) & FAKE_NAMES
    assert any(u.kind == "unknown_column" for u in doc.unresolved)
    edge = next(e for e in doc.column_edges if e.target.dataset_id == "glue://x/t")
    assert origin(edge) == {("glue://x/a", "id")}
    assert edge.provenance.confidence == "partial"


@pytest.mark.parametrize(
    ("snippet", "expected"),
    [
        ('process(spark.table("a.b"))', {"glue://a/b"}),
        ('frames = []\nframes.append(spark.table("a.b"))', {"glue://a/b"}),
        ('for row in spark.table("a.b").collect():\n    print(row)', {"glue://a/b"}),
        ('a, b = spark.table("x.a"), spark.table("x.b")', {"glue://x/a", "glue://x/b"}),
        (
            'try:\n    spark.table("t.body")\nexcept ValueError:\n    spark.table("t.handler")\n'
            'else:\n    spark.table("t.orelse")\nfinally:\n    spark.table("t.final")',
            {"glue://t/body", "glue://t/handler", "glue://t/orelse", "glue://t/final"},
        ),
        (
            'match mode:\n    case "a":\n        spark.table("m.a")\n'
            '    case _:\n        spark.table("m.b")',
            {"glue://m/a", "glue://m/b"},
        ),
        ('class Config:\n    source = spark.table("a.b")', {"glue://a/b"}),
        ('if (df := spark.table("a.b")).count():\n    print(df)', {"glue://a/b"}),
    ],
)
def test_reads_in_unevaluated_positions_reach_inputs(tmp_path, snippet, expected):
    doc = run(tmp_path, snippet + "\n")
    assert set(doc.jobs[0].inputs) >= expected


def test_augmented_string_assignment_does_not_fabricate_a_table(tmp_path):
    doc = run(tmp_path, 't = "a."\nt += "b"\nspark.table(t).write.saveAsTable("a.t")\n')
    assert "glue://default/a" not in doc.jobs[0].inputs
    assert doc.jobs[0].inputs == ["glue://a/b"] or any(
        u.kind == "dynamic_table_name" for u in doc.unresolved
    )


def test_return_nested_in_a_branch_is_not_discarded(tmp_path):
    (tmp_path / "helpers.py").write_text(
        "def load(cond):\n"
        '    if cond:\n        return spark.table("a.a")\n'
        '    return spark.table("a.b")\n'
    )
    doc = run(
        tmp_path,
        'from helpers import load\nload(flag).select("id").write.saveAsTable("a.t")\n',
    )
    assert set(doc.jobs[0].inputs) == {"glue://a/a", "glue://a/b"}
    edge = next(e for e in doc.column_edges if e.target.dataset_id == "glue://a/t")
    assert origin(edge) == {("glue://a/a", "id"), ("glue://a/b", "id")}
    assert edge.provenance.confidence == "inferred"


def test_helper_followed_frame_is_inferred_not_exact(tmp_path):
    (tmp_path / "helpers.py").write_text('def load():\n    return spark.table("a.b")\n')
    doc = run(
        tmp_path,
        'from helpers import load\nload().select("id").write.saveAsTable("a.t")\n',
    )
    edge = next(e for e in doc.column_edges if e.target.dataset_id == "glue://a/t")
    assert edge.provenance.confidence == "inferred"
    assert [e.provenance.confidence for e in doc.table_edges] == ["inferred"]


UDF_HEADER = "from pyspark.sql import functions as F\nfrom pyspark.sql.types import StringType\n"


@pytest.mark.parametrize(
    "definition",
    [
        "clean = F.udf(lambda s: s.strip(), StringType())\n",
        "@F.udf(returnType=StringType())\ndef clean(s):\n    return s.strip()\n",
        "@F.pandas_udf(StringType())\ndef clean(s):\n    return s\n",
    ],
)
def test_udf_output_is_unknown_kind_with_partial_confidence(tmp_path, definition):
    doc = run(
        tmp_path,
        UDF_HEADER + definition + 'df = spark.table("a.b")\n'
        'df.withColumn("name", clean(F.col("raw"))).write.saveAsTable("a.t")\n',
    )
    edge = next(e for e in doc.column_edges if e.target.name == "name")
    assert edge.transformation.kind == "unknown"
    assert edge.provenance.confidence == "partial"
    assert origin(edge) == {("glue://a/b", "raw")}


def test_union_by_name_merges_kind_and_column_indirect_sources(tmp_path):
    doc = run(
        tmp_path,
        "from pyspark.sql import functions as F\nfrom pyspark.sql import Window\n"
        'left = spark.table("x.a").select("amount")\n'
        'right = spark.table("x.b").withColumn(\n'
        '    "amount", F.sum("amount").over(Window.partitionBy("region"))\n'
        ').select("amount")\n'
        'left.unionByName(right).write.saveAsTable("x.t")\n',
    )
    edge = next(e for e in doc.column_edges if e.target.dataset_id == "glue://x/t")
    assert origin(edge) == {("glue://x/a", "amount"), ("glue://x/b", "amount")}
    assert ("glue://x/b", "region") in {(r.dataset_id, r.name) for r in edge.indirect_sources}
    assert edge.transformation.kind == "window"


def test_language_comes_from_imports_not_from_a_comment(tmp_path):
    doc = run(
        tmp_path,
        "import pandas as pd\n# TODO: port this to pyspark next quarter\n"
        'df = pd.read_csv("in.csv")\ndf[["x"]].to_csv("out.csv")\n',
    )
    assert doc.jobs[0].language == "python"
    assert {e.provenance.parser for e in doc.column_edges} == {"pandas_chain"}


@pytest.mark.parametrize(
    "header",
    [
        "from pyspark.sql import SparkSession\n",
        "import pyspark\n",
        "from awsglue.context import GlueContext\n",
    ],
)
def test_spark_imports_set_language_engine_and_parser(tmp_path, header):
    doc = run(
        tmp_path,
        header + 'spark.table("a.b").select("id").write.saveAsTable("a.t")\n',
    )
    job = doc.jobs[0]
    assert (job.language, job.engine) == ("pyspark", "spark")
    assert {e.provenance.parser for e in doc.column_edges} == {"spark_static"}


@pytest.mark.parametrize(
    "snippet",
    [
        'import json\nwith open("f") as fh:\n    data = json.load(fh)',
        'import numpy as np\nnp.save("out.npy", rows)',
        "text = soup.text\nbody = soup.text()",
        'pool.execute("not sql at all")',
    ],
)
def test_bare_io_suffixes_do_not_match_unrelated_calls(tmp_path, snippet):
    doc = run(tmp_path, snippet + "\n")
    assert doc.jobs[0].inputs == [] and doc.jobs[0].outputs == []
    unwanted = {"dynamic_sql", "dynamic_path", "dynamic_table_name"}
    assert not [u for u in doc.unresolved if u.kind in unwanted]


def test_cursor_execute_still_analyzed_and_inherits_the_connection_dialect(tmp_path):
    doc = run(
        tmp_path,
        "import psycopg2\n"
        'conn = psycopg2.connect("postgresql://etl@host:5432/warehouse")\n'
        "cur = conn.cursor()\n"
        'cur.execute("INSERT INTO shop.target SELECT id FROM shop.source")\n',
    )
    job = doc.jobs[0]
    assert job.inputs == ["postgres://shop/source"] and job.outputs == ["postgres://shop/target"]
    assert {e.provenance.dialect for e in doc.column_edges} == {"postgres"}


def test_sqlalchemy_text_is_matched_through_its_import(tmp_path):
    doc = run(
        tmp_path,
        "from sqlalchemy import text\n"
        'statement = text("INSERT INTO shop.target SELECT id FROM shop.source")\n',
    )
    assert doc.jobs[0].outputs == ["table://shop/target"]


def test_one_bad_zip_member_does_not_abort_the_rest(tmp_path):
    with zipfile.ZipFile(tmp_path / "a.zip", "w") as archive:
        archive.writestr("bad.py", b"\xff\xfe not utf-8 at all")
        archive.writestr("good.py", "x = 1\n")
    index = RepoScanner(tmp_path).scan()
    assert [s.path for s in index.files] == ["a.zip!/good.py"]
    assert [u.source_file for u in index.unresolved] == ["a.zip!/bad.py"]


def test_zip_member_with_a_lying_size_header_is_read_within_the_limit(tmp_path, monkeypatch):
    with zipfile.ZipFile(tmp_path / "a.zip", "w") as archive:
        archive.writestr("big.py", "x = 1\n" * 500)
        archive.writestr("small.py", "y = 2\n")
    listing = zipfile.ZipFile.infolist

    def understated(self):
        """Report the large member as one byte, however large it really is."""
        members = listing(self)
        for member in members:
            if member.filename == "big.py":
                member.file_size = 1
        return members

    monkeypatch.setattr(zipfile.ZipFile, "infolist", understated)
    index = RepoScanner(tmp_path, max_file_bytes=100).scan()
    assert [s.path for s in index.files] == ["a.zip!/small.py"]
    assert [u.source_file for u in index.unresolved] == ["a.zip!/big.py"]


def test_local_zip_respects_the_archive_size_limit(tmp_path):
    with zipfile.ZipFile(tmp_path / "a.zip", "w") as archive:
        archive.writestr("m.py", "x = 1\n")
    index = RepoScanner(tmp_path, max_archive_bytes=10).scan()
    assert index.files == []
    assert [u.source_file for u in index.unresolved] == ["a.zip"]


def test_spark_static_worker_stamps_spark_without_an_import(tmp_path):
    from etl_parser.scanner.repo import SourceFile
    from etl_parser.workers.spark_static import SparkStaticWorker

    source = SourceFile(
        "job.py", 'spark.table("a.b").select("id").write.saveAsTable("a.t")\n', ".py"
    )
    result = SparkStaticWorker().analyze_source(source)
    assert (result.jobs[0].language, result.jobs[0].engine) == ("pyspark", "spark")
    assert {e.provenance.parser for e in result.column_edges} == {"spark_static"}
