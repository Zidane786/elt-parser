import json

import pytest

from etl_parser.export.agent_catalog import export_agent_catalog
from etl_parser.export.openlineage_out import export_openlineage
from etl_parser.graph.builder import build_graph
from etl_parser.models import DatasetRef, Job, Provenance, TableEdge, WorkerResult
from etl_parser.pipeline import scan
from etl_parser.workers.sql import DictSchemaProvider


def run(tmp_path, text, schema=None):
    path = tmp_path / "job.py"
    path.write_text(text)
    return scan(path, schema=DictSchemaProvider(schema) if schema else None).document


def origin(edge):
    return {(r.dataset_id, r.name) for r in edge.sources}


def test_local_load_helper_wins_over_generic_sink_and_follows_constants(tmp_path):
    (tmp_path / "settings.py").write_text('TABLE = "db.source"\n')
    (tmp_path / "utils.py").write_text(
        'from settings import TABLE\ndef load():\n    return spark.table(TABLE).select("x")\n'
    )
    doc = run(tmp_path, 'from utils import load\nload().write.saveAsTable("db.target")')
    assert doc.jobs[0].inputs == ["glue://db/source"]
    assert origin(doc.column_edges[0]) == {("glue://db/source", "x")}


def test_relative_files_are_file_datasets(tmp_path):
    doc = run(
        tmp_path,
        'import pandas as pd\ndf = pd.read_csv("data/source.csv")\n'
        'df[["x"]].to_csv("data/target.csv")',
    )
    assert all(d.kind == "file" for d in doc.datasets)
    assert origin(doc.column_edges[0]) == {(doc.jobs[0].inputs[0], "x")}


def test_kwargs_group_aggregation_and_merge_suffixes(tmp_path):
    doc = run(
        tmp_path,
        "import pandas as pd\n"
        'a = pd.read_parquet("s3://b/a")\nb = pd.read_parquet("s3://b/b")\n'
        'joined = a.merge(b, on="id", suffixes=("_left", "_right"))\n'
        'out = joined.groupby("id").agg(total=("amount_right", "sum"))\n'
        'out.to_parquet("s3://b/out")',
        schema=None,
    )
    total = next(e for e in doc.column_edges if e.target.name == "total")
    # Without input schemas, overlap/suffix membership cannot be proven.
    assert total.provenance.confidence == "partial"


def test_pandas_merge_suffixes_with_schema(tmp_path):
    doc = run(
        tmp_path,
        "import pandas as pd\nfrom sqlalchemy import create_engine\n"
        'con = create_engine("postgresql://host/db")\n'
        'a = pd.read_sql_table("a", con, schema="db")\n'
        'b = pd.read_sql_table("b", con, schema="db")\n'
        'out = a.merge(b, on="id")[["value_x", "value_y"]]\n'
        'out.to_sql("out", con, schema="db")',
        {"db": {"a": ["id", "value"], "b": ["id", "value"]}},
    )
    edges = {e.target.name: e for e in doc.column_edges}
    assert origin(edges["value_x"]) == {("postgres://db/a", "value")}
    assert origin(edges["value_y"]) == {("postgres://db/b", "value")}


def test_polars_keyword_expressions_and_star_alias(tmp_path):
    doc = run(
        tmp_path,
        'import polars as pl\ndf = pl.read_parquet("s3://b/input")\n'
        'df.with_columns(twice=pl.col("x") * 2).select("twice").write_parquet("s3://b/out")',
    )
    assert origin(doc.column_edges[0]) == {("s3://b/input", "x")}
    assert doc.column_edges[0].provenance.confidence == "exact"


def test_spark_to_df_renames_positionally(tmp_path):
    doc = run(
        tmp_path,
        'df = spark.table("db.s").select("x", "y")\n'
        'df.toDF("first", "second").write.saveAsTable("db.t")',
    )
    edges = {e.target.name: e for e in doc.column_edges}
    assert set(edges) == {"first", "second"}
    assert origin(edges["second"]) == {("glue://db/s", "y")}


def test_read_builder_and_multiple_parquet_paths(tmp_path):
    doc = run(
        tmp_path,
        'df = spark.read.format("parquet").option("path", "s3://b/in").load()\n'
        'df.select("x").write.saveAsTable("db.out")',
    )
    assert doc.jobs[0].inputs == ["s3://b/in"]
    doc = run(
        tmp_path,
        'df = spark.read.parquet("s3://b/a", "s3://b/b")\n'
        'df.select("x").write.saveAsTable("db.out")',
    )
    assert doc.jobs[0].inputs == ["s3://b/a", "s3://b/b"]
    assert origin(doc.column_edges[0]) == {("s3://b/a", "x"), ("s3://b/b", "x")}


def test_known_literal_loops_and_expressions_keep_variable_values(tmp_path):
    doc = run(
        tmp_path,
        'for env in ["dev", "prod"]:\n'
        '    spark.table(f"db.src_{env}").select("x").write.saveAsTable(f"db.out_{env}")',
    )
    assert doc.jobs[0].inputs == ["glue://db/src_dev", "glue://db/src_prod"]
    assert doc.jobs[0].outputs == ["glue://db/out_dev", "glue://db/out_prod"]


def test_unknown_method_propagates_sources_and_diagnostics(tmp_path):
    doc = run(
        tmp_path,
        'df = spark.table("db.s").select("x").custom_transform()\ndf.write.saveAsTable("db.t")',
    )
    assert origin(doc.column_edges[0]) == {("glue://db/s", "x")}
    assert doc.column_edges[0].provenance.confidence == "partial"
    assert doc.unresolved


def test_dataset_aliases_are_applied_to_jobs_and_edges():
    result = WorkerResult(
        datasets=[
            DatasetRef(
                id="glue://db/t",
                namespace="glue://db",
                name="t",
                aliases=["s3://bucket/t"],
                physical_location="s3://bucket/t",
            )
        ],
        jobs=[
            Job(id="a", name="a", source_file="a.py", outputs=["s3://bucket/t"]),
            Job(
                id="b",
                name="b",
                source_file="b.py",
                inputs=["glue://db/t"],
                outputs=["glue://db/out"],
            ),
        ],
        table_edges=[
            TableEdge(
                source="s3://bucket/t",
                target="glue://db/out",
                job_id="b",
                provenance=Provenance(parser="test"),
            )
        ],
    )
    doc = build_graph([result]).document
    assert doc.jobs[0].outputs == ["glue://db/t"]
    assert doc.table_edges[0].source == "glue://db/t"
    assert doc.job_dependencies["b"][0].job_id == "a"
    assert result.jobs[0].outputs == ["s3://bucket/t"]  # Builder doesn't mutate its input.


def test_airflow_chain_lists_pairwise_not_cross_product(tmp_path):
    for name in ("a", "b", "c", "d"):
        (tmp_path / f"{name}.py").write_text(f'spark.table("db.{name}")')
    source = (
        "from airflow import DAG\nfrom airflow.models.baseoperator import chain\n"
        'from airflow.operators.bash import BashOperator\nwith DAG("d"):\n'
    )
    source += "".join(
        f'    {name} = BashOperator(task_id="{name}", bash_command="python {name}.py")\n'
        for name in ("a", "b", "c", "d")
    )
    source += "    chain([a,b], [c,d])\n"
    (tmp_path / "dag.py").write_text(source)
    doc = scan(tmp_path).document
    assert [d.job_id for d in doc.job_dependencies["c"]] == ["a"]
    assert [d.job_id for d in doc.job_dependencies["d"]] == ["b"]


def test_generic_sql_operator_accepts_explicit_connection_dialect(tmp_path):
    (tmp_path / "dag.py").write_text(
        "from airflow import DAG\n"
        "from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator\n"
        'with DAG("pipeline"):\n'
        '    task = SQLExecuteQueryOperator(task_id="load", conn_id="warehouse", '
        'sql="CREATE TABLE db.t AS SELECT x FROM db.s")\n'
    )
    doc = scan(tmp_path, bindings={"connection:warehouse": "postgres"}).document
    assert doc.jobs[0].inputs == ["postgres://db/s"]
    assert doc.jobs[0].outputs == ["postgres://db/t"]


def test_invalid_schema_file_has_clear_validation(tmp_path):
    file = tmp_path / "schema.json"
    file.write_text(json.dumps({"db": {"t": "not a column list"}}))
    with pytest.raises(ValueError, match="column"):
        DictSchemaProvider(file)


def test_module_attribute_constants_in_fstrings(tmp_path):
    (tmp_path / "settings.py").write_text('ENV = "prod"\n')
    doc = run(
        tmp_path,
        "import settings\n"
        'spark.table(f"db.src_{settings.ENV}").select("x").write.saveAsTable("db.t")',
    )
    assert doc.jobs[0].inputs == ["glue://db/src_prod"]


def test_recursive_imports_terminate_with_diagnostics(tmp_path):
    (tmp_path / "a.py").write_text("from b import load\ndef run():\n    return load()\n")
    (tmp_path / "b.py").write_text("from a import run\ndef load():\n    return run()\n")
    doc = run(tmp_path, 'from a import run\nrun().write.saveAsTable("db.t")')
    assert any(u.kind == "unresolved_import" for u in doc.unresolved)


def test_airflow_environment_defaults_remain_assumptions(tmp_path):
    (tmp_path / "dag.py").write_text(
        "import os\nfrom airflow import DAG\n"
        'INTERVAL = os.getenv("SCHEDULE", "@daily")\nwith DAG("d", schedule=INTERVAL):\n    pass\n'
    )
    doc = scan(tmp_path).document
    assert any(u.kind == "dynamic_schedule" for u in doc.unresolved)
    doc = scan(tmp_path, bindings={"env:SCHEDULE": "@hourly"}).document
    assert not doc.unresolved
    assert next(iter(doc.schedules.values())).cron == "0 * * * *"


def test_catalog_keeps_engines_separate_and_preserves_descriptions():
    from etl_parser.models import LineageDocument

    doc = LineageDocument(
        datasets=[
            DatasetRef(id=f"{engine}://db/t", namespace=f"{engine}://db", name="t", columns=["x"])
            for engine in ("glue", "postgres")
        ]
    )
    prior = {
        "databases": [
            {
                "db_name": "db",
                "db_type": "postgres",
                "tables": [{"table_name": "t", "description": "Postgres source", "schema": []}],
            }
        ]
    }
    # The glue table exists only in code, so adding it now requires include_code_schema;
    # every assertion below is unchanged (WP-A source-of-truth default).
    catalog = export_agent_catalog(doc, prior, include_code_schema=True)
    tables = {t["dataset_id"]: t for d in catalog["databases"] for t in d["tables"]}
    assert set(tables) == {"glue://db/t", "postgres://db/t"}
    assert tables["postgres://db/t"]["description"] == "Postgres source"
    assert not tables["glue://db/t"]["description"]
    assert {d["db_type"] for d in catalog["databases"]} == {"postgres", "athena"}


def test_openlineage_preserves_direct_and_indirect_roles(tmp_path):
    (tmp_path / "job.sql").write_text(
        "CREATE TABLE db.t AS SELECT x + 1 AS y FROM db.s WHERE x > 0;"
        "INSERT INTO db.t SELECT x * 2 AS y FROM db.s"
    )
    doc = scan(tmp_path).document
    field = export_openlineage(doc)[0]["outputs"][0]["facets"]["columnLineage"]["fields"]["y"]
    transforms = field["inputFields"][0]["transformations"]
    assert {t["type"] for t in transforms} == {"DIRECT", "INDIRECT"}
    assert "+ 1" in field["transformationDescription"]
    assert "* 2" in field["transformationDescription"]


def test_table_edges_have_expression_and_source_span(tmp_path):
    (tmp_path / "job.sql").write_text("CREATE TABLE db.t AS SELECT x FROM db.s")
    doc = scan(tmp_path).document
    edge = doc.table_edges[0]
    assert edge.transformation.source_file == "job.sql"
    assert edge.transformation.line_start == 1
    assert "SELECT" in edge.transformation.expression
