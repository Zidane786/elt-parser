import json
import zipfile
from pathlib import Path

import pytest
from typer.testing import CliRunner

from etl_parser.cli import app
from etl_parser.export.agent_catalog import export_agent_catalog
from etl_parser.export.native import read_native, write_native
from etl_parser.export.openlineage_out import export_openlineage
from etl_parser.graph.impact import downstream, upstream
from etl_parser.models import Job, Provenance, TableEdge, WorkerResult
from etl_parser.pipeline import ParserRegistry, scan
from etl_parser.scanner.repo import RepoScanner, SourceFile
from etl_parser.workers.sql import DictSchemaProvider

FIXTURES = Path(__file__).parent / "fixtures"


def test_fixture_catalog_reads_and_writes_match_reviewed_corrections():
    catalog = json.loads((FIXTURES / "etl_catalog_original.json").read_text())
    graph = scan(FIXTURES / "etl", schema=DictSchemaProvider(catalog))
    exported = export_agent_catalog(graph.document)
    golden = json.loads((FIXTURES / "catalog_scripts_golden.json").read_text())
    assert exported["scripts"] == golden
    jobs = {s["script_name"]: s for s in exported["scripts"]}
    corrections = {
        "dim_customer_scd2": {("table", "analytics_warehouse.dim_customer")},
        "ingest_marketing_attribution": {("s3", "s3://ad-server-exports/attribution/")},
        "ingest_support_tickets": {("s3", "s3://zendesk-exports/tickets/")},
        "mart_customer_ltv": {("table", "analytics_warehouse.dim_customer")},
        "mart_funnel_conversion": {("table", "ecommerce.raw_page_views")},
    }
    assert len(jobs) == 28
    for expected in catalog["scripts"]:
        actual = jobs[expected["script_name"]]
        reads = {(r["type"], r["target"]) for r in expected["reads_from"]}
        reads |= corrections.get(expected["script_name"], set())
        assert {(r["type"], r["target"]) for r in actual["reads_from"]} == reads
        assert actual["writes_to"] == expected["writes_to"]
        # Headers are authoritative; the hand-maintained catalog has stale schedules.
        from etl_parser.workers.base import parse_header

        source = (FIXTURES / "etl" / f"{expected['script_name']}.py").read_text()
        assert actual["schedule"] == parse_header(source)["schedule"]
    assert not [u for u in graph.document.unresolved if u.kind == "unsupported_syntax"]


def test_spark_expression_aliases_and_union_sources():
    graph = scan(
        FIXTURES / "etl", schema=DictSchemaProvider(FIXTURES / "etl_catalog_original.json")
    )
    edge = next(
        e
        for e in graph.document.column_edges
        if e.job_id == "stage_orders" and e.target.name == "total_usd_cents"
    )
    assert {(s.dataset_id, s.name) for s in edge.sources} == {
        ("glue://ecommerce/raw_orders", "total_cents"),
        ("glue://analytics_warehouse/dim_currency", "usd_fx_rate"),
    }
    assert "coalesce" in edge.transformation.expression
    assert edge.provenance.confidence == "exact"
    union = next(
        e
        for e in graph.document.column_edges
        if e.job_id == "dim_customer_scd2" and e.target.name == "tier"
    )
    assert {(s.dataset_id, s.name) for s in union.sources} == {
        ("glue://ecommerce/staging_customers", "tier"),
        ("glue://analytics_warehouse/dim_customer", "tier"),
    }


def test_zip_helpers_follow_columns_without_extraction_or_execution(tmp_path):
    with zipfile.ZipFile(tmp_path / "helpers.zip", "w") as archive:
        archive.writestr("company/__init__.py", "")
        archive.writestr(
            "company/transforms.py",
            "from pyspark.sql import functions as F\n"
            'raise RuntimeError("must never execute")\n'
            'def clean(df):\n    return df.select(F.col("raw").alias("renamed"))\n',
        )
        archive.writestr("../escape.py", "raise RuntimeError()")
    (tmp_path / "job.py").write_text(
        "from company.transforms import clean\n"
        'df = spark.table("a.source")\nclean(df).write.saveAsTable("b.target")'
    )
    graph = scan(tmp_path)
    assert not (tmp_path.parent / "escape.py").exists()
    assert {j.id for j in graph.document.jobs} == {"job"}
    edge = graph.document.column_edges[0]
    assert edge.target.name == "renamed"
    assert [(s.dataset_id, s.name) for s in edge.sources] == [("glue://a/source", "raw")]
    assert edge.transformation.source_file == "helpers.zip!/company/transforms.py"
    assert edge.transformation.line_start == 4
    assert any("Unsafe" in u.reason for u in graph.document.unresolved)
    single = scan(tmp_path / "job.py").document
    assert single.column_edges[0].sources == edge.sources


def test_zip_limits_and_ambiguous_imports(tmp_path):
    for name in ("a", "b"):
        with zipfile.ZipFile(tmp_path / f"{name}.zip", "w") as archive:
            archive.writestr("helpers.py", "def load(): pass")
    index = RepoScanner(tmp_path).scan()
    assert index.resolve_module("helpers", SourceFile("main.py", "", ".py")) is None
    index = RepoScanner(tmp_path, max_file_bytes=2).scan()
    assert len(index.unresolved) == 2


def test_products_dag_dependencies_and_impact():
    graph = scan(FIXTURES / "products")
    assert {p.code for p in graph.document.products} == {"bill", "meter", "reg"}
    deps = graph.document.job_dependencies["bill/jobs/build_fact_invoice"]
    parent = next(d for d in deps if d.job_id == "bill/jobs/rate_invoices")
    assert parent.sources == ["dag", "data"]
    job = next(j for j in graph.document.jobs if j.id == "reg/sql/mart_energy_sold")
    assert graph.document.schedules[job.schedule_id].orchestrator == "airflow"
    report = downstream(graph, "glue://meter_cur/fact_consumption", 1)
    assert "glue://reg_cur/mart_energy_sold" in report["by_hop"][0]["datasets"]
    assert report["cross_product_edges"]
    assert upstream(graph, "glue://reg_cur/mart_energy_sold", 1)["by_hop"]


def test_in_place_writer_not_inferred_as_dependency(tmp_path):
    (tmp_path / "a.sql").write_text("CREATE TABLE db.t AS SELECT x FROM db.source")
    (tmp_path / "purge.sql").write_text("UPDATE db.t SET x = NULL WHERE x < 0")
    (tmp_path / "b.sql").write_text("CREATE TABLE db.out AS SELECT x FROM db.t")
    graph = scan(tmp_path)
    assert [d.job_id for d in graph.document.job_dependencies["b"]] == ["a"]


def test_deterministic_roundtrip_and_export_preserves_prior(tmp_path):
    (tmp_path / "job.sql").write_text("CREATE TABLE b.t AS SELECT x AS y FROM a.s")
    graph = scan(tmp_path)
    path = tmp_path / "lineage.json"
    write_native(graph.document, path)
    initial = path.read_bytes()
    write_native(read_native(path), path)
    assert path.read_bytes() == initial
    write_native(scan(tmp_path).document, path)
    assert path.read_bytes() == initial
    prior = {
        "databases": [
            {
                "db_name": "b",
                "tables": [
                    {
                        "table_name": "t",
                        "schema": [
                            {
                                "field_name": "y",
                                "description": "human",
                                "verify": True,
                                "to_tokenize": True,
                            }
                        ],
                    }
                ],
            }
        ],
        "relations": [{"custom": "kept"}],
    }
    result = export_agent_catalog(graph.document, prior)
    column = result["databases"][0]["tables"][0]["schema"][0]
    assert column["description"] == "human" and column["verify"] and column["to_tokenize"]
    assert result["relations"] == prior["relations"]
    assert "dataset_id" not in prior["databases"][0]["tables"][0]
    events = export_openlineage(graph.document)
    assert events == export_openlineage(graph.document)
    field = events[0]["outputs"][0]["facets"]["columnLineage"]["fields"]["y"]
    assert field["inputFields"][0]["field"] == "x"


def test_custom_orchestrator_plugin_shares_graph_contract(tmp_path):
    class CustomParser:
        name = "example_scheduler"
        extensions = {".flow"}

        def accepts(self, source):
            return source.suffix == ".flow"

        def analyze(self, source, context):
            return WorkerResult(
                jobs=[
                    Job(
                        id="custom",
                        name="custom",
                        source_file=source.path,
                        inputs=["glue://a/s"],
                        outputs=["glue://b/t"],
                    )
                ],
                table_edges=[
                    TableEdge(
                        source="glue://a/s",
                        target="glue://b/t",
                        job_id="custom",
                        provenance=Provenance(parser=self.name),
                    )
                ],
            )

    (tmp_path / "workflow.flow").write_text("custom format")
    registry = ParserRegistry()
    registry.register(CustomParser())
    with pytest.raises(ValueError):
        registry.register(CustomParser())
    assert scan(tmp_path, parsers=registry).document.jobs[0].id == "custom"


def test_cli_scan_export_impact_and_errors(tmp_path):
    runner = CliRunner()
    source = tmp_path / "job.sql"
    source.write_text("CREATE TABLE b.t AS SELECT x FROM a.s")
    out = tmp_path / "lineage.json"
    assert runner.invoke(app, ["scan", str(source), "--out", str(out)]).exit_code == 0
    result = runner.invoke(app, ["impact", str(out), "glue://a/s"])
    assert result.exit_code == 0 and "glue://b/t" in result.stdout
    catalog = tmp_path / "catalog.json"
    assert runner.invoke(app, ["export", "catalog", str(out), "--out", str(catalog)]).exit_code == 0
    source.write_text("SELECT FROM")
    result = runner.invoke(app, ["scan", str(source), "--out", str(out)])
    assert result.exit_code == 1 and read_native(out).unresolved


def test_pandas_and_polars_projection_transforms(tmp_path):
    (tmp_path / "pandas_job.py").write_text(
        "import pandas as p\n"
        "from sqlalchemy import create_engine\n"
        'connection = create_engine("postgresql://localhost/db")\n'
        'df = p.read_sql_table("src", connection, schema="public")\n'
        'out = df[["x"]].rename(columns={"x": "renamed"})\n'
        'out["twice"] = out["renamed"] * 2\n'
        'out.to_sql("dst", connection, schema="public")'
    )
    (tmp_path / "polars_job.py").write_text(
        "import polars as pl\n"
        'df = pl.read_parquet("s3://bucket/input")\n'
        'out = df.select((pl.col("x") * 2).alias("twice"))\n'
        'out.write_parquet("s3://bucket/output")'
    )
    graph = scan(tmp_path)
    pandas = next(
        e
        for e in graph.document.column_edges
        if e.job_id == "pandas_job" and e.target.name == "twice"
    )
    polars = next(e for e in graph.document.column_edges if e.job_id == "polars_job")
    assert [(s.dataset_id, s.name) for s in pandas.sources] == [("postgres://public/src", "x")]
    assert [(s.dataset_id, s.name) for s in polars.sources] == [("s3://bucket/input", "x")]


def test_runtime_tables_never_become_fake_datasets(tmp_path):
    (tmp_path / "job.py").write_text(
        'env = os.getenv("ENV", "dev")\n'
        'table = f"db.table_{env}"\nspark.table(table).write.saveAsTable("db.out")'
    )
    doc = scan(tmp_path).document
    assert any(u.assumptions == {"env:ENV": "dev"} for u in doc.unresolved)
    assert not any("table_dev" in d.id or "{{" in d.id for d in doc.datasets)
    resolved = scan(tmp_path, bindings={"env:ENV": "prod"}).document
    assert "glue://db/table_prod" in resolved.jobs[0].inputs


def test_scanner_does_not_follow_symlinked_sources(tmp_path):
    outside = tmp_path.parent / "outside_etl.py"
    outside.write_text('spark.table("secret.source")')
    (tmp_path / "link.py").symlink_to(outside)
    assert not RepoScanner(tmp_path).scan().files


def test_sql_bindings_and_engine_select_dialect(tmp_path):
    path = tmp_path / "job.sql"
    path.write_text('CREATE TABLE "Out"."Dest" AS SELECT x FROM public.t_{{ env }}')
    unresolved = scan(path, sql_engine="postgres").document
    assert unresolved.unresolved[0].kind == "dynamic_sql"
    doc = scan(path, sql_engine="postgres", bindings={"env": "prod"}).document
    assert doc.jobs[0].dialect == "postgres"
    assert doc.jobs[0].inputs == ["postgres://public/t_prod"]
    assert doc.jobs[0].outputs == ["postgres://Out/Dest"]


def test_window_columns_are_indirect_and_preserve_renamed_origins(tmp_path):
    (tmp_path / "job.py").write_text(
        "from pyspark.sql import functions as F, Window\n"
        'df = spark.table("a.s").select(F.col("raw").alias("x"))\n'
        'w = Window.orderBy("x")\n'
        'out = df.withColumn("rank", F.row_number().over(w))\n'
        'out.write.saveAsTable("b.t")'
    )
    doc = scan(tmp_path).document
    edge = next(e for e in doc.column_edges if e.target.name == "rank")
    assert not edge.sources
    assert [(s.dataset_id, s.name) for s in edge.indirect_sources] == [("glue://a/s", "raw")]
    assert edge.transformation.kind == "window"
