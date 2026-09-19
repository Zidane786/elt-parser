"""Regression tests for the graph, impact, OpenLineage, registry and identity fixes.

Covers findings 3, 11, 12, 13 and 27-31 of ``docs/reports/2026-09-19-full-review.md``
(work package WP-B of ``docs/superpowers/plans/2026-09-19-review-fixes.md``).
"""

from pathlib import Path

import pytest
from openlineage.client.facet_v2 import column_lineage_dataset as cl
from openlineage.client.serde import Serde

from etl_parser.export.native import write_native
from etl_parser.export.openlineage_out import PRODUCER, export_openlineage
from etl_parser.graph.builder import LineageGraph, build_graph, dependency_names
from etl_parser.graph.impact import downstream
from etl_parser.identity import (
    agent_table_name,
    dataset_ref_from_id,
    is_unresolved_dataset_id,
    normalize_dataset_id,
    split_dataset_id,
)
from etl_parser.models import (
    ColumnEdge,
    ColumnRef,
    DatasetRef,
    Job,
    JoinCondition,
    LineageDocument,
    Provenance,
    Schedule,
    TableEdge,
    WorkerResult,
)
from etl_parser.pipeline import scan
from etl_parser.registry import ProductRegistry

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="module")
def etl_graph():
    """Scan the PySpark golden fixture once for every test that needs it."""
    return scan(FIXTURES / "etl")


@pytest.fixture(scope="module")
def products_graph():
    """Scan the multi-product fixture once for every test that needs it."""
    return scan(FIXTURES / "products")


def test_sole_in_place_writer_becomes_a_flagged_dependency(tmp_path):
    """Finding 3: the only writer of a dataset is a dependency even when it reads it."""
    (tmp_path / "scd.sql").write_text(
        "INSERT INTO db.dim SELECT c FROM db.staging UNION ALL SELECT c FROM db.dim"
    )
    (tmp_path / "mart.sql").write_text("CREATE TABLE db.mart AS SELECT c FROM db.dim")
    deps = scan(tmp_path).document.job_dependencies["mart"]
    assert [d.job_id for d in deps] == ["scd"]
    assert deps[0].in_place_writer is True
    assert deps[0].via_datasets == ["glue://db/dim"]


def test_in_place_writer_still_skipped_when_another_writer_exists(tmp_path):
    """Finding 3: an in-place writer stays excluded while a real producer exists."""
    (tmp_path / "a.sql").write_text("CREATE TABLE db.t AS SELECT x FROM db.source")
    (tmp_path / "purge.sql").write_text("UPDATE db.t SET x = NULL WHERE x < 0")
    (tmp_path / "b.sql").write_text("CREATE TABLE db.out AS SELECT x FROM db.t")
    deps = scan(tmp_path).document.job_dependencies["b"]
    assert [d.job_id for d in deps] == ["a"]
    assert deps[0].in_place_writer is False


def test_in_place_writer_flag_is_per_linking_dataset():
    """Finding 29: the flag describes the linking dataset, not the upstream job."""
    provenance = Provenance(parser="sqlglot")
    upstream_job = Job(
        id="upstream",
        name="upstream",
        source_file="upstream.sql",
        inputs=["glue://db/loop", "glue://db/src"],
        outputs=["glue://db/loop", "glue://db/handoff"],
    )
    downstream_job = Job(
        id="downstream",
        name="downstream",
        source_file="downstream.sql",
        inputs=["glue://db/handoff"],
        outputs=["glue://db/out"],
    )
    result = WorkerResult(
        jobs=[upstream_job, downstream_job],
        table_edges=[
            TableEdge(
                source="glue://db/src",
                target="glue://db/handoff",
                job_id="upstream",
                provenance=provenance,
            ),
            TableEdge(
                source="glue://db/handoff",
                target="glue://db/out",
                job_id="downstream",
                provenance=provenance,
            ),
        ],
    )
    dep = build_graph([result]).document.job_dependencies["downstream"][0]
    assert dep.job_id == "upstream"
    assert dep.via_datasets == ["glue://db/handoff"]
    assert dep.in_place_writer is False


def test_job_io_edges_make_declared_reads_visible_to_impact(etl_graph):
    """Finding 12: a declared read with no column lineage still reaches impact."""
    doc = etl_graph.document
    edge = next(
        e
        for e in doc.table_edges
        if e.source == "glue://analytics_warehouse/fact_sessions"
        and e.target == "glue://analytics_warehouse/mart_funnel_conversion"
    )
    assert edge.provenance.parser == "job_io"
    assert edge.provenance.confidence == "partial"
    assert edge.job_id == "mart_funnel_conversion"
    report = downstream(etl_graph, "glue://analytics_warehouse/fact_sessions", 1)
    assert "mart_funnel_conversion" in report["by_hop"][0]["jobs"]
    assert "glue://analytics_warehouse/mart_funnel_conversion" in report["by_hop"][0]["datasets"]


def test_job_io_edges_never_duplicate_a_parsed_edge(tmp_path):
    """The fallback only fires for input/output pairs no parser already linked."""
    (tmp_path / "job.sql").write_text("CREATE TABLE db.t AS SELECT x FROM db.s")
    edges = scan(tmp_path).document.table_edges
    assert [(e.source, e.target, e.provenance.parser) for e in edges] == [
        ("glue://db/s", "glue://db/t", "sqlglot")
    ]


def test_job_io_edges_cover_every_unlinked_pair_without_changing_dependencies():
    """Fallback edges are informational: depends_on still comes from inputs/outputs."""
    result = WorkerResult(
        jobs=[
            Job(
                id="reader",
                name="reader",
                source_file="reader.py",
                inputs=["glue://db/a", "glue://db/b"],
                outputs=["glue://db/out"],
            ),
            Job(
                id="writer",
                name="writer",
                source_file="writer.sql",
                inputs=["glue://db/seed"],
                outputs=["glue://db/a"],
            ),
        ],
        table_edges=[
            TableEdge(
                source="glue://db/a",
                target="glue://db/out",
                job_id="reader",
                provenance=Provenance(parser="python_ast"),
            )
        ],
    )
    doc = build_graph([result]).document
    fallback = {(e.source, e.target) for e in doc.table_edges if e.provenance.parser == "job_io"}
    assert fallback == {("glue://db/b", "glue://db/out"), ("glue://db/seed", "glue://db/a")}
    assert [d.job_id for d in doc.job_dependencies["reader"]] == ["writer"]


def _transformations(fields, column):
    """Return every transformation emitted for one output column of a facet."""
    return [t for reference in fields[column]["inputFields"] for t in reference["transformations"]]


def test_openlineage_indirect_subtypes_come_from_the_edge_kind(tmp_path):
    """Finding 13: INDIRECT transformations carry the subtype the spec defines.

    The kind belongs to the projection, so a subtype is emitted only where the projection
    itself says which clause used the column (aggregation, window). An identity projection
    whose indirect sources came from a filter or join carries no clause information, and
    the exporter leaves the subtype unset rather than guessing one.
    """
    (tmp_path / "job.sql").write_text(
        "CREATE TABLE db.t AS SELECT customer_id, sum(amount) AS total "
        "FROM db.s JOIN db.d ON db.s.k = db.d.k GROUP BY customer_id"
    )
    fields = export_openlineage(scan(tmp_path).document)[0]["outputs"][0]["facets"][
        "columnLineage"
    ]["fields"]
    everything = _transformations(fields, "total") + _transformations(fields, "customer_id")
    assert {t["type"] for t in everything} <= {"DIRECT", "INDIRECT"}
    assert all(
        t.get("subtype") in {"IDENTITY", "TRANSFORMATION", "AGGREGATION"}
        for t in everything
        if t["type"] == "DIRECT"
    )
    aggregated = _transformations(fields, "total")
    assert {t.get("subtype") for t in aggregated if t["type"] == "INDIRECT"} == {"GROUP_BY"}
    identity = _transformations(fields, "customer_id")
    assert {t.get("subtype") for t in identity if t["type"] == "INDIRECT"} == {None}


def test_openlineage_window_edges_get_the_window_subtype(tmp_path):
    """Finding 13: a window projection maps its indirect sources to WINDOW."""
    (tmp_path / "job.py").write_text(
        "from pyspark.sql import functions as F, Window\n"
        'df = spark.table("a.s").select(F.col("raw").alias("x"))\n'
        'w = Window.orderBy("x")\n'
        'out = df.withColumn("rank", F.row_number().over(w))\n'
        'out.write.saveAsTable("b.t")\n'
    )
    fields = export_openlineage(scan(tmp_path).document)[0]["outputs"][0]["facets"][
        "columnLineage"
    ]["fields"]
    assert [t.get("subtype") for t in _transformations(fields, "rank")] == ["WINDOW"]


def test_openlineage_field_transformation_type_stays_spec_valid(tmp_path):
    """Finding 13: the deprecated field-level type never emits invented values."""
    (tmp_path / "job.sql").write_text(
        "CREATE TABLE db.t AS SELECT x AS kept, x + 1 AS changed FROM db.s"
    )
    fields = export_openlineage(scan(tmp_path).document)[0]["outputs"][0]["facets"][
        "columnLineage"
    ]["fields"]
    assert fields["kept"]["transformationType"] == "IDENTITY"
    # Serde strips nulls: an unknown transformation omits the field rather than inventing
    # a value such as the old "EXPRESSION", which no OpenLineage version defines.
    assert fields["changed"].get("transformationType") is None


def test_openlineage_ignores_job_io_edges_in_column_facets():
    """job_io edges record a declared read, not traced column lineage."""
    document = _cross_product_document()
    document.column_edges[0].provenance = Provenance(parser="job_io", confidence="partial")
    outputs = export_openlineage(document)[0]["outputs"]
    assert "columnLineage" not in outputs[0]["facets"]


def test_openlineage_events_validate_against_the_client_models(tmp_path):
    """Spec 10: every emitted facet value is representable by openlineage-python."""
    (tmp_path / "job.sql").write_text("CREATE TABLE db.t AS SELECT x AS y FROM db.s WHERE x > 0")
    event = export_openlineage(scan(tmp_path).document)[0]
    assert event["producer"] == PRODUCER
    assert event["eventType"] == "COMPLETE"
    for field in event["outputs"][0]["facets"]["columnLineage"]["fields"].values():
        # Constructing the typed models rejects stray keys and non-spec shapes.
        rebuilt = cl.Fields(
            inputFields=[
                cl.InputField(
                    namespace=reference["namespace"],
                    name=reference["name"],
                    field=reference["field"],
                    transformations=[cl.Transformation(**t) for t in reference["transformations"]],
                )
                for reference in field["inputFields"]
            ],
            transformationDescription=field.get("transformationDescription"),
            transformationType=field.get("transformationType"),
        )
        assert Serde.to_dict(rebuilt) == field


def test_registry_reads_schedules_written_as_scripts():
    """Finding 31: meter's product.yaml uses schedules.scripts, not schedules.steps."""
    registry = ProductRegistry.load(FIXTURES / "products" / "meter" / "product.yaml")
    schedules = registry.schedules()
    assert schedules["product.meter.stage_reads"].cron == "0 1 * * *"
    assert schedules["product.meter.ingest_interval_reads"].interval_text == "hourly"
    assert schedules["product.meter.__primary__"].cron == "0 2 * * *"


def test_registry_merges_steps_and_scripts(tmp_path):
    """Both spellings are read, and a step keeps its own schedule."""
    (tmp_path / "product.yaml").write_text(
        "code: p\nname: product\n"
        "schedules:\n"
        "  steps:\n    one: '0 1 * * *'\n"
        "  scripts:\n    two: '0 2 * * *'\n"
    )
    schedules = ProductRegistry.load(tmp_path).schedules()
    assert schedules["product.p.one"].cron == "0 1 * * *"
    assert schedules["product.p.two"].cron == "0 2 * * *"


def test_product_attach_honours_the_declared_database_engine(products_graph):
    """Finding 30: a Postgres table is not owned by a database declared as Athena."""
    products = {d.id: d.product for d in products_graph.document.datasets}
    assert products["glue://meter_cur/fact_consumption"] == "meter"
    assert products["postgres://meter_cur/fact_consumption"] is None
    assert products["postgres://billing_pg/invoices"] == "bill"


def test_engine_mismatch_is_reported_rather_than_silently_unattributed(products_graph):
    """A declared database seen on another engine leaves a non-gating note behind.

    The bill product declares bill_raw/bill_stg/bill_cur as Athena while its jobs run them
    over Postgres, so those datasets are no longer attributed to it. That is drift worth
    seeing, not something to swallow.
    """
    reasons = [
        u.reason
        for u in products_graph.document.unresolved
        if u.kind == "analysis_note" and "another engine" in u.reason
    ]
    for dataset_id in ("postgres://bill_cur/fact_invoice", "postgres://meter_cur/fact_consumption"):
        assert any(dataset_id in reason for reason in reasons), dataset_id
    assert not any("glue://meter_cur/fact_consumption" in reason for reason in reasons)


def test_registry_matches_engine_families_to_schemes(tmp_path):
    """Athena, Spark and Glue all address the glue scheme; postgres addresses postgres."""
    (tmp_path / "product.yaml").write_text(
        "code: p\nname: product\n"
        "databases:\n"
        "  - name: warehouse\n    type: athena\n    layer: curated\n"
        "  - name: ledger\n    type: postgresql\n    layer: source\n"
        "  - name: anything\n    layer: raw\n"
    )
    registry = ProductRegistry.load(tmp_path)
    assert registry.product_for_database("warehouse", "glue").code == "p"
    assert registry.product_for_database("warehouse", "postgres") is None
    assert registry.product_for_database("ledger", "postgres").code == "p"
    assert registry.layer_for_database("ledger", "postgres") == "source"
    assert registry.layer_for_database("ledger", "glue") is None
    # An undeclared engine still matches any scheme, so existing product.yaml keeps working.
    assert registry.product_for_database("anything", "glue").code == "p"
    assert registry.product_for_database("anything", "s3").code == "p"


def test_empty_and_schemeless_dataset_ids_never_raise():
    """Finding 27: degenerate identifiers return a sentinel instead of raising."""
    assert normalize_dataset_id("") == "unknown://unresolved/empty"
    assert normalize_dataset_id(".") == "unknown://unresolved/empty"
    assert normalize_dataset_id("   ", engine="spark") == "unknown://unresolved/empty"
    assert normalize_dataset_id("db.", engine="spark") == "glue://default/db"
    assert split_dataset_id("no-scheme") == ("unknown", "unresolved", "no-scheme")
    assert split_dataset_id("") == ("unknown", "unresolved", "")
    assert dataset_ref_from_id("no-scheme").namespace == "unknown://unresolved"
    assert agent_table_name("no-scheme") == "unresolved.no-scheme"
    assert is_unresolved_dataset_id("unknown://unresolved/empty")
    assert not is_unresolved_dataset_id("glue://db/t")


def test_unresolved_dataset_ids_reach_the_document_as_notes():
    """Finding 27: a call site that could not name a dataset is reported, not dropped."""
    result = WorkerResult(
        jobs=[
            Job(
                id="job",
                name="job",
                source_file="job.py",
                inputs=[normalize_dataset_id("")],
                outputs=["glue://db/t"],
            )
        ]
    )
    doc = build_graph([result]).document
    note = next(u for u in doc.unresolved if u.kind == "analysis_note")
    assert "unknown://unresolved/empty" in note.reason


def test_conflicting_aliases_are_a_note_not_unsupported_syntax():
    """Finding 30/plan: a conflicting alias is an analysis note, and never gates a scan."""
    shared = "s3://bucket/shared/"
    result = WorkerResult(
        datasets=[
            DatasetRef(id="glue://db/a", namespace="glue://db", name="a", aliases=[shared]),
            DatasetRef(id="glue://db/b", namespace="glue://db", name="b", aliases=[shared]),
        ]
    )
    doc = build_graph([result]).document
    note = next(u for u in doc.unresolved if "Conflicting dataset alias" in u.reason)
    assert note.kind == "analysis_note"
    assert not [u for u in doc.unresolved if u.kind == "unsupported_syntax"]


def test_orchestrator_cycles_are_a_note_not_unsupported_syntax():
    """Plan: a task-graph cycle is reported without failing the scan."""
    schedules = {
        "one": Schedule(id="one", orchestrator="airflow", declared_upstream=["two"]),
        "two": Schedule(id="two", orchestrator="airflow", declared_upstream=["one"]),
    }
    doc = build_graph([WorkerResult(schedules=schedules)]).document
    note = next(u for u in doc.unresolved if "cycle" in u.reason)
    assert note.kind == "analysis_note"
    assert not [u for u in doc.unresolved if u.kind == "unsupported_syntax"]


def test_join_conditions_are_carried_through_deduped_and_sorted():
    """WP-D feeds join conditions in; the document must carry them deterministically."""
    provenance = Provenance(parser="sqlglot")

    def condition(left, right):
        return JoinCondition(
            left=ColumnRef(dataset_id="glue://db/a", name=left),
            right=ColumnRef(dataset_id="glue://db/b", name=right),
            job_id="job",
            provenance=provenance,
        )

    first = WorkerResult(join_conditions=[condition("z", "z"), condition("a", "a")])
    second = WorkerResult(join_conditions=[condition("a", "a"), condition("m", "m")])
    carried = build_graph([first, second]).document.join_conditions
    assert [c.left.name for c in carried] == ["a", "m", "z"]


def test_dependency_names_fall_back_to_job_id_when_script_names_collide(products_graph):
    """Finding 28: three products each have a gen_data and a load_to_athena script."""
    names = dependency_names(products_graph.document)
    assert names["bill/gen_data"] == "bill/gen_data"
    assert names["meter/gen_data"] == "meter/gen_data"
    assert names["reg/load_to_athena"] == "reg/load_to_athena"
    assert names["bill/jobs/rate_invoices"] == "rate_invoices"


def test_dependency_names_are_unique_per_document():
    """A display name never silently stands for two different jobs."""
    jobs = [
        Job(id="one/shared", name="shared", source_file="one/shared.py"),
        Job(id="two/shared", name="shared", source_file="two/shared.py"),
        Job(id="solo", name="solo", source_file="solo.py"),
    ]
    names = dependency_names(LineageDocument(jobs=jobs))
    assert names == {"one/shared": "one/shared", "two/shared": "two/shared", "solo": "solo"}
    assert len(set(names.values())) == len(names)


@pytest.mark.parametrize("fixture", ["etl", "products"])
def test_scan_output_is_byte_identical_across_runs(fixture, tmp_path):
    """Spec 13: two scans of the same tree serialize to the same bytes."""
    first, second = tmp_path / "first.json", tmp_path / "second.json"
    write_native(scan(FIXTURES / fixture).document, first)
    write_native(scan(FIXTURES / fixture).document, second)
    assert first.read_bytes() == second.read_bytes()


def _cross_product_document():
    """Build a two-product document with one table edge and one column edge across it."""
    provenance = Provenance(parser="sqlglot")
    return LineageDocument(
        datasets=[
            DatasetRef(
                id="glue://left/src",
                namespace="glue://left",
                name="src",
                product="left",
                columns=["x"],
            ),
            DatasetRef(
                id="glue://right/dst",
                namespace="glue://right",
                name="dst",
                product="right",
                columns=["y"],
            ),
        ],
        jobs=[
            Job(
                id="move",
                name="move",
                source_file="move.sql",
                inputs=["glue://left/src"],
                outputs=["glue://right/dst"],
            )
        ],
        table_edges=[
            TableEdge(
                source="glue://left/src",
                target="glue://right/dst",
                job_id="move",
                provenance=provenance,
            )
        ],
        column_edges=[
            ColumnEdge(
                target=ColumnRef(dataset_id="glue://right/dst", name="y"),
                sources=[ColumnRef(dataset_id="glue://left/src", name="x")],
                provenance=provenance,
                job_id="move",
            )
        ],
    )


def test_column_walk_reports_cross_product_edges():
    """Finding 11: column ids map back to datasets before the cross-product check."""
    graph = LineageGraph(_cross_product_document())
    report = downstream(graph, "glue://left/src#x")
    assert report["by_hop"][0]["columns"] == ["glue://right/dst#y"]
    assert report["cross_product_edges"] == [
        {
            "source": "glue://left/src",
            "target": "glue://right/dst",
            "from_product": "left",
            "to_product": "right",
        }
    ]


def test_dataset_walk_populates_columns():
    """Finding 11: a dataset walk lists the columns of the datasets it reaches."""
    graph = LineageGraph(_cross_product_document())
    hop = downstream(graph, "glue://left/src")["by_hop"][0]
    assert hop["datasets"] == ["glue://right/dst"]
    assert hop["columns"] == ["glue://right/dst#y"]


def test_job_io_edges_skip_self_loops():
    """An in-place writer's own dataset never becomes a source-equals-target edge."""
    result = WorkerResult(
        jobs=[
            Job(
                id="purge",
                name="purge",
                source_file="purge.sql",
                inputs=["glue://db/t"],
                outputs=["glue://db/t"],
            )
        ]
    )
    assert build_graph([result]).document.table_edges == []
