"""Regression tests for the graph, impact, OpenLineage, registry and identity fixes.

Covers findings 3, 11, 12, 13 and 27-31 of ``docs/reports/2026-09-19-full-review.md``
(work package WP-B of ``docs/superpowers/plans/2026-09-19-review-fixes.md``).
"""

from pathlib import Path

import pytest

from etl_parser.graph.builder import LineageGraph, build_graph
from etl_parser.graph.impact import downstream
from etl_parser.models import (
    ColumnEdge,
    ColumnRef,
    DatasetRef,
    Job,
    LineageDocument,
    Provenance,
    TableEdge,
    WorkerResult,
)
from etl_parser.pipeline import scan

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
