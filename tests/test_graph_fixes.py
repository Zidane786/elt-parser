"""Regression tests for the graph, impact, OpenLineage, registry and identity fixes.

Covers findings 3, 11, 12, 13 and 27-31 of ``docs/reports/2026-09-19-full-review.md``
(work package WP-B of ``docs/superpowers/plans/2026-09-19-review-fixes.md``).
"""

from pathlib import Path

import pytest

from etl_parser.graph.builder import build_graph
from etl_parser.models import Job, Provenance, TableEdge, WorkerResult
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
