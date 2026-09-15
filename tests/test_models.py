import json

from etl_parser.models import (
    ColumnEdge,
    ColumnRef,
    DatasetRef,
    Job,
    LineageDocument,
    Provenance,
    Transformation,
    Unresolved,
)


def _edge(job: str, ds: str, col: str) -> ColumnEdge:
    return ColumnEdge(
        target=ColumnRef(dataset_id=ds, name=col),
        sources=[ColumnRef(dataset_id="glue://a/src", name="x")],
        transformation=Transformation(expression="x + 1", kind="expression"),
        provenance=Provenance(parser="sqlglot", dialect="trino"),
        job_id=job,
    )


def test_document_round_trip_and_sorted_is_deterministic():
    doc = LineageDocument(
        datasets=[
            DatasetRef(id="glue://b/t", namespace="glue://b", name="t"),
            DatasetRef(id="glue://a/t", namespace="glue://a", name="t"),
        ],
        jobs=[Job(id="z", name="z", source_file="z.py"), Job(id="a", name="a", source_file="a.py")],
        column_edges=[_edge("j2", "glue://b/t", "c"), _edge("j1", "glue://a/t", "c")],
        unresolved=[Unresolved(kind="dynamic_sql", reason="env var", source_file="z.py", line=3)],
    )
    a = doc.sorted().model_dump_json(indent=2)
    b = LineageDocument.model_validate_json(a).sorted().model_dump_json(indent=2)
    assert a == b
    parsed = json.loads(a)
    assert [d["id"] for d in parsed["datasets"]] == ["glue://a/t", "glue://b/t"]
    assert [j["id"] for j in parsed["jobs"]] == ["a", "z"]
    assert [e["job_id"] for e in parsed["column_edges"]] == ["j1", "j2"]


def test_summary_counts():
    doc = LineageDocument(
        column_edges=[_edge("j", "glue://a/t", "c")],
        unresolved=[
            Unresolved(kind="dynamic_sql", reason="r"),
            Unresolved(kind="dynamic_sql", reason="r2"),
        ],
    )
    s = doc.summary()
    assert s["column_edges_by_confidence"] == {"exact": 1}
    assert s["unresolved_by_kind"] == {"dynamic_sql": 2}
