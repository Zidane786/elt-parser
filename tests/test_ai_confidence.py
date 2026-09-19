"""Model-reported confidence, thresholds, AI origin marking and evidence rules (WP-E)."""

import json

import pytest

from etl_parser.ai_analysis import AnalysisConfig, analyze

pytest.importorskip("agent_sdk", reason="Install trusted SDK for AI contract tests")
from agent_sdk.testing import FakeLLMRunner, text_response  # noqa: E402


def proposal_response(messages, system, tools, kwargs, *, confidence=0.9, rationale="Direct copy"):
    """Build one schema-valid response echoing the deterministic edge with confidence."""
    data = json.loads(messages[0].content)
    source = data["source"]
    edge = data["deterministic_edges"][0]
    return text_response(
        json.dumps(
            {
                "complete": True,
                "columns": [
                    {
                        "job_id": edge["job_id"],
                        "target": edge["target"],
                        "sources": [{"dataset_id": "glue://db/s", "name": "x"}],
                        "expression": edge["transformation"]["expression"],
                        "kind": edge["transformation"]["kind"],
                        "confidence": confidence,
                        "rationale": rationale,
                        "evidence": {
                            "source_file": source["source_file"],
                            "source_digest": source["source_digest"],
                            "line_start": 1,
                            "line_end": 1,
                            "quote": source["lines"][0][1],
                        },
                    }
                ],
                "descriptions": [
                    {
                        "target": edge["target"],
                        "description": "The source value carried through unchanged.",
                        "confidence": confidence,
                        "rationale": rationale,
                    }
                ],
            }
        )
    )


def catalog_column(catalog, dataset_id, name):
    """Return one catalog column dict by dataset id and field name."""
    return next(
        c
        for d in catalog["databases"]
        for t in d["tables"]
        if t.get("dataset_id") == dataset_id
        for c in t["schema"]
        if c["field_name"] == name
    )


def test_response_schema_requires_confidence_and_rationale():
    from etl_parser.ai_analysis import AnalysisResponse

    with pytest.raises(ValueError):
        AnalysisResponse.model_validate(
            {"descriptions": [{"target": {"dataset_id": "glue://db/t", "name": "x"}}]}
        )
    with pytest.raises(ValueError):
        AnalysisResponse.model_validate(
            {
                "descriptions": [
                    {
                        "target": {"dataset_id": "glue://db/t", "name": "x"},
                        "description": "text",
                        "confidence": 1.5,
                        "rationale": "too confident",
                    }
                ]
            }
        )


def test_accepted_edge_and_description_carry_model_confidence(tmp_path):
    source = tmp_path / "job.py"
    source.write_text('spark.table("db.s").select("x").custom().write.saveAsTable("db.t")')
    result = analyze(
        source,
        runner=FakeLLMRunner([proposal_response]),
        config=AnalysisConfig(ai_lineage="fallback", descriptions=True, model="test-model"),
    )
    edge = next(e for e in result.document.column_edges if e.provenance.parser == "agent_sdk_ai")
    assert edge.provenance.ai_confidence == 0.9
    assert edge.provenance.ai_rationale == "Direct copy"
    column = catalog_column(result.catalog, "glue://db/t", "x")
    assert column["description_source"] == "ai"
    assert column["ai_confidence"] == 0.9
    assert column["ai_rationale"] == "Direct copy"
    assert column["ai_model"] == "test-model"


def test_below_threshold_proposal_is_deferred_but_visible(tmp_path):
    source = tmp_path / "job.py"
    source.write_text('spark.table("db.s").select("x").custom().write.saveAsTable("db.t")')
    result = analyze(
        source,
        runner=FakeLLMRunner([lambda *args: proposal_response(*args, confidence=0.2)]),
        config=AnalysisConfig(
            ai_lineage="fallback", descriptions=True, model="test", min_ai_confidence=0.5
        ),
    )
    change = next(c for c in result.changes if c["kind"] == "column")
    assert change["status"] == "deferred"
    assert change["reason"] == "below_confidence_threshold"
    assert result.document == result.baseline
    assert result.ai_document.column_edges
    assert not catalog_column(result.catalog, "glue://db/t", "x")["description"]
