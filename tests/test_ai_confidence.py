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


def test_ai_dataset_and_job_additions_are_marked_and_recorded(tmp_path):
    source = tmp_path / "job.py"
    source.write_text('spark.table("db.s").select("x").custom().write.saveAsTable("db.t")')

    def new_dataset(*args):
        payload = json.loads(proposal_response(*args).content[0]["text"])
        payload["columns"][0]["sources"] = [{"dataset_id": "glue://db/extra", "name": "x"}]
        payload["columns"][0]["expression"] = "x from db.extra"
        return text_response(json.dumps(payload))

    # db.extra is grounded in the text but never read deterministically, so the merged
    # graph gains that dataset only because the proposal was accepted.
    source.write_text(
        "# joins db.extra when the flag is on\n"
        'spark.table("db.s").select("x").custom().write.saveAsTable("db.t")'
    )
    result = analyze(
        source,
        runner=FakeLLMRunner([new_dataset]),
        config=AnalysisConfig(ai_lineage="fallback", model="test"),
    )
    dataset = next(d for d in result.document.datasets if d.id == "glue://db/extra")
    assert dataset.origin == "ai"
    assert dataset.provenance is not None
    assert dataset.provenance.parser == "agent_sdk_ai"
    assert dataset.provenance.ai_confidence == 0.9
    job = next(j for j in result.document.jobs if j.id == "job")
    assert "glue://db/extra" in job.ai_inputs
    assert "glue://db/extra" not in job.inputs
    assert job.origin == "parser"
    additions = {c["kind"] for c in result.changes}
    assert "dataset" in additions
    added = next(c for c in result.changes if c["kind"] == "dataset")
    assert added["status"] == "accepted"
    assert added["after"]["id"] == "glue://db/extra"


def test_ai_only_document_marks_every_job_and_dataset_as_ai(tmp_path):
    source = tmp_path / "job.py"
    source.write_text('spark.table("db.s").select("x").custom().write.saveAsTable("db.t")')
    result = analyze(
        source,
        runner=FakeLLMRunner([proposal_response]),
        config=AnalysisConfig(ai_lineage="fallback", model="test"),
    )
    # lineage.ai.json exists only because of the proposals, so nothing in it is parser work.
    assert result.ai_document.jobs
    assert all(j.origin == "ai" for j in result.ai_document.jobs)
    assert all(j.ai_outputs and not j.outputs for j in result.ai_document.jobs)
    assert all(d.origin == "ai" and d.provenance for d in result.ai_document.datasets)


def catalog_table(catalog, dataset_id):
    """Return one catalog table dict by dataset id."""
    return next(
        t for d in catalog["databases"] for t in d["tables"] if t.get("dataset_id") == dataset_id
    )


def with_table_description(*args, text="One row per source row.", confidence=0.8):
    """Add a table-level description proposal to the standard column response."""
    payload = json.loads(proposal_response(*args, confidence=confidence).content[0]["text"])
    payload["table_descriptions"] = [
        {
            "dataset_id": "glue://db/t",
            "description": text,
            "confidence": confidence,
            "rationale": "Summarised from the columns written here",
        }
    ]
    return text_response(json.dumps(payload))


def test_table_description_is_generated_alongside_its_columns(tmp_path):
    source = tmp_path / "job.sql"
    source.write_text("CREATE TABLE db.t AS SELECT x FROM db.s")
    runner = FakeLLMRunner([with_table_description])
    result = analyze(
        source, runner=runner, config=AnalysisConfig(descriptions=True, model="test-model")
    )
    assert len(runner.calls) == 1
    payload = json.loads(runner.calls[0]["messages"][0].content)
    assert payload["table_description_targets"] == ["glue://db/t"]
    assert payload["target_column_facts"]
    table = catalog_table(result.catalog, "glue://db/t")
    assert table["description"] == "One row per source row."
    assert table["description_source"] == "ai"
    assert table["ai_confidence"] == 0.8
    assert table["ai_model"] == "test-model"


def test_low_confidence_table_description_is_not_written(tmp_path):
    source = tmp_path / "job.sql"
    source.write_text("CREATE TABLE db.t AS SELECT x FROM db.s")
    result = analyze(
        source,
        runner=FakeLLMRunner([lambda *args: with_table_description(*args, confidence=0.1)]),
        config=AnalysisConfig(descriptions=True, model="test", min_ai_confidence=0.5),
    )
    assert not catalog_table(result.catalog, "glue://db/t")["description"]


def test_human_text_survives_override_but_generated_text_does_not(tmp_path):
    source = tmp_path / "job.sql"
    source.write_text("CREATE TABLE db.t AS SELECT x FROM db.s")
    prior = analyze(source).catalog
    table = catalog_table(prior, "glue://db/t")
    table.update(description="Human table text", description_source="human")
    column = catalog_column(prior, "glue://db/t", "x")
    column.update(description="Older model text", description_source="ai")
    result = analyze(
        source,
        prior=prior,
        runner=FakeLLMRunner([with_table_description]),
        config=AnalysisConfig(descriptions=True, model="test-model", override_existing=True),
    )
    regenerated = catalog_column(result.catalog, "glue://db/t", "x")
    assert regenerated["description"] == "The source value carried through unchanged."
    assert regenerated["ai_model"] == "test-model"
    assert catalog_table(result.catalog, "glue://db/t")["description"] == "Human table text"


def test_existing_generated_text_is_kept_without_override(tmp_path):
    source = tmp_path / "job.sql"
    source.write_text("CREATE TABLE db.t AS SELECT x FROM db.s")
    prior = analyze(source).catalog
    catalog_column(prior, "glue://db/t", "x").update(
        description="Older model text", description_source="ai"
    )
    runner = FakeLLMRunner([])
    result = analyze(
        source,
        prior=prior,
        runner=runner,
        config=AnalysisConfig(descriptions=True, model="test"),
    )
    assert not runner.calls
    assert catalog_column(result.catalog, "glue://db/t", "x")["description"] == "Older model text"


def test_missing_provider_configuration_raises_before_any_file(tmp_path):
    from etl_parser.ai_analysis import AnalysisPolicyError

    for name in ("a", "b"):
        (tmp_path / f"{name}.sql").write_text(f"CREATE TABLE db.{name} AS SELECT x FROM db.s")
    with pytest.raises(AnalysisPolicyError) as failure:
        analyze(
            tmp_path,
            config=AnalysisConfig(ai_lineage="improve", model="test", lambda_arn=None),
        )
    assert str(failure.value).startswith("provider_configuration_invalid:")
    assert "lambda_arn" in str(failure.value)


def test_dry_run_needs_no_provider_configuration(tmp_path):
    source = tmp_path / "job.sql"
    source.write_text("CREATE TABLE db.t AS SELECT x FROM db.s")
    result = analyze(
        source, config=AnalysisConfig(ai_lineage="improve", model="test", dry_run=True)
    )
    assert result.work and result.decisions[0]["reason"] == "dry_run"


def test_non_provider_failure_is_reported_as_internal_error(tmp_path, monkeypatch):
    import etl_parser.ai_analysis as ai_analysis

    source = tmp_path / "job.sql"
    source.write_text("CREATE TABLE db.t AS SELECT x FROM db.s")

    def boom(*args, **kwargs):
        raise KeyError("internal bookkeeping bug")

    monkeypatch.setattr(ai_analysis, "_proposal_edges", boom)
    result = analyze(
        source,
        runner=FakeLLMRunner([proposal_response]),
        config=AnalysisConfig(ai_lineage="improve", model="test"),
    )
    failure = result.decisions[0]
    assert failure["status"] == "failed"
    assert failure["reason"] == "internal_error"
    assert failure["error_type"] == "KeyError"
    assert result.document == result.baseline


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
