import json

import pytest

from etl_parser.ai_analysis import AnalysisConfig, analyze

pytest.importorskip("agent_sdk", reason="Install trusted SDK for AI contract tests")
from agent_sdk.testing import FakeLLMRunner, text_response  # noqa: E402


def response_for(messages, system, tools, kwargs, *, source_name="x", bad_evidence=False):
    data = json.loads(messages[0].content)
    source = data["source"]
    assert tools == []
    edge = data["deterministic_edges"][0]
    response = {
        "complete": True,
        "columns": [
            {
                "job_id": edge["job_id"],
                "target": edge["target"],
                "sources": [{"dataset_id": "glue://db/s", "name": source_name}],
                "expression": edge["transformation"]["expression"]
                if source_name == "x" and edge["provenance"]["confidence"] == "exact"
                else source_name,
                "kind": edge["transformation"]["kind"]
                if edge["provenance"]["confidence"] == "exact"
                else "identity",
                "evidence": {
                    "source_file": source["source_file"],
                    "source_digest": source["source_digest"],
                    "line_start": 999 if bad_evidence else 1,
                    "line_end": 999 if bad_evidence else 1,
                    "quote": source["lines"][0][1],
                },
            }
        ],
        "descriptions": [{"target": edge["target"], "description": "Source value"}],
    }
    return text_response(json.dumps(response))


def test_background_comparison_shares_one_description_call_and_never_mutates(tmp_path):
    source = tmp_path / "job.sql"
    source.write_text("CREATE TABLE db.t AS SELECT x FROM db.s")
    runner = FakeLLMRunner([response_for])
    result = analyze(source, runner=runner, config=AnalysisConfig(descriptions=True, model="test"))
    assert len(runner.calls) == 1
    assert result.document == result.baseline
    assert result.ai_document.column_edges
    assert result.comparison["files"][0]["agreed"]
    assert any(
        c.get("description") == "Source value"
        for d in result.catalog["databases"]
        for t in d["tables"]
        for c in t["schema"]
    )


def test_no_background_call_when_descriptions_already_exist(tmp_path):
    source = tmp_path / "job.sql"
    source.write_text("CREATE TABLE db.t AS SELECT x FROM db.s")
    first = analyze(source)
    prior = first.catalog
    for database in prior["databases"]:
        for table in database["tables"]:
            for column in table["schema"]:
                column["description"] = "Human text"
    runner = FakeLLMRunner([])
    result = analyze(
        source, prior=prior, runner=runner, config=AnalysisConfig(descriptions=True, model="test")
    )
    assert not runner.calls
    assert not result.comparison["files"]


def test_fallback_only_calls_problem_file_and_preserves_partial_baseline(tmp_path):
    (tmp_path / "good.sql").write_text("CREATE TABLE db.good AS SELECT x FROM db.s")
    (tmp_path / "bad.py").write_text(
        'spark.table("db.s").select("x").custom().write.saveAsTable("db.t")'
    )
    runner = FakeLLMRunner([response_for])
    result = analyze(
        tmp_path, runner=runner, config=AnalysisConfig(ai_lineage="fallback", model="test")
    )
    assert len(runner.calls) == 1
    assert json.loads(runner.calls[0]["messages"][0].content)["source"]["source_file"] == "bad.py"
    assert any(e.provenance.parser == "agent_sdk_ai" for e in result.document.column_edges)
    assert all(e.provenance.confidence != "exact" for e in result.ai_document.column_edges)
    assert result.baseline.unresolved == result.document.unresolved
    assert any(c["status"] == "accepted" for c in result.changes)


def test_exact_conflict_is_deferred_and_conflicting_description_is_not_published(tmp_path):
    source = tmp_path / "job.sql"
    source.write_text("CREATE TABLE db.t AS SELECT x FROM db.s")
    runner = FakeLLMRunner([lambda *args: response_for(*args, source_name="y")])
    result = analyze(
        source,
        runner=runner,
        config=AnalysisConfig(ai_lineage="improve", descriptions=True, model="test"),
    )
    assert result.document == result.baseline
    assert result.changes[0]["status"] == "deferred"
    assert not any(
        c.get("description")
        for d in result.catalog["databases"]
        for t in d["tables"]
        for c in t["schema"]
    )


def test_invalid_evidence_is_rejected_and_cannot_mutate_main(tmp_path):
    source = tmp_path / "job.sql"
    source.write_text("CREATE TABLE db.t AS SELECT x FROM db.s")
    runner = FakeLLMRunner([lambda *args: response_for(*args, bad_evidence=True)])
    result = analyze(
        source, runner=runner, config=AnalysisConfig(ai_lineage="improve", model="test")
    )
    assert result.document == result.baseline
    assert result.changes[0]["reason"] == "evidence_line_range_invalid"


def test_budget_zero_makes_no_call(tmp_path):
    source = tmp_path / "job.sql"
    source.write_text("CREATE TABLE db.t AS SELECT x FROM db.s")
    runner = FakeLLMRunner([])
    result = analyze(
        source,
        runner=runner,
        config=AnalysisConfig(ai_lineage="improve", model="test", max_calls=0),
    )
    assert not runner.calls
    assert result.decisions[0]["reason"] == "budget_exhausted"


def test_oversized_source_skips_comparison_not_second_call(tmp_path):
    source = tmp_path / "job.sql"
    source.write_text("-- " + "long comment " * 3000 + "\nCREATE TABLE db.t AS SELECT x FROM db.s")

    def description_only(messages, system, tools, kwargs):
        data = json.loads(messages[0].content)
        assert data["request_lineage"] is False and data["source"] is None
        return text_response(
            json.dumps(
                {
                    "descriptions": [
                        {
                            "target": {"dataset_id": "glue://db/t", "name": "x"},
                            "description": "Value",
                        }
                    ]
                }
            )
        )

    runner = FakeLLMRunner([description_only])
    result = analyze(
        source,
        runner=runner,
        config=AnalysisConfig(descriptions=True, model="test", max_context_chars=20000),
    )
    assert len(runner.calls) == 1
    assert not result.comparison["files"]
    assert result.document == result.baseline


@pytest.mark.parametrize("mode", ["off", "fallback", "improve"])
@pytest.mark.parametrize("descriptions", [False, True])
def test_stage_control_matrix_on_exact_file(tmp_path, mode, descriptions):
    source = tmp_path / "job.sql"
    source.write_text("CREATE TABLE db.t AS SELECT x FROM db.s")
    runner = FakeLLMRunner([response_for])
    result = analyze(
        source,
        runner=runner,
        config=AnalysisConfig(ai_lineage=mode, descriptions=descriptions, model="test"),
    )
    assert len(runner.calls) == int(descriptions or mode == "improve")
    assert result.document == result.baseline


def test_disabled_background_ignores_unsolicited_lineage(tmp_path):
    source = tmp_path / "job.sql"
    source.write_text("CREATE TABLE db.t AS SELECT x FROM db.s")
    runner = FakeLLMRunner([text_response('{"complete": true}')])
    result = analyze(
        source,
        runner=runner,
        config=AnalysisConfig(descriptions=True, background_comparison=False, model="test"),
    )
    assert len(runner.calls) == 1
    assert not json.loads(runner.calls[0]["messages"][0].content)["request_lineage"]
    assert not result.comparison["files"]
    assert result.document == result.baseline


@pytest.mark.parametrize("payload", ["not json", '{"extra": 1}', '{"columns": "wrong"}'])
def test_malformed_ai_response_is_visible_and_non_mutating(tmp_path, payload):
    source = tmp_path / "job.sql"
    source.write_text("CREATE TABLE db.t AS SELECT x FROM db.s")
    runner = FakeLLMRunner([text_response(payload)])
    result = analyze(
        source, runner=runner, config=AnalysisConfig(ai_lineage="improve", model="test")
    )
    assert result.warnings and result.decisions[0]["status"] == "failed"
    assert result.document == result.baseline
    assert len(runner.calls) == 1  # No implicit paid repair/retry call.


def test_partial_description_requires_accepted_lineage_support(tmp_path):
    source = tmp_path / "job.py"
    source.write_text('spark.table("db.s").select("x").custom().write.saveAsTable("db.t")')

    def only_description(messages, *args):
        edge = json.loads(messages[0].content)["deterministic_edges"][0]
        return text_response(
            json.dumps(
                {
                    "descriptions": [
                        {
                            "target": edge["target"],
                            "description": "Unsubstantiated transformation",
                        }
                    ]
                }
            )
        )

    result = analyze(
        source,
        runner=FakeLLMRunner([only_description]),
        config=AnalysisConfig(ai_lineage="fallback", descriptions=True, model="test"),
    )
    assert not any(
        c.get("description")
        for d in result.catalog["databases"]
        for t in d["tables"]
        for c in t["schema"]
    )


@pytest.mark.parametrize(
    "mutation,reason",
    [
        ("job", "proposal_job_outside_file_scope"),
        ("digest", "evidence_snapshot_or_scope_mismatch"),
        ("table", "new_dataset_not_supported_by_literal_source"),
        ("dynamic", "invalid_or_dynamic_dataset_id"),
    ],
)
def test_adversarial_proposals_are_rejected(tmp_path, mutation, reason):
    source = tmp_path / "job.sql"
    source.write_text("CREATE TABLE db.t AS SELECT x FROM db.s")

    def malicious(*args):
        response = response_for(*args)
        payload = json.loads(response.content[0]["text"])
        proposal = payload["columns"][0]
        if mutation == "job":
            proposal["job_id"] = "unrelated_job"
        elif mutation == "digest":
            proposal["evidence"]["source_digest"] = "stale"
        else:
            proposal["target"]["dataset_id"] = (
                "glue://db/unrelated" if mutation == "table" else "glue://db/t_{env}"
            )
        return text_response(json.dumps(payload))

    result = analyze(
        source,
        runner=FakeLLMRunner([malicious]),
        config=AnalysisConfig(ai_lineage="improve", model="test"),
    )
    assert result.changes[0]["reason"] == reason
    assert result.document == result.baseline


def test_schema_membership_is_enforced(tmp_path):
    from etl_parser.workers.sql import DictSchemaProvider

    source = tmp_path / "job.sql"
    source.write_text("CREATE TABLE db.t AS SELECT x FROM db.s")
    result = analyze(
        source,
        schema=DictSchemaProvider({"db": {"s": ["x"], "t": ["x"]}}),
        runner=FakeLLMRunner([lambda *args: response_for(*args, source_name="invented")]),
        config=AnalysisConfig(ai_lineage="improve", model="test"),
    )
    assert result.changes[0]["reason"] == "column_absent_from_supplied_schema"
    assert result.document == result.baseline


def test_failed_audit_rolls_back_file_changes(tmp_path, monkeypatch):
    from etl_parser.observability import RunObserver

    source = tmp_path / "job.py"
    source.write_text('spark.table("db.s").select("x").custom().write.saveAsTable("db.t")')
    original = RunObserver.event

    def failing(self, event, **kwargs):
        original(self, event, **kwargs)
        if event == "ai.change_decided":
            self.sink_failed = True

    monkeypatch.setattr(RunObserver, "event", failing)
    result = analyze(
        source,
        runner=FakeLLMRunner([response_for]),
        config=AnalysisConfig(ai_lineage="fallback", descriptions=True, model="test"),
    )
    assert result.document == result.baseline
    assert result.warnings
    assert result.changes[0]["reason"] == "file_transaction_rolled_back"


def test_timeout_and_token_budget_do_not_trigger_retries(tmp_path):
    import asyncio

    source = tmp_path / "job.sql"
    source.write_text("CREATE TABLE db.t AS SELECT x FROM db.s")

    class SlowRunner:
        calls = 0

        async def complete(self, **kwargs):
            self.calls += 1
            await asyncio.sleep(10)

    runner = SlowRunner()
    result = analyze(
        source,
        runner=runner,
        config=AnalysisConfig(
            ai_lineage="improve",
            model="test",
            timeout_seconds=0.001,
        ),
    )
    assert runner.calls == 1
    assert result.decisions[0]["error_type"] == "TimeoutError"
    assert result.document == result.baseline
    runner = FakeLLMRunner([])
    result = analyze(
        source,
        runner=runner,
        config=AnalysisConfig(
            ai_lineage="improve",
            model="test",
            max_total_tokens=1,
        ),
    )
    assert not runner.calls and result.decisions[0]["reason"] == "token_budget"


def test_new_upstream_description_inherits_without_extra_background_call(tmp_path):
    (tmp_path / "a.sql").write_text("CREATE TABLE db.t AS SELECT x FROM db.s")
    (tmp_path / "b.sql").write_text("CREATE TABLE db.u AS SELECT x FROM db.t")
    runner = FakeLLMRunner([response_for])
    result = analyze(
        tmp_path, runner=runner, config=AnalysisConfig(descriptions=True, model="test")
    )
    assert len(runner.calls) == 1
    target = next(
        t
        for d in result.catalog["databases"]
        for t in d["tables"]
        if t["dataset_id"] == "glue://db/u"
    )
    assert target["schema"][0]["description_source"] == "inherited"


def test_table_and_transformation_differences_are_reported_without_replacing_exact(tmp_path):
    source = tmp_path / "job.sql"
    source.write_text("CREATE TABLE db.t AS SELECT x FROM db.s")

    def changed(*args):
        response = response_for(*args)
        payload = json.loads(response.content[0]["text"])
        item = payload["columns"][0]
        item["expression"] = "x + 999"
        item["kind"] = "expression"
        payload["tables"] = [
            {
                "job_id": item["job_id"],
                "source": "glue://db/s",
                "target": "glue://db/t",
                "evidence": item["evidence"],
            }
        ]
        return text_response(json.dumps(payload))

    result = analyze(
        source,
        runner=FakeLLMRunner([changed]),
        config=AnalysisConfig(ai_lineage="improve", descriptions=True, model="test"),
    )
    assert result.document == result.baseline
    comparison = result.comparison["files"][0]
    assert comparison["transformation_differences"]
    assert comparison["tables"]["agreed"]
    assert not any(
        c.get("description")
        for d in result.catalog["databases"]
        for t in d["tables"]
        for c in t["schema"]
    )


def test_new_ai_column_and_description_share_call_and_metrics_match_effective_graph(tmp_path):
    from etl_parser.observability import RunObserver

    source = tmp_path / "job.py"
    source.write_text('spark.table("db.s").select("x").custom().write.saveAsTable("db.t")')

    def new_column(*args):
        response = response_for(*args)
        payload = json.loads(response.content[0]["text"])
        payload["columns"][0]["target"]["name"] = "new_output"
        payload["descriptions"][0]["target"]["name"] = "new_output"
        return text_response(json.dumps(payload))

    runner = FakeLLMRunner([new_column])
    observer = RunObserver(log_level="ERROR")
    result = analyze(
        source,
        runner=runner,
        observer=observer,
        config=AnalysisConfig(ai_lineage="fallback", descriptions=True, model="test"),
    )
    observer.finish()
    assert len(runner.calls) == 1
    target = next(
        t
        for d in result.catalog["databases"]
        for t in d["tables"]
        if t["dataset_id"] == "glue://db/t"
    )
    assert next(c for c in target["schema"] if c["field_name"] == "new_output")["description"]
    assert observer.gauges["lineage.column_edges"] == len(result.document.column_edges)
    assert observer.gauges["lineage.deterministic.column_edges"] == len(
        result.baseline.column_edges
    )
    assert len(result.document.table_edges) == len(result.baseline.table_edges)
