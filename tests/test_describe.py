import asyncio
import copy
import io
import json

import pytest

from etl_parser.describe.engine import DescriptionEngine
from etl_parser.export.agent_catalog import export_agent_catalog
from etl_parser.pipeline import scan

pytest.importorskip("agent_sdk", reason="Install the trusted gdtc-agent-sdk to test descriptions")
from agent_sdk.testing import FakeLLMRunner  # noqa: E402
from agent_sdk.types import LLMResponse  # noqa: E402


def batch_response(columns, table=None, confidence=0.8):
    """Build one batched describe response for a table and its columns."""
    payload = {
        "columns": [
            {
                "name": name,
                "description": text,
                "confidence": confidence,
                "rationale": "Derived from the supplied transformation",
            }
            for name, text in columns.items()
        ]
    }
    if table is not None:
        payload["table"] = {
            "description": table,
            "confidence": confidence,
            "rationale": "Summarised from the columns it holds",
        }
    return json.dumps(payload)


def test_identity_inherits_and_expression_uses_grounded_prompt(tmp_path):
    (tmp_path / "job.sql").write_text(
        "CREATE TABLE b.t AS SELECT x AS renamed, x * 2 AS twice FROM a.s"
    )
    doc = scan(tmp_path).document
    catalog = export_agent_catalog(doc)
    source = next(d for d in catalog["databases"] if d["db_name"] == "a")
    source["tables"][0]["schema"][0]["description"] = "Amount in cents"
    client = FakeLLMRunner([batch_response({"twice": "Double the amount in cents"})])
    engine = DescriptionEngine(client, model="test-model")
    result = engine.run(doc, catalog)
    target = next(d for d in result["databases"] if d["db_name"] == "b")
    columns = {c["field_name"]: c for c in target["tables"][0]["schema"]}
    assert columns["renamed"]["description"] == "Amount in cents"
    assert columns["twice"]["description_source"] == "ai"
    assert columns["twice"]["ai_confidence"] == 0.8
    assert columns["twice"]["ai_model"] == "test-model"
    assert columns["twice"]["ai_rationale"]
    assert len(client.calls) == 1
    assert "Amount in cents" in client.calls[0]["messages"][0].content
    assert "* 2" in client.calls[0]["messages"][0].content
    assert client.calls[0]["tools"] == []
    assert client.calls[0]["kwargs"]["model"] == "test-model"
    assert engine.summary == {"generated": 1, "inherited": 1, "skipped_existing": 0, "failed": 0}


def test_one_call_per_table_covers_all_of_its_columns(tmp_path):
    (tmp_path / "job.sql").write_text(
        "CREATE TABLE b.t AS SELECT x * 2 AS twice, x + 1 AS bumped FROM a.s;\n"
        "CREATE TABLE b.u AS SELECT twice * 3 AS tripled FROM b.t;\n"
    )
    doc = scan(tmp_path).document
    client = FakeLLMRunner(
        [
            batch_response({"twice": "Twice the source", "bumped": "Source plus one"}, "First hop"),
            batch_response({"tripled": "Three times twice"}, "Second hop"),
        ]
    )
    engine = DescriptionEngine(client, model="test-model")
    result = engine.run(doc, export_agent_catalog(doc))
    tables = {t["dataset_id"]: t for d in result["databases"] for t in d["tables"]}
    assert len(client.calls) == 2
    assert tables["glue://b/t"]["description"] == "First hop"
    assert tables["glue://b/t"]["description_source"] == "ai"
    assert tables["glue://b/t"]["ai_model"] == "test-model"
    assert {c["field_name"] for c in tables["glue://b/t"]["schema"] if c.get("description")} == {
        "twice",
        "bumped",
    }
    # Upstream columns are described before the downstream prompt is built.
    assert "Twice the source" in client.calls[1]["messages"][0].content


def test_prompt_carries_column_facts_and_no_provenance_tokens(tmp_path):
    (tmp_path / "job.sql").write_text("CREATE TABLE b.t AS SELECT x * 2 AS twice FROM a.s")
    doc = scan(tmp_path).document
    catalog = export_agent_catalog(doc)
    table = next(t for d in catalog["databases"] for t in d["tables"] if t["table_name"] == "t")
    table["description"] = "Existing table description"
    column = next(c for c in table["schema"] if c["field_name"] == "twice")
    column.update(datatype="bigint", is_partition=False, is_categorical=False)
    client = FakeLLMRunner([batch_response({"twice": "Twice the source value"})])
    DescriptionEngine(client, model="test-model").run(doc, catalog)
    payload = client.calls[0]["messages"][0].content
    system = client.calls[0]["system"]
    assert "bigint" in payload and "is_partition" in payload
    assert "Existing table description" in payload
    for banned in ("provenance", "job_id", "agent_sdk_ai", "confidence_label"):
        assert banned not in payload
    transformations = json.loads(payload)["columns"][0]["transformations"]
    assert transformations[0]["expression"] and transformations[0]["kind"]
    assert set(transformations[0]) == {
        "expression",
        "kind",
        "sources",
        "indirect_sources",
        "source_file",
        "line_start",
        "line_end",
    }
    assert "confidence" in system and "rationale" in system


def test_existing_descriptions_are_regenerated_only_with_override(tmp_path):
    (tmp_path / "job.sql").write_text("CREATE TABLE b.t AS SELECT x * 2 AS twice FROM a.s")
    doc = scan(tmp_path).document
    catalog = export_agent_catalog(doc)
    table = next(t for d in catalog["databases"] for t in d["tables"] if t["table_name"] == "t")
    column = next(c for c in table["schema"] if c["field_name"] == "twice")
    column.update(description="Model text", description_source="ai")
    table.update(description="Human text", description_source="human")
    engine = DescriptionEngine(FakeLLMRunner([]), model="test-model")
    result = engine.run(doc, catalog)
    assert not engine.warnings
    assert engine.summary["skipped_existing"] == 1
    kept = next(t for d in result["databases"] for t in d["tables"] if t["table_name"] == "t")
    assert kept["schema"][0]["description"] == "Model text"

    client = FakeLLMRunner([batch_response({"twice": "Regenerated text"}, "Never written")])
    result = DescriptionEngine(client, model="test-model", override_existing=True).run(doc, catalog)
    overridden = next(t for d in result["databases"] for t in d["tables"] if t["table_name"] == "t")
    assert overridden["schema"][0]["description"] == "Regenerated text"
    # Human table text is never replaced, even when overriding is requested.
    assert overridden["description"] == "Human text"


def test_malformed_description_is_skipped_with_warning(tmp_path):
    (tmp_path / "job.sql").write_text("CREATE TABLE b.t AS SELECT x * 2 AS y FROM a.s")
    doc = scan(tmp_path).document
    engine = DescriptionEngine(FakeLLMRunner(["not JSON"]), model="test-model")
    engine.run(doc, export_agent_catalog(doc))
    assert len(engine.warnings) == 1


@pytest.mark.parametrize("stop_reason", ["max_tokens", "guardrail_intervened", "tool_use"])
def test_incomplete_responses_never_update_catalog(tmp_path, stop_reason):
    (tmp_path / "job.sql").write_text("CREATE TABLE b.t AS SELECT x * 2 AS y FROM a.s")
    doc = scan(tmp_path).document
    catalog = export_agent_catalog(doc)
    original = copy.deepcopy(catalog)
    runner = FakeLLMRunner(
        [
            LLMResponse(
                content=[{"type": "text", "text": batch_response({"y": "bad"})}],
                stop_reason=stop_reason,
            )
        ]
    )
    engine = DescriptionEngine(runner, model="test-model")
    assert engine.run(doc, catalog) == original
    assert catalog == original
    assert engine.warnings


def test_async_entry_and_sync_entry_loop_guard(tmp_path):
    (tmp_path / "job.sql").write_text("CREATE TABLE b.t AS SELECT x * 2 AS y FROM a.s")
    doc = scan(tmp_path).document
    engine = DescriptionEngine(
        FakeLLMRunner([batch_response({"y": "Twice x"}), batch_response({"y": "Twice x"})]),
        model="test",
    )

    async def check():
        with pytest.raises(RuntimeError, match="await engine.arun"):
            engine.run(doc, export_agent_catalog(doc))
        await engine.arun(doc, export_agent_catalog(doc))

    asyncio.run(check())
    assert not engine.warnings


@pytest.mark.parametrize("web_adapter", [True, False])
def test_real_sdk_lambda_invoke_contract(tmp_path, monkeypatch, web_adapter):
    from etl_parser.describe.client import bedrock_lambda_runner

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    (tmp_path / "job.sql").write_text("CREATE TABLE b.t AS SELECT x * 2 AS y FROM a.s")
    doc = scan(tmp_path).document
    calls = []

    class LambdaTransport:
        def invoke(self, **kwargs):
            calls.append(kwargs)
            response = {
                "content": [{"type": "text", "text": batch_response({"y": "Twice x"})}],
                "stop_reason": "end_turn",
                "usage": {"input_tokens": 1, "output_tokens": 1},
            }
            return {
                "StatusCode": 200,
                "Payload": io.BytesIO(
                    json.dumps({"statusCode": 200, "body": json.dumps(response)}).encode()
                ),
            }

    runner = bedrock_lambda_runner("test-function", web_adapter=web_adapter)
    runner._lambda = LambdaTransport()  # Only AWS transport is replaced; SDK handles the rest.
    model = "anthropic.claude-3-haiku-20240307-v1:0"
    engine = DescriptionEngine(runner, model=model)
    engine.run(doc, export_agent_catalog(doc))
    assert not engine.warnings
    assert calls[0]["FunctionName"] == "test-function"
    payload = json.loads(calls[0]["Payload"])
    if web_adapter:
        payload = json.loads(payload["body"])
    assert payload["modelId"] == model
    assert payload["payload"]["messages"][0]["role"] == "user"


def test_description_usage_and_decisions_are_logged_without_payloads(tmp_path):
    from agent_sdk.usage import Usage

    (tmp_path / "job.sql").write_text("CREATE TABLE b.t AS SELECT x * 2 AS y FROM a.s")
    doc = scan(tmp_path).document
    runner = FakeLLMRunner(
        [
            LLMResponse(
                content=[{"type": "text", "text": batch_response({"y": "PRIVATE DESCRIPTION"})}],
                stop_reason="end_turn",
                usage=Usage(
                    input_tokens=20,
                    output_tokens=5,
                    cache_read_tokens=3,
                    raw={"provider_private_value": "DO NOT LOG RAW"},
                ),
            )
        ]
    )
    engine = DescriptionEngine(runner, model="test-model")
    engine.run(doc, export_agent_catalog(doc), log_dir=tmp_path / "logs")
    folder = next((tmp_path / "logs").iterdir())
    metrics = json.loads((folder / "metrics.json").read_text())
    events = (folder / "events.jsonl").read_text()
    assert metrics["counters"]["ai.calls.attempted"] == 1
    assert metrics["counters"]["ai.usage.input_tokens"] == 20
    assert metrics["counters"]["ai.usage.cache_read_tokens"] == 3
    assert metrics["counters"]["descriptions.generated"] == 1
    assert "ai.request.started" in events and "description.accepted" in events
    assert "PRIVATE DESCRIPTION" not in events
    assert "DO NOT LOG RAW" not in events


def test_provider_error_is_counted_without_logging_error_payload(tmp_path):
    (tmp_path / "job.sql").write_text("CREATE TABLE b.t AS SELECT x * 2 AS y FROM a.s")
    doc = scan(tmp_path).document

    def unavailable(*args):
        raise RuntimeError("SENSITIVE_PROVIDER_ERROR")

    engine = DescriptionEngine(FakeLLMRunner([unavailable]), model="test-model")
    engine.run(doc, export_agent_catalog(doc), log_dir=tmp_path / "logs")
    folder = next((tmp_path / "logs").iterdir())
    metrics = json.loads((folder / "metrics.json").read_text())
    assert metrics["counters"]["ai.calls.failed"] == 1
    assert metrics["status"] == "partial"
    assert "SENSITIVE_PROVIDER_ERROR" not in (folder / "events.jsonl").read_text()
