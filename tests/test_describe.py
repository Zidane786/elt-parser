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


def test_identity_inherits_and_expression_uses_grounded_prompt(tmp_path):
    (tmp_path / "job.sql").write_text(
        "CREATE TABLE b.t AS SELECT x AS renamed, x * 2 AS twice FROM a.s"
    )
    doc = scan(tmp_path).document
    catalog = export_agent_catalog(doc)
    source = next(d for d in catalog["databases"] if d["db_name"] == "a")
    source["tables"][0]["schema"][0]["description"] = "Amount in cents"
    client = FakeLLMRunner(['{"description":"Double the amount in cents"}'])
    result = DescriptionEngine(client, model="test-model").run(doc, catalog)
    target = next(d for d in result["databases"] if d["db_name"] == "b")
    columns = {c["field_name"]: c for c in target["tables"][0]["schema"]}
    assert columns["renamed"]["description"] == "Amount in cents"
    assert columns["twice"]["description_source"] == "ai"
    assert len(client.calls) == 1
    assert "Amount in cents" in client.calls[0]["messages"][0].content
    assert "* 2" in client.calls[0]["messages"][0].content
    assert client.calls[0]["tools"] == []
    assert client.calls[0]["kwargs"]["model"] == "test-model"


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
                content=[{"type": "text", "text": '{"description":"bad"}'}], stop_reason=stop_reason
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
    engine = DescriptionEngine(FakeLLMRunner(['{"description":"Twice x"}']), model="test")

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
                "content": [{"type": "text", "text": '{"description":"Twice x"}'}],
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
