"""Real private SDK AnthropicRunner + mocked HTTP transport; no paid calls in CI."""

import json

import pytest
from typer.testing import CliRunner

pytest.importorskip("agent_sdk", reason="Install trusted private SDK for Anthropic contract tests")
import httpx  # noqa: E402

from etl_parser.ai_analysis import AnalysisConfig, analyze  # noqa: E402
from etl_parser.cli import app  # noqa: E402
from etl_parser.export.agent_catalog import export_agent_catalog  # noqa: E402
from etl_parser.export.native import write_native  # noqa: E402
from etl_parser.pipeline import scan  # noqa: E402


def install_transport(monkeypatch, handler):
    from agent_sdk.runners import _client_config

    original = _client_config.httpx.AsyncClient
    clients = []

    def client(**kwargs):
        result = original(**kwargs, transport=httpx.MockTransport(handler))
        clients.append(result)
        return result

    monkeypatch.setattr(_client_config.httpx, "AsyncClient", client)
    return clients


def response(text):
    return httpx.Response(
        200,
        json={
            "id": "test",
            "type": "message",
            "role": "assistant",
            "content": [{"type": "text", "text": text}],
            "stop_reason": "end_turn",
            "model": "codex/gpt-5.6-terra",
            "usage": {"input_tokens": 100, "output_tokens": 20},
        },
    )


@pytest.mark.parametrize(
    "mode,descriptions", [("off", True), ("improve", True), ("fallback", False)]
)
def test_real_anthropic_sdk_headers_model_and_analysis(monkeypatch, tmp_path, mode, descriptions):
    source = tmp_path / "job.py"
    source.write_text(
        'spark.table("db.s").select("x").custom().write.saveAsTable("db.t")'
        if mode == "fallback"
        else 'spark.table("db.s").select("x").write.saveAsTable("db.t")'
    )
    calls = []

    def handler(request):
        calls.append(request)
        assert str(request.url) == "https://gateway.invalid/aigw/v1/messages"
        assert request.headers["x-api-key"] == "PRIVATE_KEY"
        assert request.headers["x-duke-mode"] == "invoke"
        assert request.headers["x-duke-stream"] == "true"
        body = json.loads(request.content)
        assert body["model"] == "codex/gpt-5.6-terra"
        content = body["messages"][0]["content"]
        data = json.loads(content if isinstance(content, str) else content[0]["text"])
        edge = data["deterministic_edges"][0]
        context = data["source"]
        proposal = {k: edge[k] for k in ("job_id", "target", "sources", "indirect_sources")}
        proposal.update(
            expression=edge["transformation"]["expression"],
            kind=edge["transformation"]["kind"],
            evidence={
                "source_file": context["source_file"],
                "source_digest": context["source_digest"],
                "line_start": 1,
                "line_end": 1,
                "quote": context["lines"][0][1],
            },
        )
        return response(
            json.dumps(
                {
                    "complete": True,
                    "columns": [proposal],
                    "descriptions": [{"target": edge["target"], "description": "Source value"}]
                    if descriptions
                    else [],
                }
            )
        )

    clients = install_transport(monkeypatch, handler)
    result = analyze(
        source,
        config=AnalysisConfig(
            runner="anthropic",
            api_key="PRIVATE_KEY",
            base_url="https://gateway.invalid/aigw",
            model="codex/gpt-5.6-terra",
            extra_headers={"x-duke-mode": "invoke", "x-duke-stream": "true"},
            ai_lineage=mode,
            descriptions=descriptions,
        ),
        log_dir=tmp_path / "logs",
    )
    assert len(calls) == 1 and not result.warnings
    assert all(c.is_closed for c in clients)
    if mode == "off":
        assert result.document == result.baseline
    assert result.comparison["files"]
    assert "PRIVATE_KEY" not in "".join(
        f.read_text() for f in (tmp_path / "logs").rglob("*") if f.is_file()
    )


def test_legacy_describe_uses_anthropic_without_lambda_and_closes(monkeypatch, tmp_path):
    source = tmp_path / "job.sql"
    source.write_text("CREATE TABLE db.t AS SELECT x * 2 AS doubled FROM db.s")
    doc = scan(source).document
    native, catalog, out = (
        tmp_path / name for name in ("lineage.json", "catalog.json", "out.json")
    )
    write_native(doc, native)
    catalog.write_text(json.dumps(export_agent_catalog(doc)))
    calls = []

    def handler(request):
        calls.append(request)
        assert "x-duke-mode" not in request.headers
        return response('{"description":"Twice the source value"}')

    clients = install_transport(monkeypatch, handler)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "PRIVATE_KEY")
    result = CliRunner().invoke(
        app,
        [
            "describe",
            str(native),
            "--catalog",
            str(catalog),
            "--out",
            str(out),
            "--runner",
            "anthropic",
            "--base-url",
            "https://gateway.invalid/aigw",
            "--model",
            "codex/gpt-5.6-terra",
        ],
    )
    assert result.exit_code == 0, result.output
    assert len(calls) == 1 and all(c.is_closed for c in clients)
    assert "Twice the source value" in out.read_text()


@pytest.mark.parametrize("status", [401, 429, 500, 302])
def test_anthropic_errors_do_not_retry_follow_redirect_or_leak(monkeypatch, tmp_path, status):
    source = tmp_path / "job.sql"
    source.write_text("CREATE TABLE db.t AS SELECT x FROM db.s")
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(
            status, text="PRIVATE_PROVIDER_BODY", headers={"Location": "https://other.invalid"}
        )

    clients = install_transport(monkeypatch, handler)
    result = analyze(
        source,
        config=AnalysisConfig(
            runner="anthropic",
            api_key="PRIVATE_KEY",
            base_url="https://gateway.invalid",
            model="codex/gpt-5.6-terra",
            ai_lineage="improve",
        ),
        log_dir=tmp_path / "logs",
    )
    assert len(calls) == 1 and all(c.is_closed for c in clients)
    assert result.warnings and result.document == result.baseline
    assert "PRIVATE_PROVIDER_BODY" not in "".join(
        f.read_text() for f in (tmp_path / "logs").rglob("*") if f.is_file()
    )
