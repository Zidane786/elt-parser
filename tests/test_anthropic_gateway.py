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
            "model": "example-model",
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
        assert request.headers["x-example-mode"] == "invoke"
        assert request.headers["x-example-stream"] == "true"
        body = json.loads(request.content)
        assert body["model"] == "example-model"
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
            model="example-model",
            extra_headers={"x-example-mode": "invoke", "x-example-stream": "true"},
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
        assert "x-example-mode" not in request.headers
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
            "example-model",
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
            model="example-model",
            ai_lineage="improve",
        ),
        log_dir=tmp_path / "logs",
    )
    assert len(calls) == 1 and all(c.is_closed for c in clients)
    assert result.warnings and result.document == result.baseline
    if status >= 400:
        assert result.decisions[0]["http_status"] == status
    assert "PRIVATE_PROVIDER_BODY" not in "".join(
        f.read_text() for f in (tmp_path / "logs").rglob("*") if f.is_file()
    )


@pytest.mark.parametrize("status", [401, 403, 404, 429])
def test_provider_circuit_stops_following_files(monkeypatch, tmp_path, status):
    from etl_parser import ParserClient

    for name in ("a", "b"):
        (tmp_path / f"{name}.sql").write_text(f"CREATE TABLE db.{name} AS SELECT x FROM db.s")
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(status, text="PRIVATE_PROVIDER_BODY", headers={"Retry-After": "45"})

    install_transport(monkeypatch, handler)
    result = ParserClient(
        config=AnalysisConfig(
            runner="anthropic",
            api_key="PRIVATE_KEY",
            base_url="https://gateway.invalid",
            model="test",
            ai_lineage="improve",
        ),
        log_level="ERROR",
    ).run(tmp_path)
    assert len(calls) == 1
    assert result.status == "partial"
    assert result.document == result.baseline
    assert result.decisions[0]["http_status"] == status
    assert result.decisions[1]["reason"] == "provider_unavailable"
    assert result.metrics["counters"]["ai.skipped.provider"] == 1
    assert "PRIVATE_PROVIDER_BODY" not in json.dumps(result.to_dict())
    if status == 429:
        assert result.decisions[0]["retry_after_seconds"] == 45


def test_output_token_limit_and_explicit_token_setting(monkeypatch, tmp_path):
    source = tmp_path / "job.sql"
    source.write_text("CREATE TABLE db.t AS SELECT x FROM db.s")

    def handler(request):
        assert json.loads(request.content)["max_tokens"] == 16000
        data = response('{"columns":[').json()
        data["stop_reason"] = "max_tokens"
        return httpx.Response(200, json=data)

    install_transport(monkeypatch, handler)
    result = analyze(
        source,
        config=AnalysisConfig(
            runner="anthropic",
            api_key="PRIVATE_KEY",
            base_url="https://gateway.invalid",
            model="test",
            ai_lineage="improve",
        ),
    )
    assert result.decisions[0]["reason"] == "output_token_limit"
    assert result.document == result.baseline


def test_invalid_schema_reports_safe_details_and_preserves_graph(monkeypatch, tmp_path):
    source = tmp_path / "job.sql"
    source.write_text("CREATE TABLE db.t AS SELECT x FROM db.s")
    install_transport(monkeypatch, lambda _: response('{"columns":[{"kind":"PRIVATE_VALUE"}]}'))
    result = analyze(
        source,
        config=AnalysisConfig(
            runner="anthropic",
            api_key="PRIVATE_KEY",
            base_url="https://gateway.invalid",
            model="test",
            ai_lineage="improve",
        ),
    )
    failure = result.decisions[0]
    assert failure["reason"] == "response_schema_invalid"
    assert failure["response_digest"]
    assert failure["validation_errors"]
    assert "PRIVATE_VALUE" not in json.dumps(failure)
    assert result.document == result.baseline


@pytest.mark.parametrize(
    "base_url,model",
    [
        ("https://gateway.invalid", "test"),
        ("https://gateway.example/api/anthropic", "EXAMPLE-MODEL"),
    ],
)
def test_public_sdk_preserves_credentials_and_token_override(
    monkeypatch, tmp_path, base_url, model
):
    from etl_parser import ParserClient

    source = tmp_path / "job.sql"
    source.write_text("CREATE TABLE db.t AS SELECT x FROM db.s")

    def handler(request):
        assert str(request.url) == base_url + "/v1/messages"
        assert request.headers["x-api-key"] == "PRIVATE_KEY"
        assert "x-example-mode" not in request.headers
        assert json.loads(request.content)["model"] == model
        assert json.loads(request.content)["max_tokens"] == 2222
        return response('{"complete":true}')

    clients = install_transport(monkeypatch, handler)
    result = ParserClient(
        config=AnalysisConfig(
            runner="anthropic",
            api_key="PRIVATE_KEY",
            base_url=base_url,
            model=model,
            ai_lineage="improve",
            max_output_tokens=2222,
        ),
        log_level="ERROR",
    ).run(source)
    assert not result.warnings and result.metrics["counters"]["ai.calls.completed"] == 1
    assert all(client.is_closed for client in clients)
    assert "PRIVATE_KEY" not in json.dumps(result.to_dict())


@pytest.mark.parametrize(
    "error,reason",
    [
        (httpx.ReadTimeout, "timeout"),
        (httpx.RemoteProtocolError, "provider_transport_failure"),
    ],
)
def test_sdk_wrapped_transport_errors_are_classified_without_payload(
    monkeypatch, tmp_path, error, reason
):
    from etl_parser import ParserClient

    source = tmp_path / "job.sql"
    source.write_text("CREATE TABLE db.t AS SELECT x FROM db.s")

    def handler(request):
        raise error("PRIVATE_TRANSPORT_DETAILS", request=request)

    install_transport(monkeypatch, handler)
    result = ParserClient(
        config=AnalysisConfig(
            runner="anthropic",
            api_key="PRIVATE_KEY",
            base_url="https://gateway.invalid",
            model="test",
            ai_lineage="improve",
        ),
        log_level="ERROR",
    ).run(source)
    assert result.decisions[0]["reason"] == reason
    assert result.decisions[0]["transport_error_type"] == error.__name__
    assert result.document == result.baseline
    assert "PRIVATE_TRANSPORT_DETAILS" not in json.dumps(result.to_dict())
