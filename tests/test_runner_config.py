"""Runner validation and secret boundaries work without installing the private SDK."""

import json

import pytest
from typer.testing import CliRunner

from etl_parser.ai_analysis import AnalysisConfig, analyze
from etl_parser.cli import app
from etl_parser.describe.client import RunnerConfig, configured_runner


@pytest.mark.parametrize(
    "url",
    [
        "http://gateway.invalid",
        "https://user:secret@gateway.invalid",
        "https://gateway.invalid/?token=secret",
        "https://gateway.invalid/#x",
    ],
)
def test_unsafe_gateway_urls_are_rejected(url):
    with pytest.raises(ValueError):
        RunnerConfig(base_url=url)


@pytest.mark.parametrize(
    "headers",
    [{"x-test": "bad\r\nInjected: true"}, {"Host": "other"}, {"x": 42}, {"X": "one", "x": "two"}],
)
def test_invalid_custom_headers_are_rejected(headers):
    with pytest.raises(ValueError):
        RunnerConfig(extra_headers=headers)


def test_credentials_never_serialize_and_defaults_need_no_runner(tmp_path):
    source = tmp_path / "job.sql"
    source.write_text("CREATE TABLE db.t AS SELECT x FROM db.s")
    config = AnalysisConfig(
        runner="anthropic",
        base_url="https://gateway.invalid/aigw",
        api_key="PRIVATE_API_KEY",
        extra_headers={"x-private": "HEADER_SECRET"},
    )
    assert "PRIVATE_API_KEY" not in repr(config)
    assert "HEADER_SECRET" not in config.model_dump_json()
    result = analyze(source, config=config, log_dir=tmp_path / "logs")
    assert result.document == result.baseline
    assert not result.work
    metadata = json.dumps(result.configuration) + "".join(
        f.read_text() for f in (tmp_path / "logs").rglob("*") if f.is_file()
    )
    assert "PRIVATE_API_KEY" not in metadata and "HEADER_SECRET" not in metadata


def test_selected_runner_requirements_are_checked_before_sdk_import():
    with pytest.raises(ValueError, match="lambda_arn"):
        configured_runner(RunnerConfig())
    with pytest.raises(ValueError, match="base_url and api_key"):
        configured_runner(RunnerConfig(runner="anthropic"))


@pytest.mark.parametrize("name", ["lambda-bedrock-invoke", "lbi"])
def test_lambda_runner_aliases_are_unambiguous(name):
    assert RunnerConfig(runner=name).runner == "lambda-bedrock-invoke"
    with pytest.raises(ValueError):
        RunnerConfig(runner="bedrock")


def test_cli_header_json_validation_and_env_config_overrides(tmp_path, monkeypatch):
    source = tmp_path / "job.sql"
    source.write_text("CREATE TABLE db.t AS SELECT x FROM db.s")
    settings = tmp_path / "settings.json"
    settings.write_text(
        json.dumps({"runner": "lambda-bedrock-invoke", "ai_lineage": "improve", "dry_run": True})
    )
    monkeypatch.setenv("ANTHROPIC_API_KEY", "CLI_PRIVATE_KEY")
    monkeypatch.setenv("ANTHROPIC_API_BASE_URL", "https://gateway.invalid/aigw")
    args = [
        "run",
        str(source),
        "--config",
        str(settings),
        "--runner",
        "anthropic",
        "--extra-headers",
        '{"x-example-mode":"invoke"}',
        "--out-dir",
        str(tmp_path / "out"),
    ]
    result = CliRunner().invoke(app, args)
    assert result.exit_code == 0, result.output
    manifest = next((tmp_path / "out").rglob("manifest.json"))
    config = json.loads(manifest.read_text())["configuration"]
    assert config["runner"] == "anthropic" and config["base_url"].endswith("/aigw")
    assert "CLI_PRIVATE_KEY" not in result.output + manifest.read_text()
    result = CliRunner().invoke(app, ["run", str(source), "--extra-headers", "SECRET_INVALID_JSON"])
    assert result.exit_code != 0 and "SECRET_INVALID_JSON" not in result.output
