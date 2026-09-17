"""These checks run even when the private SDK is not installed."""

import builtins
import subprocess
import sys

import pytest
from typer.testing import CliRunner

from etl_parser.cli import app
from etl_parser.describe.client import bedrock_lambda_runner


def test_missing_sdk_has_actionable_message(monkeypatch):
    real_import = builtins.__import__

    def without_sdk(name, *args, **kwargs):
        if name.startswith("agent_sdk"):
            raise ModuleNotFoundError(name)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", without_sdk)
    with pytest.raises(RuntimeError, match="trusted internal distribution"):
        bedrock_lambda_runner("example")


def test_empty_lambda_is_rejected_before_loading_sdk():
    with pytest.raises(ValueError, match="Lambda"):
        bedrock_lambda_runner(" ")


def test_cli_uses_lambda_sdk_options_only():
    result = CliRunner().invoke(app, ["describe", "--help"])
    assert result.exit_code == 0
    assert "lambda-arn" in result.output
    assert "web-adapter" in result.output
    assert "--client" not in result.output


def test_scanning_never_imports_sdk_or_aws(tmp_path):
    source = tmp_path / "job.sql"
    source.write_text("CREATE TABLE db.t AS SELECT x FROM db.s")
    code = """
import builtins
import sys
real_import = builtins.__import__
def guarded(name, *args, **kwargs):
    if name.split('.')[0] in {'agent_sdk', 'boto3', 'botocore'}:
        raise AssertionError(f'Scan imported an optional provider: {name}')
    return real_import(name, *args, **kwargs)
builtins.__import__ = guarded
from etl_parser.cli import app
from etl_parser.pipeline import scan
assert scan(sys.argv[1]).document.column_edges
"""
    result = subprocess.run(
        [sys.executable, "-c", code, str(source)], capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, result.stderr
