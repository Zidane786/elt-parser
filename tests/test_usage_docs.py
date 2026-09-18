import json
from pathlib import Path

import pytest
from typer.main import get_command
from typer.testing import CliRunner

from etl_parser.cli import app
from etl_parser.export.agent_catalog import export_agent_catalog
from etl_parser.pipeline import scan
from etl_parser.workers.sql import DictSchemaProvider

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    "command",
    [
        [],
        ["run"],
        ["scan"],
        ["describe"],
        ["export"],
        ["export", "catalog"],
        ["export", "openlineage"],
        ["impact"],
        ["products"],
    ],
)
def test_every_command_has_help_without_credentials(command, monkeypatch):
    import builtins

    original = builtins.__import__

    def no_provider(name, *args, **kwargs):
        if name.startswith(("agent_sdk", "boto3")):
            raise AssertionError("Help must not import providers")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", no_provider)
    result = CliRunner().invoke(app, [*command, "--help"])
    assert result.exit_code == 0, result.output
    assert "Usage:" in result.output


def test_readme_covers_every_registered_option():
    text = (ROOT / "README.md").read_text()

    def check(command):
        for parameter in command.params:
            for option in getattr(parameter, "opts", []) + getattr(parameter, "secondary_opts", []):
                if option.startswith("--"):
                    assert option in text, f"Missing README documentation for {option}"
        for child in getattr(command, "commands", {}).values():
            check(child)

    check(get_command(app))


def test_catalog_template_is_valid_schema_and_prior(tmp_path):
    template = json.loads((ROOT / "catalog.template.json").read_text())
    schema = DictSchemaProvider(template)
    assert schema.columns("glue://raw/orders") == ["order_id", "amount", "order_date"]
    source = tmp_path / "job.sql"
    source.write_text(
        "CREATE TABLE analytics.order_totals AS "
        "SELECT order_id, amount * 2 AS double_amount, order_date FROM raw.orders"
    )
    doc = scan(source, schema=schema).document
    assert not doc.unresolved and len(doc.column_edges) == 3
    catalog = export_agent_catalog(doc, template)
    assert catalog["scripts"][0]["writes_to"][0]["target"] == "analytics.order_totals"
    assert catalog["databases"][0]["tables"][0]["schema"][0]["description"]
