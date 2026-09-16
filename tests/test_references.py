from etl_parser.scanner.references import inspect_references


def inspect(tmp_path, source, bindings=None):
    path = tmp_path / "job.py"
    path.write_text(source)
    return inspect_references(path, root=tmp_path, bindings=bindings)


def test_dynamic_template_preserves_variables_through_assignments(tmp_path):
    report = inspect(tmp_path, 'table = f"db.table_{env}"\nspark.table(table)')
    issue = report.unresolved[0]
    assert issue.partial_text == "db.table_{{?}}"
    assert issue.symbols == ["env"]
    assert issue.expression == "table" and issue.line == 2
    assert issue.remediation
    resolved = inspect(tmp_path, 'table = f"db.table_{env}"\nspark.table(table)', {"env": "prod"})
    assert resolved.references[0].value == "db.table_prod"
    assert not resolved.unresolved


def test_environment_defaults_are_auditable_and_overridable(tmp_path):
    source = 'env = os.getenv("ENV", "dev")\ntable = f"db.t_{env}"\nspark.table(table)'
    report = inspect(tmp_path, source)
    assert report.references[0].value == "db.t_dev"
    assert report.unresolved[0].assumptions == {"env:ENV": "dev"}
    resolved = inspect(tmp_path, source, {"env:ENV": "prod"})
    assert resolved.references[0].value == "db.t_prod"
    assert not resolved.unresolved


def test_reassignment_scopes_and_import_aliases(tmp_path):
    source = (
        'import pandas as p\ntable = "db.first"\nspark.table(table)\n'
        "table = unknown()\nspark.table(table)\n"
        "def helper(table):\n    spark.table(table)\n"
        'p.read_sql(f"SELECT x FROM {table}", conn)'
    )
    report = inspect(tmp_path, source)
    assert len(report.references) == 4
    assert report.references[0].value == "db.first"
    assert len(report.unresolved) == 3
    assert report.unresolved[-1].kind == "dynamic_sql"


def test_branch_assignment_is_not_promoted_to_certain_value(tmp_path):
    report = inspect(
        tmp_path, 'if condition:\n    table = "a"\nelse:\n    table = "b"\nspark.table(table)'
    )
    assert report.unresolved[0].symbols == ["table"]


def test_bad_file_is_reported(tmp_path):
    assert inspect_references(tmp_path / "missing.py").unresolved
