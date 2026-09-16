from etl_parser.workers.base import first_docstring_line, normalize_cron, parse_header


def test_parse_header_from_docstring_and_sql_comment():
    doc = (
        '"""Build thing.\n\nSource: a.b, c.d\nTarget: x.y\nOwner: me@x\n'
        'Schedule: hourly\nDependencies: j1\n"""'
    )
    h = parse_header(doc)
    assert h["source"] == "a.b, c.d" and h["schedule"] == "hourly" and h["owner"] == "me@x"
    sql = "-- Title line\n--\n-- Source: s\n-- Schedule: 0 5 1 * *\nCREATE TABLE ..."
    assert parse_header(sql)["schedule"] == "0 5 1 * *"
    assert first_docstring_line(doc) == "Build thing."


def test_normalize_cron_variants():
    assert normalize_cron("0 4 * * *") == "0 4 * * *"
    assert normalize_cron("@daily") == "0 0 * * *"
    assert normalize_cron("hourly") == "0 * * * *"
    assert normalize_cron("daily 02:00 UTC") == "0 2 * * *"
    assert normalize_cron("every 15 minutes") == "*/15 * * * *"
    assert normalize_cron("timedelta(hours=1)") is None
    assert normalize_cron("timedelta(hours=1, minutes=30)") is None
    assert normalize_cron("on-demand (kicked off via Jira)") is None
    assert normalize_cron("@once") is None


def test_invalid_and_inexact_schedules_are_not_cron():
    for value in (
        "daily 25:99",
        "every 0 minutes",
        "every 7 minutes",
        "every 2 days",
        "timedelta(hours=0)",
        "90 30 * * *",
        "*/0 * * * *",
        "daily 01:00 junk",
    ):
        assert normalize_cron(value) is None
    assert normalize_cron("0 9 * JAN MON-FRI") == "0 9 * JAN MON-FRI"


def test_header_does_not_read_later_code_or_comments():
    assert parse_header("x = 1\n# Owner: unrelated\n") == {}
    assert parse_header('"""Owner: real\n"""\nx = 1\n# Schedule: @daily') == {"owner": "real"}
    assert first_docstring_line('#!/usr/bin/python\n"""Title."""') == "Title."
