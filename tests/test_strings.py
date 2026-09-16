import ast

from etl_parser.scanner.strings import PLACEHOLDER, collect_constants, fold_string


def _expr(src: str, env=None):
    return fold_string(ast.parse(src, mode="eval").body, env or {})


def test_plain_and_fstring_with_known_names():
    assert _expr('"a.b"').text == "a.b"
    f = _expr('f"select * from {DB}.orders where dt = {dt}"', {"DB": "ecom"})
    assert f.text == f"select * from ecom.orders where dt = {PLACEHOLDER}"
    assert not f.complete and f.placeholders == ["dt"]


def test_concat_percent_and_format():
    assert _expr('"a" + "." + "b"').complete
    assert _expr('"a" + "." + "b"').text == "a.b"
    assert _expr('"select %s from %s" % ("x", "t")').text == "select x from t"
    assert _expr('"from {}.{}".format("db", "t")').text == "from db.t"
    assert _expr('"from {db}.{t}".format(db="d", t="t")').text == "from d.t"
    r = _expr('"from {db}".format(db=name)')
    assert r.text == f"from {PLACEHOLDER}" and r.placeholders == ["name"]


def test_environ_default_and_missing():
    assert _expr('os.environ.get("TARGET", "analytics.t")').text == "analytics.t"
    assert _expr('os.getenv("TARGET", "x")').text == "x"
    r = _expr('os.environ["TARGET"]')
    assert not r.complete and r.placeholders == ["env:TARGET"]


def test_join_and_str_methods():
    assert _expr('", ".join(["a", "b"])').text == "a, b"
    assert _expr('"  X ".strip().lower()').text == "x"


def test_collect_constants_chains_in_order():
    tree = ast.parse(
        'DB = "ecom"\nTABLE = f"{DB}.orders"\nOTHER = unknown()\nPATH: str = "s3://b/" + TABLE\n'
    )
    env = collect_constants(tree)
    assert env == {"DB": "ecom", "TABLE": "ecom.orders", "PATH": "s3://b/ecom.orders"}


def test_format_specs_escapes_and_stale_constants():
    assert _expr('f"t_{3:03d}"').text == "t_003"
    assert _expr('"t_%s_%%" % "prod"').text == "t_prod_%"
    assert not _expr('"t_%s_%s" % "prod"').complete
    assert not _expr('"t_{".format()').complete
    assert _expr('"xxTABLExx".strip("x")').text == "TABLE"
    assert collect_constants(ast.parse('T="old"\nT=dynamic()')) == {}


def test_numeric_formats_and_arithmetic_do_not_corrupt_table_names():
    from etl_parser.scanner.strings import Folded

    assert _expr('"t_%03d" % 2').text == "t_002"
    assert _expr('"t_{:03d}".format(2)').text == "t_002"
    assert _expr('f"t_{n:03d}"', {"n": Folded("2", True, value_type="int")}).text == "t_002"
    assert _expr('f"t_{1 + 2}"').text == "t_3"
    assert not _expr('"t_" + 2').complete
    assert not _expr('f"t_{1:1000000000d}"').complete
