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
