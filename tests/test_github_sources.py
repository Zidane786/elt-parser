import base64
import io
import zipfile

import pytest

from etl_parser.pipeline import scan
from etl_parser.sources import GitHubSource


def provider(files, **kwargs):
    calls = []
    entries = [
        {"path": path, "sha": str(i), "type": "blob", "mode": "100644", "size": len(value)}
        for i, (path, value) in enumerate(files.items())
    ]

    def transport(endpoint):
        calls.append(endpoint)
        if endpoint.startswith("commits/"):
            return {"sha": "pinned", "commit": {"tree": {"sha": "root"}}}
        if endpoint == "git/trees/root?recursive=1":
            return {"tree": entries, "truncated": False}
        content = list(files.values())[int(endpoint.rsplit("/", 1)[-1])]
        return {"encoding": "base64", "content": base64.b64encode(content).decode()}

    return GitHubSource("https://github.com/example/etl", transport=transport, **kwargs), calls


def test_remote_and_local_semantic_parity_without_checkout(tmp_path):
    files = {
        "job.py": b'from helper import load\nload().select("x").write.saveAsTable("db.t")',
        "helper.py": b'def load():\n    return spark.table("db.s")\n',
    }
    for path, value in files.items():
        (tmp_path / path).write_bytes(value)
    remote, calls = provider(files)
    actual = scan(remote.origin, source_provider=remote, scan_commit="same").document
    expected = scan(tmp_path, scan_commit="same").document
    assert actual == expected
    assert calls[0] == "commits/HEAD"
    assert calls[1] == "git/trees/root?recursive=1"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["helper.py", "job.py"]


def test_remote_file_scope_follows_sibling_zip_without_creating_helper_job():
    zipped = io.BytesIO()
    with zipfile.ZipFile(zipped, "w") as archive:
        archive.writestr("helper.py", 'def load():\n    return spark.table("db.s")')
    remote, _ = provider(
        {
            "jobs/job.py": b'from helper import load\nload().write.saveAsTable("db.t")',
            "jobs/helper.zip": zipped.getvalue(),
            "unrelated.py": b'spark.table("private.other")',
        },
        path="jobs/job.py",
    )
    doc = scan(remote.origin, source_provider=remote).document
    assert [j.id for j in doc.jobs] == ["job"]
    assert doc.jobs[0].inputs == ["glue://db/s"]
    assert doc.scan_commit == "pinned"


def test_truncated_tree_walk_is_complete_and_snapshot_pinned():
    calls = []

    def transport(endpoint):
        calls.append(endpoint)
        return {
            "commits/feature%2Fetl": {"sha": "pinned", "commit": {"tree": {"sha": "root"}}},
            "git/trees/root?recursive=1": {"truncated": True, "tree": []},
            "git/trees/root": {
                "truncated": False,
                "tree": [{"path": "jobs", "type": "tree", "sha": "nested"}],
            },
            "git/trees/nested": {
                "truncated": False,
                "tree": [{"path": "job.sql", "type": "blob", "mode": "100644", "sha": "file"}],
            },
            "git/blobs/file": {
                "encoding": "base64",
                "content": base64.b64encode(b"CREATE TABLE db.t AS SELECT x FROM db.s").decode(),
            },
        }[endpoint]

    remote = GitHubSource("https://github.com/example/etl", ref="feature/etl", transport=transport)
    doc = scan(remote.origin, source_provider=remote).document
    assert doc.jobs[0].inputs == ["glue://db/s"]
    assert "git/trees/nested" in calls
    assert doc.scan_commit == "pinned"


@pytest.mark.parametrize(
    "url",
    [
        "http://github.com/a/b",
        "https://evil.com/a/b",
        "https://user:secret@github.com/a/b",
        "https://github.com/a/b/tree/x",
    ],
)
def test_rejects_untrusted_or_ambiguous_urls(url):
    with pytest.raises(ValueError):
        GitHubSource(url)


def test_remote_limits_and_lfs_are_visible_diagnostics():
    remote, _ = provider(
        {"lfs.py": b"version https://git-lfs.github.com/spec/v1\n", "large.py": b"x" * 1000},
        max_file_bytes=100,
    )
    doc = scan(remote.origin, source_provider=remote).document
    assert len(doc.unresolved) == 2
    assert not doc.jobs


def test_no_redirect_token_forwarding():
    from etl_parser.sources import _NoRedirect

    assert _NoRedirect().redirect_request(None, None, 302, None, None, "https://evil.com") is None


@pytest.mark.parametrize("code", [401, 403, 404])
def test_auth_and_missing_file_failures_are_not_retried_or_leaked(code):
    from urllib.error import HTTPError

    source = GitHubSource("https://github.com/example/etl", token="PRIVATE_TOKEN")
    calls = []

    class Opener:
        def open(self, request, **kwargs):
            calls.append(request)
            assert request.get_header("Authorization") == "Bearer PRIVATE_TOKEN"
            assert request.full_url.startswith("https://api.github.com/repos/example/etl/")
            raise HTTPError(request.full_url, code, "PRIVATE MESSAGE", {}, None)

    source._opener = Opener()
    with pytest.raises(RuntimeError, match=f"HTTP {code}") as exc:
        source.list_files()
    assert "PRIVATE" not in str(exc.value)
    assert len(calls) == 1


@pytest.mark.parametrize("code", [429, 500, 502, 503, 504])
def test_transient_http_failures_have_bounded_retries(code, monkeypatch):
    from urllib.error import HTTPError

    source = GitHubSource("https://github.com/example/etl", retries=2)
    calls, sleeps = [], []

    class Opener:
        def open(self, request, **kwargs):
            calls.append(request)
            raise HTTPError(request.full_url, code, "failed", {}, None)

    source._opener = Opener()
    monkeypatch.setattr("etl_parser.sources.time.sleep", sleeps.append)
    with pytest.raises(RuntimeError, match=f"HTTP {code}"):
        source.list_files()
    assert len(calls) == 3 and sleeps == [1, 2]


def test_long_rate_limit_is_visible_not_a_long_blocking_sleep(monkeypatch):
    from urllib.error import HTTPError

    source = GitHubSource("https://github.com/example/etl")

    class Opener:
        def open(self, request, **kwargs):
            raise HTTPError(request.full_url, 403, "failed", {"Retry-After": "3600"}, None)

    source._opener = Opener()
    monkeypatch.setattr("etl_parser.sources.time.sleep", lambda _: pytest.fail("Must not sleep"))
    with pytest.raises(RuntimeError, match="scan incomplete"):
        source.list_files()


def test_tree_listing_is_not_limited_to_contents_api_1000_entries():
    remote, calls = provider({f"ignored_{i}.txt": b"unused" for i in range(1001)})
    index = remote.scan(extensions={".py", ".sql"})
    assert not index.files and not index.unresolved
    assert len(calls) == 2  # Complete tree; unsupported blobs need no network read.


def test_remote_archive_traversal_is_not_extracted(tmp_path, monkeypatch):
    zipped = io.BytesIO()
    with zipfile.ZipFile(zipped, "w") as archive:
        archive.writestr("../escape.py", "raise RuntimeError('must not run')")
        archive.writestr("valid.py", 'spark.table("db.s").write.saveAsTable("db.t")')
    remote, _ = provider({"lib.zip": zipped.getvalue()}, path="lib.zip")
    monkeypatch.chdir(tmp_path)
    doc = scan(remote.origin, source_provider=remote).document
    assert not list(tmp_path.iterdir())
    assert doc.unresolved
    assert doc.jobs
