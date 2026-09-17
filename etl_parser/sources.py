"""Read-only local/GitHub source providers. GitHub reads never create a checkout."""

from __future__ import annotations

import base64
import io
import json
import os
import time
import zipfile
from pathlib import Path, PurePosixPath
from typing import Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from etl_parser.models import Unresolved
from etl_parser.observability import current_observer
from etl_parser.scanner.repo import RepoScanner, ScanIndex, SourceFile


class SourceProvider(Protocol):
    def scan(self, *, extensions: set[str]) -> ScanIndex: ...


class LocalSource:
    def __init__(self, path):
        self.path = path

    def scan(self, *, extensions):
        return RepoScanner(self.path, extensions=extensions).scan()


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None  # Never send a repository token to an arbitrary redirect.


class GitHubSource:
    def __init__(
        self,
        url,
        *,
        ref=None,
        path=None,
        token=None,
        timeout=30,
        retries=2,
        max_files=20_000,
        max_file_bytes=5_000_000,
        max_total_bytes=100_000_000,
        transport=None,
    ):
        parsed = urlsplit(url)
        parts = parsed.path.strip("/").split("/")
        if (
            parsed.scheme != "https"
            or parsed.netloc != "github.com"
            or len(parts) != 2
            or parsed.query
            or parsed.fragment
            or any(p in {"", ".", ".."} for p in parts)
        ):
            raise ValueError("Use https://github.com/OWNER/REPO and separate ref/path options")
        if timeout <= 0 or retries < 0 or min(max_files, max_file_bytes, max_total_bytes) < 1:
            raise ValueError("Source limits/timeouts must be positive and retries nonnegative")
        selected = PurePosixPath(path or ".")
        if selected.is_absolute() or ".." in selected.parts or "\\" in str(selected):
            raise ValueError("GitHub source path must be a repository-relative path")
        self.repo = "/".join(quote(p, safe="") for p in (parts[0], parts[1].removesuffix(".git")))
        self.origin = "https://github.com/" + self.repo
        self.ref = ref
        self.path = "" if str(selected) == "." else str(selected)
        self.token = (
            token if token is not None else os.getenv("GITHUB_TOKEN", os.getenv("GH_TOKEN"))
        )
        self.timeout, self.retries = timeout, retries
        self.max_files, self.max_file_bytes = max_files, max_file_bytes
        self.max_total_bytes = max_total_bytes
        self.transport = transport
        self._opener = build_opener(_NoRedirect())
        self._total_bytes = 0

    def _request(self, endpoint):
        observer = current_observer()
        if self.transport is not None:
            if observer:
                observer.count("github.requests")
            return self.transport(endpoint)
        url = f"https://api.github.com/repos/{self.repo}/{endpoint}"
        headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "etl-parser",
            "X-GitHub-Api-Version": "2026-03-10",
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        for attempt in range(self.retries + 1):
            started = time.perf_counter()
            if observer:
                observer.count("github.requests")
                observer.event(
                    "github.request",
                    level="DEBUG",
                    endpoint=endpoint,
                    attempt=attempt + 1,
                    authenticated=bool(self.token),
                )
            try:
                with self._opener.open(
                    Request(url, headers=headers), timeout=self.timeout
                ) as response:
                    # A JSON blob is base64; bound transport overhead as well as decoded content.
                    limit = max(8_000_000, self.max_file_bytes * 2 + 4096)
                    body = response.read(limit + 1)
                    if len(body) > limit:
                        raise ValueError("GitHub response exceeds configured transport limit")
                    if observer:
                        observer.count("github.response_bytes", len(body))
                    return json.loads(body)
            except HTTPError as exc:
                retryable = exc.code in {429, 500, 502, 503, 504} or (
                    exc.code == 403
                    and (
                        exc.headers.get("Retry-After")
                        or exc.headers.get("X-RateLimit-Remaining") == "0"
                    )
                )
                if not retryable or attempt == self.retries:
                    raise RuntimeError(f"GitHub read failed with HTTP {exc.code}") from None
                delay = min(2**attempt, 8)
                wait = exc.headers.get("Retry-After")
                reset = exc.headers.get("X-RateLimit-Reset")
                try:
                    if wait:
                        delay = max(delay, float(wait))
                    elif reset:
                        delay = max(delay, float(reset) - time.time())
                except ValueError:
                    raise RuntimeError("Invalid GitHub rate-limit response") from None
                if delay > 30:
                    raise RuntimeError(
                        "GitHub rate limit requires a later retry; scan incomplete"
                    ) from None
            except (URLError, TimeoutError):
                if attempt == self.retries:
                    raise RuntimeError("GitHub transport failed; check connectivity") from None
                delay = min(2**attempt, 8)
            finally:
                if observer:
                    observer.duration("github.request", (time.perf_counter() - started) * 1000)
            if observer:
                observer.count("github.retries")
                observer.count("github.retry_wait_ms", delay * 1000)
                observer.event("github.retry", level="WARNING", delay_seconds=delay)
            time.sleep(delay)
        raise RuntimeError("GitHub retry budget exhausted")

    def list_files(self):
        commit = self._request("commits/" + quote(self.ref or "HEAD", safe=""))
        revision = commit["sha"]
        tree_sha = commit["commit"]["tree"]["sha"]
        tree = self._request(f"git/trees/{tree_sha}?recursive=1")
        if not tree.get("truncated"):
            entries = tree["tree"]
        else:
            entries = []
            pending = [("", tree_sha)]
            traversed = 0
            while pending:
                prefix, sha = pending.pop()
                traversed += 1
                if traversed > self.max_files:
                    raise ValueError("GitHub subtree traversal limit exceeded")
                branch = self._request(f"git/trees/{sha}")
                if branch.get("truncated"):
                    raise ValueError("GitHub nonrecursive tree is incomplete")
                for entry in branch["tree"]:
                    item = {**entry, "path": prefix + entry["path"]}
                    if item["type"] == "tree":
                        pending.append((item["path"] + "/", item["sha"]))
                    else:
                        entries.append(item)
                    if len(entries) > self.max_files:
                        raise ValueError("GitHub file inventory limit exceeded")
        if len(entries) > self.max_files:
            raise ValueError("GitHub file inventory limit exceeded")
        return revision, sorted(entries, key=lambda item: item["path"])

    def read_file(self, entry):
        if entry.get("size", 0) > self.max_file_bytes:
            raise ValueError("GitHub file exceeds configured size limit")
        data = self._request(f"git/blobs/{entry['sha']}")
        if data.get("encoding") != "base64":
            raise ValueError("Unsupported GitHub blob encoding")
        content = base64.b64decode("".join(data["content"].split()), validate=True)
        self._total_bytes += len(content)
        if len(content) > self.max_file_bytes or self._total_bytes > self.max_total_bytes:
            raise ValueError("GitHub source byte limit exceeded")
        if content.startswith(b"version https://git-lfs.github.com/spec/"):
            raise ValueError("Git LFS pointer: external object not fetched")
        return content

    def scan(self, *, extensions):
        self._total_bytes = 0
        revision, entries = self.list_files()
        observer = current_observer()
        index = ScanIndex(Path("."), origin=self.origin, revision=revision)
        matching = [e for e in entries if e["path"] == self.path]
        selected_file = bool(matching and matching[0]["type"] != "tree")
        scope = str(PurePosixPath(self.path).parent) if selected_file else self.path
        scope = "" if scope == "." else scope
        selected = self.path.removeprefix(scope + "/") if scope else self.path
        index.archive_entries = selected_file and selected.endswith(".zip")
        if selected_file and not index.archive_entries:
            index.entry_paths = {selected}
        zip_reader = RepoScanner(".", extensions=extensions, max_file_bytes=self.max_file_bytes)
        seen = set()
        found_scope = False
        for entry in entries:
            path = entry["path"]
            if scope and not path.startswith(scope + "/"):
                continue
            found_scope = True
            relative = path[len(scope) + 1 :] if scope else path
            if index.archive_entries and relative != selected:
                continue
            if observer:
                observer.count("source.entries_listed")
            if entry["type"] == "tree":
                continue
            if any(
                p in {".git", ".venv", "venv", "node_modules", "__pycache__", "build", "dist"}
                for p in PurePosixPath(relative).parts
            ):
                if observer:
                    observer.count("source.excluded_entries")
                continue
            try:
                if (
                    PurePosixPath(relative).is_absolute()
                    or ".." in PurePosixPath(relative).parts
                    or "\\" in relative
                    or relative in seen
                ):
                    raise ValueError("Unsafe or duplicate repository path")
                seen.add(relative)
                if entry["type"] != "blob" or entry.get("mode") in {"120000", "160000"}:
                    raise ValueError("Symlink/submodule is not followed")
                suffix = PurePosixPath(relative).suffix
                if suffix not in extensions | {".zip"}:
                    if observer:
                        observer.count("source.unsupported_files")
                        observer.event(
                            "source.skipped",
                            level="DEBUG",
                            source=relative,
                            reason="unsupported_extension",
                        )
                    continue
                content = self.read_file(entry)
                if observer:
                    observer.count("source.files_read")
                    observer.event(
                        "source.read",
                        level="DEBUG",
                        source=relative,
                        blob_sha=entry["sha"],
                        size_bytes=len(content),
                        revision=revision,
                    )
                if suffix == ".zip":
                    before = len(index.files)
                    zip_reader._zip(io.BytesIO(content), relative, index)
                    self._total_bytes += sum(
                        len(f.text.encode("utf-8")) for f in index.files[before:]
                    )
                    if self._total_bytes > self.max_total_bytes:
                        del index.files[before:]
                        raise ValueError("GitHub expanded-source byte limit exceeded")
                else:
                    if b"\x00" in content:
                        raise ValueError("Binary source cannot be parsed as text")
                    index.files.append(SourceFile(relative, content.decode("utf-8"), suffix))
            except (
                ValueError,
                KeyError,
                RuntimeError,
                UnicodeError,
                OSError,
                zipfile.BadZipFile,
            ) as exc:
                index.unresolved.append(
                    Unresolved(
                        kind="unsupported_syntax",
                        source_file=relative,
                        reason=f"GitHub source read: {type(exc).__name__}: {exc}",
                    )
                )
                if observer:
                    observer.count("source.read_failures")
                if self._total_bytes > self.max_total_bytes:
                    break
        if self.path and not found_scope:
            raise ValueError("Requested GitHub path does not exist at the selected revision")
        return index.build_modules()
