"""Read-only local and GitHub source providers for the ETL parser.

Both providers implement :class:`SourceProvider` and return a
:class:`~etl_parser.scanner.repo.ScanIndex` of the files the parser should analyze.
:class:`GitHubSource` is the clone-free GitHub source described in
``docs/superpowers/specs/2026-09-17-ai-lineage-observability-github-design.md``: it
reads a repository at a pinned commit through the GitHub REST API (bounded, serial
requests with retry/backoff) rather than by creating a local git checkout, so no
working tree or ``.git`` directory is ever written to disk. :class:`LocalSource`
scans an existing local directory with :class:`~etl_parser.scanner.repo.RepoScanner`.
"""

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
    """Structural interface implemented by every scannable source (local or remote)."""

    def scan(self, *, extensions: set[str]) -> ScanIndex:
        """Scan the source and return its indexed files.

        Args:
            extensions: File extensions (including the leading dot) to include.

        Returns:
            ScanIndex: The scanned files, unresolved entries and source metadata.
        """
        ...


class LocalSource:
    """Source provider that scans an existing directory on the local filesystem."""

    def __init__(self, path):
        """Store the directory to scan.

        Args:
            path: Filesystem path to the local repository or directory to scan.
        """
        self.path = path

    def scan(self, *, extensions):
        """Scan ``self.path`` for files with the given extensions.

        Args:
            extensions: File extensions (including the leading dot) to include.

        Returns:
            ScanIndex: The result of :class:`~etl_parser.scanner.repo.RepoScanner`.
        """
        return RepoScanner(self.path, extensions=extensions).scan()


class _NoRedirect(HTTPRedirectHandler):
    """urllib redirect handler that refuses every redirect.

    Prevents the GitHub API token from being replayed against an arbitrary
    redirect target.
    """

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        """Refuse to follow any HTTP redirect.

        Args:
            req: The original request.
            fp: The response file object.
            code: The HTTP status code that triggered the redirect.
            msg: The HTTP status message.
            headers: The response headers.
            newurl: The redirect target URL.

        Returns:
            None: Always, so urllib treats the redirect as unfollowed.
        """
        return None  # Never send a repository token to an arbitrary redirect.


class GitHubSource:
    """Clone-free source provider that reads a GitHub repository via the REST API.

    Lists and reads files at a single pinned commit (the resolved ``ref``, or the
    default branch's ``HEAD``) using bounded, serial, retried HTTP requests, without
    ever creating a local git checkout. Optionally scopes the scan to a repository
    subdirectory or a single file, and can transparently expand a selected ``.zip``
    entry. Byte, file-count and total-size limits bound both memory and network use;
    symlinks, submodules and Git LFS pointers are rejected rather than followed.

    Attributes:
        repo: The ``OWNER/REPO`` path, percent-encoded per segment.
        origin: The canonical ``https://github.com/OWNER/REPO`` URL.
        ref: The requested branch, tag or commit SHA, or ``None`` for the default branch.
        path: Repository-relative path scoping the scan, or ``""`` for the whole repo.
        token: The GitHub API token used for authenticated requests, if any.
        timeout: Per-request timeout in seconds.
        retries: Number of retries after the first attempt for retryable failures.
        max_files: Maximum number of file/tree entries the scan will enumerate.
        max_file_bytes: Maximum size in bytes of any single file (or zip member).
        max_total_bytes: Maximum cumulative decoded bytes read across the scan.
        transport: Optional callable overriding HTTP requests, for testing.
    """

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
        """Validate and store the repository URL, scope and read limits.

        Args:
            url: Repository URL in the form ``https://github.com/OWNER/REPO``
                (no query, fragment, ref or subpath embedded in it).
            ref: Branch, tag or commit SHA to read; the default branch's ``HEAD``
                when ``None``.
            path: Repository-relative path scoping the scan to a subdirectory or
                single file; the whole repository when ``None``.
            token: GitHub API token. Falls back to ``GITHUB_TOKEN``, then ``GH_TOKEN``
                when ``GITHUB_TOKEN`` is unset or empty, then unauthenticated.
            timeout: Per-request timeout in seconds; must be positive.
            retries: Retries after the first attempt for retryable failures;
                must be nonnegative.
            max_files: Maximum number of file/tree entries to enumerate.
            max_file_bytes: Maximum size in bytes of any single file or zip member.
            max_total_bytes: Maximum cumulative decoded bytes read across the scan.
            transport: Optional callable ``endpoint -> dict`` replacing the real
                HTTP transport, for testing.

        Raises:
            ValueError: If ``url`` is not a bare ``https://github.com/OWNER/REPO``
                URL, if ``timeout``/``retries``/the byte or file limits are out of
                range, or if ``path`` is absolute, contains ``..`` or a backslash.
        """
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
        # An empty GITHUB_TOKEN is no token at all; fall through to GH_TOKEN as the
        # SDK guide documents, rather than letting the empty value shadow it.
        self.token = token if token is not None else (os.getenv("GITHUB_TOKEN") or None)
        if self.token is None:
            self.token = os.getenv("GH_TOKEN") or None
        self.timeout, self.retries = timeout, retries
        self.max_files, self.max_file_bytes = max_files, max_file_bytes
        self.max_total_bytes = max_total_bytes
        self.transport = transport
        self._opener = build_opener(_NoRedirect())
        self._total_bytes = 0

    def _request(self, endpoint):
        """Call one GitHub API endpoint, retrying retryable failures with backoff.

        Uses ``self.transport`` when set (test injection point); otherwise issues a
        real HTTPS request to ``https://api.github.com/repos/{self.repo}/{endpoint}``
        with the configured token and a non-redirecting opener. Rate-limited (429, or
        403 with rate-limit headers), 5xx, and transport-level failures are retried up
        to ``self.retries`` times with capped exponential backoff, honoring
        ``Retry-After``/``X-RateLimit-Reset`` when present.

        Args:
            endpoint: Path appended to the repository API base URL, e.g.
                ``"commits/HEAD"``.

        Returns:
            dict: The parsed JSON response body.

        Raises:
            ValueError: If the response body exceeds the configured transport byte
                limit, is not valid JSON, or the rate-limit response cannot be parsed.
            RuntimeError: If a non-retryable HTTP error occurs, retries are exhausted,
                the transport fails repeatedly, or the required backoff exceeds the
                30-second bound.
        """
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
        """Resolve ``self.ref`` to a commit and list every entry in its tree.

        Resolves the commit first, pinning the scan to that single SHA, then reads the
        recursive tree. Falls back to a manual breadth-first walk of individual
        subtrees when GitHub reports the recursive listing as truncated.

        Returns:
            tuple[str, list[dict]]: The resolved commit SHA, and the tree entries
            (each with at least ``path``, ``type``, ``sha`` and, for blobs, ``size``)
            sorted by path.

        Raises:
            ValueError: If the number of entries exceeds ``self.max_files``, subtree
                traversal exceeds that same limit, or a non-recursive subtree listing
                is itself truncated.
            KeyError: If a GitHub API response is missing an expected field.
            RuntimeError: Propagated from :meth:`_request` on API/transport failure.
        """
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
        """Fetch and decode one blob entry, enforcing per-file and total byte limits.

        Args:
            entry: A tree entry from :meth:`list_files`, with ``sha`` and optionally
                ``size``.

        Returns:
            bytes: The decoded file content.

        Raises:
            ValueError: If the reported or decoded size exceeds ``self.max_file_bytes``
                or the running total exceeds ``self.max_total_bytes``, if the blob
                encoding is not base64, if the content is not valid base64, or if the
                content is a Git LFS pointer.
            RuntimeError: Propagated from :meth:`_request` on API/transport failure.
        """
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

    def _unfetched(self, remaining, scope, extensions):
        """Count the entries a stopped scan would still have read.

        Args:
            remaining: Tree entries after the one the scan stopped on.
            scope: Repository-relative prefix the scan is limited to, or ``""``.
            extensions: File extensions (including the leading dot) being collected.

        Returns:
            int: How many remaining blobs are in scope, outside excluded directories,
            and carry a collected extension, so the diagnostic can say what was missed.
        """
        excluded = {".git", ".venv", "venv", "node_modules", "__pycache__", "build", "dist"}
        count = 0
        for entry in remaining:
            path = entry["path"]
            if scope and not path.startswith(scope + "/"):
                continue
            relative = path[len(scope) + 1 :] if scope else path
            parts = PurePosixPath(relative).parts
            if entry["type"] != "blob" or any(p in excluded for p in parts):
                continue
            if PurePosixPath(relative).suffix in extensions | {".zip"}:
                count += 1
        return count

    def scan(self, *, extensions):
        """Scan the configured repository scope and build a :class:`ScanIndex`.

        Lists the repository tree once via :meth:`list_files`, narrows it to
        ``self.path`` (a single file, a ``.zip`` archive to expand, a subdirectory, or
        the whole repository), skips VCS/build/dependency directories, symlinks and
        submodules, and reads each remaining file matching ``extensions`` (or member
        of a matched ``.zip``) through :meth:`read_file`. Symlinks and submodules become
        non-gating ``skipped_entry`` items: there is no file behind them to read, so they
        are not a parser limitation. Per-entry read/decode failures are recorded as
        :class:`~etl_parser.models.Unresolved` entries rather than aborting the scan. The
        scan stops early when the total byte budget is exceeded, and after three
        consecutive request failures, which adds one ``analysis_note`` saying how many
        files were left unfetched.

        Args:
            extensions: File extensions (including the leading dot) to include.

        Returns:
            ScanIndex: The scanned files, unresolved entries and source metadata, via
            :meth:`~etl_parser.scanner.repo.ScanIndex.build_modules`.

        Raises:
            ValueError: If ``self.path`` does not match any entry at the resolved
                revision.
            RuntimeError: Propagated from :meth:`list_files`/:meth:`_request` on
                API/transport failure.
        """
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
        consecutive_failures = 0
        for position, entry in enumerate(entries):
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
                    # Not a parser limitation: there is no file here to read. Recorded so
                    # the entry is visible, but never as a coverage failure.
                    index.unresolved.append(
                        Unresolved(
                            kind="skipped_entry",
                            source_file=relative,
                            reason="Symlink or submodule is not followed",
                            remediation="Scan the target repository or path directly.",
                        )
                    )
                    if observer:
                        observer.count("source.skipped_entries")
                        observer.event(
                            "source.skipped",
                            level="DEBUG",
                            source=relative,
                            reason="symlink_or_submodule",
                        )
                    continue
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
                consecutive_failures = 0
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
                # A request failure (rate limit, outage) tends to repeat. Stop asking
                # after three in a row instead of spending the rest of the inventory
                # on requests that will fail the same way.
                consecutive_failures += 1 if isinstance(exc, RuntimeError) else 0
                if consecutive_failures >= 3:
                    unfetched = self._unfetched(entries[position + 1 :], scope, extensions)
                    index.unresolved.append(
                        Unresolved(
                            kind="analysis_note",
                            reason=f"GitHub read stopped after {consecutive_failures} "
                            f"consecutive request failures; {unfetched} files were not "
                            "fetched at this revision.",
                            remediation="Re-run the scan once the GitHub API is available.",
                        )
                    )
                    if observer:
                        observer.count("source.request_breaker_opened")
                        observer.event(
                            "source.scan_stopped",
                            level="WARNING",
                            reason="consecutive_request_failures",
                            consecutive_failures=consecutive_failures,
                            unfetched_files=unfetched,
                        )
                    break
        if self.path and not found_scope:
            raise ValueError("Requested GitHub path does not exist at the selected revision")
        return index.build_modules()
