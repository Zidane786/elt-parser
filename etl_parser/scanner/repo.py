"""Bounded repository and ZIP source indexing. No extracted module is executed.

This is the first stage of the pipeline described in design spec section 4: it walks a
repository path (or a ``.zip`` archive) and builds the ``ModuleGraph`` that ``RepoScanner``
hands to the downstream workers. Its entry points are :class:`RepoScanner`, whose
:meth:`RepoScanner.scan` method walks the source tree (or opens ``.zip`` members) under
configurable size/count limits and returns a :class:`ScanIndex`, and
:func:`imports_airflow`, used by the scanner to route ``.py`` files that import Airflow to
the AirflowWorker (section 8.4) instead of PythonWorker (section 8.2). ``ScanIndex`` builds
the dotted-module map used by :mod:`etl_parser.scanner.imports` to resolve Python imports to
files, per the "RepoScanner ... ModuleGraph (imports resolved to files)" step of section 4.
"""

from __future__ import annotations

import ast
import stat
import zipfile
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath

from etl_parser.models import Unresolved
from etl_parser.observability import current_observer


@dataclass(frozen=True)
class SourceFile:
    """A single scanned source file, from disk or from inside a ``.zip`` archive.

    Attributes:
        path (str): Repository-relative path. Archive members use ``"archive.zip!/member"``.
        text (str): The file's decoded UTF-8 text content.
        suffix (str): File extension, including the leading dot (for example ``".py"``).
        archive (str | None): Repository-relative path to the containing ``.zip`` file, or
            ``None`` when the source came directly from the filesystem.
    """

    path: str
    text: str
    suffix: str
    archive: str | None = None

    @property
    def job_id(self) -> str:
        """Return the job identifier derived from this file's path.

        Returns:
            str: The path with its final extension removed, used as the default ``Job.id``
            for the worker that analyzes this file.
        """
        return self.path.rsplit(".", 1)[0]


@dataclass
class ScanIndex:
    """The scanned repository: every source file plus the dotted-module lookup over them.

    ``RepoScanner.scan`` builds and returns one ``ScanIndex`` per scan. It is the
    "ModuleGraph" of design spec section 4: downstream workers use it to resolve Python
    imports and script paths to concrete files instead of guessing from strings.

    Attributes:
        root (Path): Directory the scan was rooted at (the archive's parent for a ``.zip``
            scan).
        files (list[SourceFile]): Every scanned source file, sorted by path after
            :meth:`build_modules` runs.
        module_map (dict[str, list[SourceFile]]): Maps each dotted module name suffix (for
            example both ``"pkg.mod"`` and ``"mod"``) to the files whose module path ends
            with it, built by :meth:`build_modules`.
        unresolved (list[Unresolved]): Files or archive members that could not be read or
            safely extracted, recorded with a reason instead of being silently dropped.
        entry_paths (set[str] | None): When the scan target was a single file (not a
            directory or archive), the set containing just that file's name; otherwise
            ``None``.
        archive_entries (bool): Whether the scanned path was itself a ``.zip`` file.
        origin (str | None): String form of the path passed to :class:`RepoScanner`.
        revision (str | None): Optional source control revision the scan corresponds to.
    """

    root: Path
    files: list[SourceFile] = field(default_factory=list)
    module_map: dict[str, list[SourceFile]] = field(default_factory=dict)
    unresolved: list[Unresolved] = field(default_factory=list)
    entry_paths: set[str] | None = None
    archive_entries: bool = False
    origin: str | None = None
    revision: str | None = None

    def build_modules(self):
        """Sort scanned files and (re)build the dotted-module lookup map.

        For every ``.py`` file, derives its dotted module name (dropping a trailing
        ``.__init__``) and indexes it under every dotted suffix of that name (for example
        ``pkg.sub.mod`` is indexed under ``"pkg.sub.mod"``, ``"sub.mod"``, and ``"mod"``), so
        :meth:`resolve_module` can match both absolute and shortened import references.

        Returns:
            ScanIndex: This instance, for chaining after :meth:`RepoScanner.scan` populates
            ``files``.
        """
        self.files.sort(key=lambda source: source.path)
        self.module_map.clear()
        for source in self.files:
            if source.suffix != ".py":
                continue
            name = source.path.split("!/")[-1].removesuffix(".py").replace("/", ".")
            name = name.removesuffix(".__init__")
            parts = name.split(".")
            for i in range(len(parts)):
                self.module_map.setdefault(".".join(parts[i:]), []).append(source)
        return self

    def resolve_module(self, name: str, caller: SourceFile, level: int = 0) -> SourceFile | None:
        """Resolve a dotted module name to the unique file it refers to, if unambiguous.

        Used by :func:`etl_parser.scanner.imports.resolve_import` so ``PythonWorker`` and
        orchestration workers turn ``import``/``from`` statements into concrete source files.

        Args:
            name (str): Dotted module name as written in the import statement (relative
                imports pass the part after the dots).
            caller (SourceFile): The file containing the import, used to compute the
                caller's own module path for relative imports and as the local-directory tie
                breaker below.
            level (int): Relative import level; ``0`` means an absolute import. When
                nonzero, ``name`` is resolved against the caller's module path with the last
                ``level`` components stripped.

        Returns:
            SourceFile | None: The single matching file. When multiple files share the
            dotted name, falls back to the unique file in the same archive and directory as
            ``caller``; returns ``None`` when the name is unresolved or still ambiguous.
        """
        observer = current_observer()
        if observer:
            observer.count("imports.resolution_attempts")
            observer.event(
                "import.resolve",
                level="DEBUG",
                actor="source_index",
                module=name,
                source=caller.path,
                relative_level=level,
            )
        candidates = self.module_map.get(name, [])
        if level:
            caller_name = caller.path.split("!/")[-1].removesuffix(".py").replace("/", ".")
            parts = caller_name.split(".")[:-level]
            candidates = self.module_map.get(".".join([*parts, name]).strip("."), [])
        if len(candidates) == 1:
            if observer:
                observer.count("imports.resolved")
                observer.event(
                    "import.resolved",
                    level="DEBUG",
                    source=caller.path,
                    module=name,
                    target=candidates[0].path,
                )
            return candidates[0]
        local = [
            s
            for s in candidates
            if s.archive == caller.archive
            and s.path.rsplit("/", 1)[0] == caller.path.rsplit("/", 1)[0]
        ]
        found = local[0] if len(local) == 1 else None
        if observer:
            observer.count("imports.resolved" if found else "imports.unresolved")
            observer.event(
                "import.resolved" if found else "import.unresolved",
                level="DEBUG",
                source=caller.path,
                module=name,
                target=found.path if found else None,
                candidate_count=len(candidates),
            )
        return found

    def script(self, value: str) -> SourceFile | None:
        """Resolve a script path (for example from an Airflow ``BashOperator``) to a file.

        Tries an exact path match first, then progressively strips leading path segments so
        a script path recorded under a different deployment prefix (a relocated fixture or
        a build output directory) still resolves, as long as exactly one scanned file ends
        with the remaining tail.

        Args:
            value (str): The script path as written in code, possibly using backslashes or
                a leading ``"./"``.

        Returns:
            SourceFile | None: The uniquely matching file, or ``None`` when no file matches
            or more than one file matches at the same stripped-prefix level.
        """
        clean = value.replace("\\", "/").removeprefix("./")
        exact = [s for s in self.files if s.path == clean]
        if len(exact) == 1:
            return exact[0]
        # Relocated fixture/deployment prefixes are accepted only when unambiguous.
        parts = clean.split("/")
        for offset in range(1, len(parts)):
            tail = "/".join(parts[offset:])
            matches = [s for s in self.files if s.path == tail or s.path.endswith("/" + tail)]
            if len(matches) == 1:
                return matches[0]
            if len(matches) > 1:
                return None
        return None


class RepoScanner:
    """Walk a repository directory or ``.zip`` archive and index its source files.

    This is the pipeline's entry point (design spec section 4): it reads files from disk or
    from archive members under configurable size and count limits, skips ignored/vendor
    directories and symlinks, and never imports or executes any scanned code.

    Attributes:
        path (Path): Resolved absolute path to the scan target (a directory, a single file,
            or a ``.zip`` archive).
        max_file_bytes (int): Per-file size limit; larger files are recorded as unresolved
            instead of read.
        max_archive_bytes (int): Total uncompressed size limit for a ``.zip`` archive.
        max_archive_members (int): Maximum number of entries a ``.zip`` archive may contain.
        extensions (set[str]): File extensions eligible for scanning.
    """

    def __init__(
        self,
        root: Path | str,
        *,
        max_file_bytes=5_000_000,
        max_archive_bytes=50_000_000,
        max_archive_members=2000,
        extensions: set[str] | None = None,
    ):
        """Configure a scanner for a repository path or archive.

        Args:
            root (Path | str): Directory, single file, or ``.zip`` archive to scan.
            max_file_bytes (int): Per-file size limit in bytes; files over this size are
                recorded as unresolved rather than read. Defaults to 5,000,000.
            max_archive_bytes (int): Total uncompressed size limit in bytes for a ``.zip``
                archive. Defaults to 50,000,000.
            max_archive_members (int): Maximum number of entries a ``.zip`` archive may
                contain. Defaults to 2000.
            extensions (set[str] | None): File extensions to scan. Defaults to
                ``{".py", ".sql", ".yaml", ".yml"}`` when not given.
        """
        self.path = Path(root).resolve()
        self.max_file_bytes = max_file_bytes
        self.max_archive_bytes = max_archive_bytes
        self.max_archive_members = max_archive_members
        self.extensions = extensions or {".py", ".sql", ".yaml", ".yml"}

    def scan(self) -> ScanIndex:
        """Walk the configured path and build a :class:`ScanIndex` of its source files.

        Directories are walked recursively, skipping ``.git``, ``.venv``, ``venv``,
        ``node_modules``, ``__pycache__``, ``build``, ``dist``, and symlinks. ``.zip``
        archives found while walking (or given directly as the scan target) are indexed via
        :meth:`_zip`. Files whose extension is not in ``self.extensions`` are skipped. Read
        or size-limit failures are recorded as ``Unresolved`` items rather than raised,
        except for the initial existence check.

        Returns:
            ScanIndex: The populated index, with :meth:`ScanIndex.build_modules` already
            applied.

        Raises:
            FileNotFoundError: If the configured scan path does not exist.
        """
        observer = current_observer()
        root = self.path if self.path.is_dir() else self.path.parent
        index = ScanIndex(root)
        if not self.path.exists():
            raise FileNotFoundError(self.path)
        index.origin = str(self.path)
        index.archive_entries = self.path.suffix == ".zip"
        if self.path.is_file() and not index.archive_entries:
            index.entry_paths = {self.path.name}
        ignored = {".git", ".venv", "venv", "node_modules", "__pycache__", "build", "dist"}

        def paths(directory):
            """Yield scannable file paths under ``directory``, recursing into subdirectories.

            Skips entries in the ``ignored`` set and symlinks, counting excluded entries on
            the current observer when one is active.

            Args:
                directory (Path): Directory to walk.

            Yields:
                Path: Each non-ignored file found under ``directory``, in sorted order.
            """
            for path in sorted(directory.iterdir()):
                if observer:
                    observer.count("source.entries_listed")
                if path.name in ignored or path.is_symlink():
                    if observer:
                        observer.count("source.excluded_entries")
                        observer.event(
                            "source.skipped",
                            level="DEBUG",
                            source=str(path),
                            reason="excluded_directory_or_symlink",
                        )
                    continue
                if path.is_dir():
                    yield from paths(path)
                else:
                    yield path

        for path in paths(root) if self.path.suffix != ".zip" else [self.path]:
            relative = path.relative_to(root).as_posix()
            try:
                if path.suffix == ".zip":
                    if path.stat().st_size > self.max_archive_bytes:
                        raise ValueError("ZIP archive exceeds configured size limit")
                    if observer:
                        observer.count("archives.opened")
                    self._zip(path, relative, index)
                elif path.suffix in self.extensions:
                    if path.stat().st_size > self.max_file_bytes:
                        raise ValueError("source exceeds configured file size limit")
                    index.files.append(
                        SourceFile(relative, path.read_text(encoding="utf-8"), path.suffix)
                    )
                    if observer:
                        observer.count("source.files_read")
                        observer.event(
                            "source.read",
                            level="DEBUG",
                            source=relative,
                            size_bytes=path.stat().st_size,
                        )
                elif observer:
                    observer.count("source.unsupported_files")
                    observer.event(
                        "source.skipped",
                        level="DEBUG",
                        source=relative,
                        reason="unsupported_extension",
                    )
            except (OSError, UnicodeError, ValueError, zipfile.BadZipFile, RuntimeError) as exc:
                if observer:
                    observer.count("source.read_failures")
                    observer.event(
                        "source.failed",
                        level="WARNING",
                        source=relative,
                        error_type=type(exc).__name__,
                    )
                index.unresolved.append(
                    Unresolved(
                        kind="unsupported_syntax",
                        source_file=relative,
                        reason=f"source read: {exc}",
                    )
                )
        return index.build_modules()

    def _zip(self, path, relative, index):
        """Read scannable members from a ``.zip`` archive into ``index``.

        Rejects members with absolute paths, ``..`` segments, backslashes, colons, symlink
        members, and duplicate names as unsafe or ambiguous, recording each as an
        ``Unresolved`` item and skipping it. Non-matching extensions and directory entries
        are skipped silently (counted on the observer).

        Args:
            path (Path): Path to the ``.zip`` file to read.
            relative (str): ``path`` expressed relative to the scan root, used as the
                archive prefix for member ``SourceFile.path`` values.
            index (ScanIndex): Index to append resulting :class:`SourceFile` and
                :class:`~etl_parser.models.Unresolved` entries to.

        Raises:
            ValueError: If the archive has more members than ``max_archive_members``, more
                total uncompressed bytes than ``max_archive_bytes``, or a single member
                larger than ``max_file_bytes``.
        """
        observer = current_observer()
        with zipfile.ZipFile(path) as archive:
            members = archive.infolist()
            if observer:
                observer.count("archives.members_listed", len(members))
            if len(members) > self.max_archive_members:
                raise ValueError("ZIP exceeds configured member count limit")
            if sum(m.file_size for m in members) > self.max_archive_bytes:
                raise ValueError("ZIP exceeds configured uncompressed size limit")
            names: set[str] = set()
            total = 0
            for member in members:
                name = PurePosixPath(member.filename)
                invalid = (
                    name.is_absolute()
                    or ".." in name.parts
                    or "\\" in member.filename
                    or ":" in member.filename
                    or stat.S_ISLNK(member.external_attr >> 16)
                    or member.filename in names
                )
                names.add(member.filename)
                if invalid:
                    if observer:
                        observer.count("archives.members_rejected")
                        observer.event(
                            "archive.member_rejected",
                            level="WARNING",
                            source=f"{relative}!/{member.filename}",
                            reason="unsafe_or_ambiguous_name",
                        )
                    index.unresolved.append(
                        Unresolved(
                            kind="unresolved_import",
                            source_file=f"{relative}!/{member.filename}",
                            reason="Unsafe or ambiguous ZIP member name; member skipped",
                        )
                    )
                    continue
                if member.is_dir() or name.suffix not in self.extensions:
                    if observer:
                        observer.count("archives.members_skipped")
                    continue
                member_path = f"{relative}!/{member.filename}"
                try:
                    if member.file_size > self.max_file_bytes:
                        raise ValueError("ZIP member exceeds file size limit")
                    # The header's size is a claim, not a fact: read one byte past the
                    # limit and judge by what actually arrives (review finding 36).
                    with archive.open(member) as handle:
                        raw = handle.read(self.max_file_bytes + 1)
                    if len(raw) > self.max_file_bytes:
                        raise ValueError("ZIP member exceeds file size limit when decompressed")
                    total += len(raw)
                    if total > self.max_archive_bytes:
                        raise ValueError("ZIP exceeds configured uncompressed size limit")
                    text = raw.decode("utf-8")
                except (OSError, UnicodeError, ValueError, zipfile.BadZipFile, RuntimeError) as exc:
                    # One unreadable member is not a reason to drop the others.
                    if observer:
                        observer.count("archives.members_rejected")
                        observer.event(
                            "archive.member_rejected",
                            level="WARNING",
                            source=member_path,
                            reason=type(exc).__name__,
                        )
                    index.unresolved.append(
                        Unresolved(
                            kind="unsupported_syntax",
                            source_file=member_path,
                            reason=f"archive member read: {exc}",
                        )
                    )
                    continue
                index.files.append(SourceFile(member_path, text, name.suffix, relative))
                if observer:
                    observer.count("archives.members_read")
                    observer.event(
                        "archive.member_read",
                        level="DEBUG",
                        source=member_path,
                        size_bytes=len(raw),
                    )


def imports_package(source: SourceFile, *packages: str) -> bool:
    """Detect whether a Python source file imports any of ``packages``.

    Import statements are the only reliable statement of what a file runs on: a package
    name in a comment or a string is not a dependency (review finding 33). The file is
    parsed, never imported or executed.

    Args:
        source (SourceFile): The Python file to inspect.
        *packages (str): Top-level package names to look for, for example ``"pyspark"``.

    Returns:
        bool: ``True`` if any top-level or nested ``import <pkg>...`` or
        ``from <pkg>... import ...`` statement is present; ``False`` if none is found or
        the file fails to parse as Python.
    """
    try:
        tree = ast.parse(source.text)
    except SyntaxError:
        return False
    roots = tuple(packages)
    return any(
        (isinstance(n, ast.ImportFrom) and (n.module or "").split(".")[0] in roots)
        or (isinstance(n, ast.Import) and any(a.name.split(".")[0] in roots for a in n.names))
        for n in ast.walk(tree)
    )


def imports_airflow(source: SourceFile) -> bool:
    """Detect whether a Python source file imports the ``airflow`` package.

    Used to route ``.py`` files to the AirflowWorker (design spec section 8.4) instead of
    PythonWorker (section 8.2); the DAG file is only ever parsed statically, never imported.

    Args:
        source (SourceFile): The Python file to inspect.

    Returns:
        bool: ``True`` if the file imports ``airflow``; ``False`` if it does not or the
        file fails to parse as Python.
    """
    return imports_package(source, "airflow")
