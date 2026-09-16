"""Bounded repository and ZIP source indexing. No extracted module is executed."""

from __future__ import annotations

import ast
import stat
import zipfile
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath

from etl_parser.models import Unresolved


@dataclass(frozen=True)
class SourceFile:
    path: str
    text: str
    suffix: str
    archive: str | None = None

    @property
    def job_id(self) -> str:
        return self.path.rsplit(".", 1)[0]


@dataclass
class ScanIndex:
    root: Path
    files: list[SourceFile] = field(default_factory=list)
    module_map: dict[str, list[SourceFile]] = field(default_factory=dict)
    unresolved: list[Unresolved] = field(default_factory=list)

    def resolve_module(self, name: str, caller: SourceFile, level: int = 0) -> SourceFile | None:
        candidates = self.module_map.get(name, [])
        if level:
            caller_name = caller.path.split("!/")[-1].removesuffix(".py").replace("/", ".")
            parts = caller_name.split(".")[:-level]
            candidates = self.module_map.get(".".join([*parts, name]).strip("."), [])
        if len(candidates) == 1:
            return candidates[0]
        local = [
            s
            for s in candidates
            if s.archive == caller.archive
            and s.path.rsplit("/", 1)[0] == caller.path.rsplit("/", 1)[0]
        ]
        return local[0] if len(local) == 1 else None

    def script(self, value: str) -> SourceFile | None:
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
    def __init__(
        self,
        root: Path | str,
        *,
        max_file_bytes=5_000_000,
        max_archive_bytes=50_000_000,
        max_archive_members=2000,
        extensions: set[str] | None = None,
    ):
        self.path = Path(root).resolve()
        self.max_file_bytes = max_file_bytes
        self.max_archive_bytes = max_archive_bytes
        self.max_archive_members = max_archive_members
        self.extensions = extensions or {".py", ".sql", ".yaml", ".yml"}

    def scan(self) -> ScanIndex:
        root = self.path if self.path.is_dir() else self.path.parent
        index = ScanIndex(root)
        if not self.path.exists():
            raise FileNotFoundError(self.path)
        ignored = {".git", ".venv", "venv", "node_modules", "__pycache__", "build", "dist"}

        def paths(directory):
            for path in sorted(directory.iterdir()):
                if path.name in ignored or path.is_symlink():
                    continue
                if path.is_dir():
                    yield from paths(path)
                else:
                    yield path

        for path in paths(root) if self.path.suffix != ".zip" else [self.path]:
            relative = path.relative_to(root).as_posix()
            try:
                if path.suffix == ".zip":
                    self._zip(path, relative, index)
                elif path.suffix in self.extensions:
                    if path.stat().st_size > self.max_file_bytes:
                        raise ValueError("source exceeds configured file size limit")
                    index.files.append(
                        SourceFile(relative, path.read_text(encoding="utf-8"), path.suffix)
                    )
            except (OSError, UnicodeError, ValueError, zipfile.BadZipFile, RuntimeError) as exc:
                index.unresolved.append(
                    Unresolved(
                        kind="unsupported_syntax",
                        source_file=relative,
                        reason=f"source read: {exc}",
                    )
                )
        index.files.sort(key=lambda source: source.path)
        for source in index.files:
            if source.suffix != ".py":
                continue
            name = source.path.split("!/")[-1].removesuffix(".py").replace("/", ".")
            name = name.removesuffix(".__init__")
            parts = name.split(".")
            for i in range(len(parts)):
                index.module_map.setdefault(".".join(parts[i:]), []).append(source)
        return index

    def _zip(self, path, relative, index):
        with zipfile.ZipFile(path) as archive:
            members = archive.infolist()
            if len(members) > self.max_archive_members:
                raise ValueError("ZIP exceeds configured member count limit")
            if sum(m.file_size for m in members) > self.max_archive_bytes:
                raise ValueError("ZIP exceeds configured uncompressed size limit")
            names: set[str] = set()
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
                    index.unresolved.append(
                        Unresolved(
                            kind="unresolved_import",
                            source_file=f"{relative}!/{member.filename}",
                            reason="Unsafe or ambiguous ZIP member name; member skipped",
                        )
                    )
                    continue
                if member.is_dir() or name.suffix not in self.extensions:
                    continue
                if member.file_size > self.max_file_bytes:
                    raise ValueError(f"ZIP member exceeds file size limit: {member.filename}")
                text = archive.read(member).decode("utf-8")
                index.files.append(
                    SourceFile(f"{relative}!/{member.filename}", text, name.suffix, relative)
                )


def imports_airflow(source: SourceFile) -> bool:
    try:
        tree = ast.parse(source.text)
    except SyntaxError:
        return False
    return any(
        (isinstance(n, ast.ImportFrom) and (n.module or "").startswith("airflow"))
        or (isinstance(n, ast.Import) and any(a.name.startswith("airflow") for a in n.names))
        for n in ast.walk(tree)
    )
