"""Import resolution shared by Python and orchestration plugins.

Thin wrapper around :meth:`etl_parser.scanner.repo.ScanIndex.resolve_module` so
``PythonWorker`` (section 8.2 of the design spec) and the Airflow/orchestration workers
resolve absolute and relative imports to a concrete source file through one entry point,
without depending on :class:`~etl_parser.scanner.repo.ScanIndex` internals directly.
"""

from etl_parser.scanner.repo import ScanIndex, SourceFile


def resolve_import(
    module: str, level: int, from_file: SourceFile, index: ScanIndex
) -> SourceFile | None:
    """Resolve an import statement to the source file it refers to.

    Args:
        module (str): Dotted module name as written in the import statement.
        level (int): Relative import level (0 for absolute, 1+ for ``from . import``).
        from_file (SourceFile): The file containing the import, used to anchor relative
            imports and to disambiguate identically named modules.
        index (ScanIndex): The scanned repository's module map to resolve against.

    Returns:
        SourceFile | None: The resolved source file, or ``None`` when the import cannot be
        resolved unambiguously within the scanned repository.
    """
    return index.resolve_module(module, from_file, level)
