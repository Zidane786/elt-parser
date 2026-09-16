"""Import resolution shared by Python and orchestration plugins."""

from etl_parser.scanner.repo import ScanIndex, SourceFile


def resolve_import(
    module: str, level: int, from_file: SourceFile, index: ScanIndex
) -> SourceFile | None:
    return index.resolve_module(module, from_file, level)
