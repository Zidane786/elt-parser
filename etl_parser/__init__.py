"""etl-parser: deterministic lineage from ETL repositories."""

__version__ = "0.1.0"

# Provider transports are still lazily imported only when AI is explicitly enabled.
from etl_parser.ai_analysis import AnalysisConfig, AnalysisRun, analyze, analyze_async  # noqa: E402
from etl_parser.sdk import ParserClient  # noqa: E402

__all__ = ["AnalysisConfig", "AnalysisRun", "ParserClient", "analyze", "analyze_async", "scan"]


def scan(*args, **kwargs):
    """Scan a repository/path; see :func:`etl_parser.pipeline.scan` for options."""
    from etl_parser.pipeline import scan as scan_repository

    return scan_repository(*args, **kwargs)
