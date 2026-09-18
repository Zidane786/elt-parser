"""Top-level package for etl-parser: deterministic lineage from ETL repositories.

Re-exports the package's main entry points so callers can ``import etl_parser`` without
knowing its internal module layout: :func:`scan` (deterministic lineage, see
``etl_parser.pipeline.scan``), :class:`~etl_parser.sdk.ParserClient` (the service-style
SDK), and :func:`~etl_parser.ai_analysis.analyze` /
:func:`~etl_parser.ai_analysis.analyze_async` (optional, explicitly-enabled AI analysis).
See ``docs/superpowers/specs/2026-09-15-etl-parser-design.md`` sections 3 and 12.
"""

__version__ = "0.1.0"

# Provider transports are still lazily imported only when AI is explicitly enabled.
from etl_parser.ai_analysis import AnalysisConfig, AnalysisRun, analyze, analyze_async  # noqa: E402
from etl_parser.sdk import ParserClient  # noqa: E402

__all__ = ["AnalysisConfig", "AnalysisRun", "ParserClient", "analyze", "analyze_async", "scan"]


def scan(*args, **kwargs):
    """Scan a repository/path and build its deterministic lineage graph.

    A thin, lazily-imported wrapper around :func:`etl_parser.pipeline.scan` kept here so
    ``import etl_parser`` alone does not pull in the full scanner/worker import graph.

    Args:
        *args: Forwarded to :func:`etl_parser.pipeline.scan`.
        **kwargs: Forwarded to :func:`etl_parser.pipeline.scan`; see that function for the
            full set of options (schema provider, bindings, products, parsers, etc.).

    Returns:
        graph.builder.LineageGraph: The built lineage graph, as returned by
        :func:`etl_parser.pipeline.scan`.
    """
    from etl_parser.pipeline import scan as scan_repository

    return scan_repository(*args, **kwargs)
