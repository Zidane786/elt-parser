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

__all__ = [
    "AnalysisConfig",
    "AnalysisRun",
    "GlueSchemaSource",
    "ParserClient",
    "PostgresSchemaSource",
    "RedshiftSchemaSource",
    "SchemaSource",
    "SchemaSourceError",
    "analyze",
    "analyze_async",
    "scan",
    "write_schema_catalog",
]

_SCHEMA_EXPORTS = {
    "GlueSchemaSource",
    "PostgresSchemaSource",
    "RedshiftSchemaSource",
    "SchemaSource",
    "SchemaSourceError",
    "write_schema_catalog",
}


def __getattr__(name: str):
    """Lazily resolve the schema-source exports so no driver is imported eagerly.

    Args:
        name: Attribute requested on the package.

    Returns:
        The attribute from :mod:`etl_parser.schema`.

    Raises:
        AttributeError: For any name that is not a lazy export.
    """
    if name in _SCHEMA_EXPORTS:
        import etl_parser.schema as schema

        return getattr(schema, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


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
