"""etl-parser: deterministic lineage from ETL repositories."""

__version__ = "0.1.0"


def scan(*args, **kwargs):
    """Scan a repository/path; see :func:`etl_parser.pipeline.scan` for options."""
    from etl_parser.pipeline import scan as scan_repository

    return scan_repository(*args, **kwargs)
