"""Source-of-truth schema fetchers: Glue, Postgres and Redshift, plus catalog merging.

Importing this package pulls in no driver or AWS SDK; each source imports its transport
lazily on first use, so ``import etl_parser.schema`` is safe in environments without
``boto3``, ``psycopg`` or ``redshift_connector`` installed.
"""

from etl_parser.schema.base import SchemaSource, SchemaSourceError, as_schema_provider
from etl_parser.schema.catalog import write_schema_catalog
from etl_parser.schema.glue import GlueSchemaProvider, GlueSchemaSource
from etl_parser.schema.postgres import PostgresSchemaSource

__all__ = [
    "GlueSchemaProvider",
    "GlueSchemaSource",
    "PostgresSchemaSource",
    "SchemaSource",
    "SchemaSourceError",
    "as_schema_provider",
    "write_schema_catalog",
]
