"""``etl-parser schema`` command group: fetch source-of-truth schemas into ``catalog.json``.

Database credentials are read from the environment only (see
``etl_parser.schema.postgres`` and ``etl_parser.schema.redshift``); there is deliberately
no ``--dsn`` or ``--password`` option so secrets never appear in shell history or logs.
No driver or AWS SDK is imported until a source is actually fetched.
"""

from __future__ import annotations

import json
from pathlib import Path

import typer

from etl_parser.observability import observed

schema_app = typer.Typer(
    help="Fetch database/table/column schemas and relations from source systems.",
    no_args_is_help=True,
)


def build_sources(
    *,
    glue: bool = False,
    databases: list[str] | None = None,
    postgres: bool = False,
    redshift: bool = False,
    schema_names: list[str] | None = None,
    iam: bool = False,
    aws_profile: str | None = None,
    region: str | None = None,
) -> list:
    """Instantiate the requested schema sources without connecting to anything yet.

    Args:
        glue: Include a :class:`~etl_parser.schema.glue.GlueSchemaSource`.
        databases: Glue database names to restrict to (``None`` means all).
        postgres: Include a :class:`~etl_parser.schema.postgres.PostgresSchemaSource`
            configured from the environment.
        redshift: Include a :class:`~etl_parser.schema.redshift.RedshiftSchemaSource`
            configured from the environment.
        schema_names: Postgres/Redshift schema names to restrict to (``None`` means all).
        iam: Use IAM authentication for Redshift.
        aws_profile: AWS profile for Glue and Redshift IAM authentication.
        region: AWS region for Glue and Redshift IAM authentication.

    Returns:
        list: Schema sources in the order Glue, Postgres, Redshift.
    """
    from etl_parser.schema import GlueSchemaSource, PostgresSchemaSource, RedshiftSchemaSource

    sources = []
    if glue:
        sources.append(GlueSchemaSource(profile=aws_profile, region=region, databases=databases))
    if postgres:
        sources.append(PostgresSchemaSource(schemas=schema_names))
    if redshift:
        sources.append(
            RedshiftSchemaSource(schemas=schema_names, iam=iam, profile=aws_profile, region=region)
        )
    return sources


@schema_app.command("fetch")
@observed("command.schema_fetch")
def schema_fetch(
    glue: bool = typer.Option(False, "--glue", help="Fetch the AWS Glue Data Catalog"),
    database: list[str] | None = typer.Option(
        None, "--database", help="Glue database to fetch (repeatable); default: all"
    ),
    postgres: bool = typer.Option(
        False,
        "--postgres",
        help="Fetch PostgreSQL via ETL_PARSER_POSTGRES_DSN or PGHOST/PGPORT/PGDATABASE/PGUSER/"
        "PGPASSWORD",
    ),
    redshift: bool = typer.Option(
        False,
        "--redshift",
        help="Fetch Redshift via ETL_PARSER_REDSHIFT_DSN or REDSHIFT_HOST/REDSHIFT_PORT/"
        "REDSHIFT_DATABASE/REDSHIFT_USER/REDSHIFT_PASSWORD",
    ),
    schema_name: list[str] | None = typer.Option(
        None, "--schema-name", help="Postgres/Redshift schema to fetch (repeatable); default: all"
    ),
    iam: bool = typer.Option(False, "--iam", help="Redshift IAM authentication (no password)"),
    aws_profile: str | None = typer.Option(
        None, envvar="AWS_PROFILE", help="AWS profile for Glue and Redshift IAM authentication"
    ),
    region: str | None = typer.Option(
        None, envvar="AWS_REGION", help="AWS region for Glue and Redshift IAM authentication"
    ),
    out: Path = typer.Option(Path("catalog.json"), help="Output catalog file"),
    log_dir: Path | None = typer.Option(None, help="Persist run events and metrics in this folder"),
    log_level: str = typer.Option("INFO", help="Console level; file logs retain DEBUG events"),
):
    """Fetch schemas from Glue, PostgreSQL and/or Redshift into one ``catalog.json``.

    Credentials are never accepted on the command line: Postgres and Redshift read them
    from the environment, Glue uses the AWS credential chain (``--aws-profile``). The
    written file has the shape ``{"databases": [...], "relations": [...]}`` and can be
    passed to ``scan``/``run`` as ``--schema``.

    Args:
        glue: Fetch the Glue Data Catalog.
        database: Glue database names to restrict to; repeatable.
        postgres: Fetch PostgreSQL using environment credentials.
        redshift: Fetch Redshift using environment credentials.
        schema_name: Postgres/Redshift schema names to restrict to; repeatable.
        iam: Authenticate to Redshift through IAM instead of a password.
        aws_profile: AWS profile for Glue and Redshift IAM.
        region: AWS region for Glue and Redshift IAM.
        out: Destination ``catalog.json`` path.
        log_dir: Directory to persist run events and metrics in.
        log_level: Console log level; file logs always retain DEBUG events.

    Raises:
        typer.BadParameter: If no source flag is given.
        typer.Exit: With code 2 when a source cannot be read (missing driver, missing
            credentials, denied permission); the message never contains a secret.
    """
    from etl_parser.schema import SchemaSourceError, write_schema_catalog

    if not (glue or postgres or redshift):
        raise typer.BadParameter("Choose at least one of --glue, --postgres, --redshift")
    sources = build_sources(
        glue=glue,
        databases=database or None,
        postgres=postgres,
        redshift=redshift,
        schema_names=schema_name or None,
        iam=iam,
        aws_profile=aws_profile,
        region=region,
    )
    try:
        catalog = write_schema_catalog(sources, out)
    except (SchemaSourceError, ImportError) as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(2) from None
    typer.echo(
        json.dumps(
            {
                "databases": len(catalog["databases"]),
                "tables": sum(len(d["tables"]) for d in catalog["databases"]),
                "relations": len(catalog["relations"]),
                "out": str(out),
            },
            sort_keys=True,
        )
    )
