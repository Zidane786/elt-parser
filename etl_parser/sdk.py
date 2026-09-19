"""Public service-style SDK: a reusable client wrapping deterministic scan plus AI analysis.

Wraps :func:`etl_parser.ai_analysis.analyze_async` (deterministic lineage, and optionally
audited AI lineage/descriptions per spec sections 3, 11, 12) behind a class that other
services can embed without a web framework or provider SDK dependency. Each call to
:class:`ParserClient` owns independent results, metrics, logs, and provider clients;
caller-injected runners remain caller-owned and are not closed by the client.
"""

from __future__ import annotations

import asyncio
import inspect
from pathlib import Path

from etl_parser.ai_analysis import AnalysisConfig, AnalysisRun, analyze_async
from etl_parser.artifacts import write_analysis
from etl_parser.export.agent_catalog import export_agent_catalog
from etl_parser.observability import RunObserver, observed
from etl_parser.schema.base import is_schema_source
from etl_parser.schema.catalog import write_schema_catalog


def apply_export_options(
    result,
    *,
    prior=None,
    schema=None,
    include_code_schema: bool = False,
    generate: list[str] | None = None,
    databases: list[str] | None = None,
) -> list[str]:
    """Re-export ``result.catalog`` with catalog options the exporter understands.

    ``export_agent_catalog`` gains ``schema=``, ``include_code_schema=``, ``generate=`` and
    ``databases=`` in a parallel work package; this wrapper forwards each option only when
    the installed exporter's signature accepts it (or takes ``**kwargs``), so the SDK/CLI
    surface is stable regardless of which exporter version is present. It also mirrors
    ``catalog["schema_drift"]`` onto ``result.schema_drift`` (``None`` when absent).

    Args:
        result: The :class:`~etl_parser.ai_analysis.AnalysisRun` to update in place.
        prior: The prior catalog the run was exported against.
        schema: The scan's schema argument; forwarded only when it is a full
            :class:`~etl_parser.schema.base.SchemaSource`.
        include_code_schema: Whether code-only tables/columns may be added to
            ``databases``.
        generate: Catalog sections to generate (``None`` means all).
        databases: Database names to restrict the catalog to (``None`` means all).

    Returns:
        list[str]: Requested option names the exporter does not support yet (dropped).
    """
    parameters = inspect.signature(export_agent_catalog).parameters
    var_kwargs = any(p.kind is inspect.Parameter.VAR_KEYWORD for p in parameters.values())
    requested = {}
    if is_schema_source(schema):
        requested["schema"] = schema
    if include_code_schema:
        requested["include_code_schema"] = True
    if generate is not None:
        requested["generate"] = list(generate)
    if databases is not None:
        requested["databases"] = list(databases)
    supported = {k: v for k, v in requested.items() if k in parameters or var_kwargs}
    if supported:
        configuration = getattr(result, "configuration", None) or {}
        ai_ran = configuration.get("ai_lineage", "off") != "off" or configuration.get(
            "descriptions"
        )
        # After AI stages the current catalog carries their descriptions; keep them.
        base = result.catalog if ai_ran else prior
        result.catalog = export_agent_catalog(result.document, base, **supported)
    result.schema_drift = (result.catalog or {}).get("schema_drift")
    return sorted(set(requested) - set(supported))


class ParserClient:
    """Reusable configuration, with isolated state for each sync/async invocation.

    ``config`` on a call replaces the client's default configuration. Scanner
    keyword arguments match ``etl_parser.pipeline.scan`` (schema, bindings,
    parsers, source_provider, ref, source_path, etc.). Environment-based provider
    configuration is a CLI feature; SDK credentials/configuration are explicit.

    Attributes:
        log_dir: Default directory to persist run events and metrics in, unless
            overridden per call.
        log_level: Default console log level, unless overridden per call.
    """

    def __init__(
        self,
        *,
        config: AnalysisConfig | dict | None = None,
        log_dir: str | Path | None = None,
        log_level: str = "INFO",
    ):
        """Create a client with a default analysis configuration.

        Args:
            config: Default :class:`~etl_parser.ai_analysis.AnalysisConfig`, or an
                equivalent dict. Defaults to an all-deterministic, no-AI configuration.
            log_dir: Default directory to persist run events and metrics in.
            log_level: Default console log level; file logs always retain DEBUG events.
        """
        self._config = AnalysisConfig.model_validate(config or {}).model_copy(deep=True)
        self.log_dir = log_dir
        self.log_level = log_level

    @staticmethod
    def fetch_schema(sources, out_path: str | Path | None = None) -> dict:
        """Fetch and merge source-of-truth schemas into one catalog dict.

        Args:
            sources: :class:`~etl_parser.schema.base.SchemaSource` instances (Glue,
                Postgres, Redshift, or custom), fetched in order.
            out_path: Optional ``catalog.json`` path to write as well.

        Returns:
            dict: ``{"databases": [...], "relations": [...]}``, deterministically sorted
            and usable as ``schema=`` on :meth:`run`/:meth:`arun` or as a CLI ``--schema``
            file.
        """
        return write_schema_catalog(sources, out_path)

    def run(self, source, **kwargs) -> AnalysisRun:
        """Analyze synchronously. Inside an event loop use ``await arun(...)``.

        Args:
            source: Repository path or GitHub URL to analyze; forwarded to :meth:`arun`.
            **kwargs: Forwarded to :meth:`arun`.

        Returns:
            AnalysisRun: The completed analysis run.

        Raises:
            RuntimeError: If called from within a running event loop.
        """
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.arun(source, **kwargs))
        raise RuntimeError("An event loop is running; use await client.arun(...)")

    async def arun(
        self,
        source,
        *,
        config: AnalysisConfig | dict | None = None,
        runner=None,
        prior=None,
        out_dir: str | Path | None = None,
        schema=None,
        include_code_schema: bool = False,
        generate: list[str] | None = None,
        databases: list[str] | None = None,
        **scan_options,
    ) -> AnalysisRun:
        """Analyze without blocking the event loop on scanning or artifact export.

        No files are written unless log_dir/out_dir is configured. Each invocation
        owns an observer, even inside another observed run. Do not pass an observer
        here; advanced caller-owned observation is available via analyze_async.

        Args:
            source: Repository path or GitHub URL to analyze.
            config: Configuration for this call only; defaults to the client's own
                configuration set at construction time. Does not mutate the client.
            runner: Caller-injected LLM runner; forwarded to
                :func:`~etl_parser.ai_analysis.analyze_async`. Left open by the caller.
            prior: Prior catalog dict to preserve descriptions/flags from, when exporting.
            out_dir: Directory to write result artifacts to. No artifacts are written if
                omitted.
            schema: Source-of-truth schema: a :class:`~etl_parser.schema.base.SchemaSource`
                (Glue/Postgres/Redshift), a catalog dict, or a path to a ``catalog.json``
                or schema mapping file. Used to qualify SQL, expand stars, and (when a
                full source) to supply ``databases``/``relations`` to the exporter.
            include_code_schema: Also add tables/columns discovered only in code to the
                catalog ``databases`` (marked ``schema_source: code``). Off by default:
                the source schema is the truth and code-only tables become
                ``missing_in_source`` diagnostics.
            generate: Catalog sections to generate (among ``databases``, ``scripts``,
                ``relations``, ``lineage``, ``schedules``); ``None`` means all.
            databases: Restrict the catalog to these database names; ``None`` means all.
            **scan_options: Additional keyword arguments forwarded to
                ``etl_parser.pipeline.scan`` (bindings, parsers, source_provider, ref,
                source_path, etc.), plus optional per-call ``log_dir``, ``log_level``,
                ``log_max_bytes``, and ``log_max_files`` overrides.

        Returns:
            AnalysisRun: The completed analysis run, with ``status``, ``metrics``,
            ``run_id``, ``log_path`` and ``schema_drift`` (the catalog's drift block, or
            ``None``) set from this call.

        Raises:
            ValueError: If ``observer`` is passed in ``scan_options``; the client always
                owns its own observer.
        """
        options = (
            self._config if config is None else AnalysisConfig.model_validate(config)
        ).model_copy(deep=True)
        if "observer" in scan_options:
            raise ValueError(
                "ParserClient owns its observer; use analyze_async for custom observers"
            )
        log_dir = scan_options.pop("log_dir", self.log_dir)
        log_level = scan_options.pop("log_level", self.log_level)
        observer = RunObserver(
            log_dir=log_dir,
            log_level=log_level,
            max_log_bytes=scan_options.pop("log_max_bytes", 10_000_000),
            max_log_files=scan_options.pop("log_max_files", 20),
        )
        error = None
        result = None
        try:
            observer.event("run.started", command="sdk.run")
            result = await self._execute(
                source,
                config=options,
                runner=runner,
                prior=prior,
                out_dir=out_dir,
                observer=observer,
                schema=schema,
                include_code_schema=include_code_schema,
                generate=generate,
                databases=databases,
                **scan_options,
            )
        except BaseException as exc:
            error = exc
            raise
        finally:
            observer.finish(error=error)
            if result is not None:
                result.status = observer.status
                result.metrics = observer.snapshot()
                result.run_id = observer.run_id
                result.log_path = observer.run_dir
        return result

    @staticmethod
    @observed("sdk.run")
    async def _execute(
        source,
        *,
        config,
        runner,
        prior,
        out_dir,
        observer,
        schema=None,
        include_code_schema=False,
        generate=None,
        databases=None,
        **scan_options,
    ):
        """Run the analysis and, if requested, write its artifacts in a worker thread.

        Args:
            source: Repository path or GitHub URL to analyze.
            config: Resolved :class:`~etl_parser.ai_analysis.AnalysisConfig` for this run.
            runner: Caller-injected LLM runner, or ``None``.
            prior: Prior catalog dict to preserve descriptions/flags from, or ``None``.
            out_dir: Directory to write result artifacts to, or ``None`` to skip writing.
            observer: This call's observer, used by the ``@observed`` decorator's span.
            schema: Schema source/dict/path forwarded to the scan and the exporter.
            include_code_schema: Exporter option; see :meth:`ParserClient.arun`.
            generate: Exporter option; see :meth:`ParserClient.arun`.
            databases: Exporter option; see :meth:`ParserClient.arun`.
            **scan_options: Additional keyword arguments forwarded to
                :func:`~etl_parser.ai_analysis.analyze_async`.

        Returns:
            AnalysisRun: The result of :func:`~etl_parser.ai_analysis.analyze_async`, with
            ``schema_drift`` mirrored from the catalog and ``artifact_path`` set when
            ``out_dir`` was given.
        """
        result = await analyze_async(
            source,
            config=config,
            runner=runner,
            prior=prior,
            schema=schema,
            include_code_schema=include_code_schema,
            generate=generate,
            databases=databases,
            **scan_options,
        )
        dropped = apply_export_options(
            result,
            prior=prior,
            schema=schema,
            include_code_schema=include_code_schema,
            generate=generate,
            databases=databases,
        )
        for name in dropped:
            observer.event("export.option_unsupported", level="DEBUG", actor="sdk", option=name)
        if out_dir is not None:
            result.artifact_path = await asyncio.to_thread(write_analysis, result, out_dir)
        return result
