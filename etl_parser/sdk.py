"""Public service-style SDK: a reusable client wrapping deterministic scan plus AI analysis.

Wraps :func:`etl_parser.ai_analysis.analyze_async` (deterministic lineage, and optionally
audited AI lineage/descriptions per spec sections 3, 11, 12) behind a class that other
services can embed without a web framework or provider SDK dependency. Each call to
:class:`ParserClient` owns independent results, metrics, logs, and provider clients;
caller-injected runners remain caller-owned and are not closed by the client.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from etl_parser.ai_analysis import AnalysisConfig, AnalysisRun, analyze_async
from etl_parser.artifacts import write_analysis
from etl_parser.observability import RunObserver, observed


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
            **scan_options: Additional keyword arguments forwarded to
                ``etl_parser.pipeline.scan`` (schema, bindings, parsers, source_provider,
                ref, source_path, etc.), plus optional per-call ``log_dir``, ``log_level``,
                ``log_max_bytes``, and ``log_max_files`` overrides.

        Returns:
            AnalysisRun: The completed analysis run, with ``status``, ``metrics``,
            ``run_id``, and ``log_path`` set from this call's observer.

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
    async def _execute(source, *, config, runner, prior, out_dir, observer, **scan_options):
        """Run the analysis and, if requested, write its artifacts in a worker thread.

        Args:
            source: Repository path or GitHub URL to analyze.
            config: Resolved :class:`~etl_parser.ai_analysis.AnalysisConfig` for this run.
            runner: Caller-injected LLM runner, or ``None``.
            prior: Prior catalog dict to preserve descriptions/flags from, or ``None``.
            out_dir: Directory to write result artifacts to, or ``None`` to skip writing.
            observer: This call's observer, used by the ``@observed`` decorator's span.
            **scan_options: Additional keyword arguments forwarded to
                :func:`~etl_parser.ai_analysis.analyze_async`.

        Returns:
            AnalysisRun: The result of :func:`~etl_parser.ai_analysis.analyze_async`, with
            ``artifact_path`` set when ``out_dir`` was given.
        """
        result = await analyze_async(
            source, config=config, runner=runner, prior=prior, **scan_options
        )
        if out_dir is not None:
            result.artifact_path = await asyncio.to_thread(write_analysis, result, out_dir)
        return result
