"""Public Python/service interface. No web framework or provider SDK is required.

Create a reusable ParserClient; each invocation owns independent results, metrics,
logs and provider clients. Caller-injected runners remain caller-owned.
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
    """

    def __init__(
        self,
        *,
        config: AnalysisConfig | dict | None = None,
        log_dir: str | Path | None = None,
        log_level: str = "INFO",
    ):
        self._config = AnalysisConfig.model_validate(config or {}).model_copy(deep=True)
        self.log_dir = log_dir
        self.log_level = log_level

    def run(self, source, **kwargs) -> AnalysisRun:
        """Analyze synchronously. Inside an event loop use ``await arun(...)``."""
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
        result = await analyze_async(
            source, config=config, runner=runner, prior=prior, **scan_options
        )
        if out_dir is not None:
            result.artifact_path = await asyncio.to_thread(write_analysis, result, out_dir)
        return result
