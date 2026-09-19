"""Lazy selection of the company's Agent SDK runners (no custom LLM transport).

Install ``agent-sdk`` from the trusted internal distribution or its source checkout.
Scanning does not need the SDK, AWS credentials, or an LLM. Implements the pluggable
runner side of the description engine (spec section 11): :class:`RunnerConfig` validates
and holds runner selection/credentials, and :func:`configured_runner` and
:func:`bedrock_lambda_runner` construct the selected ``agent_sdk`` runner without this
package depending on any provider SDK at import time.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator

if TYPE_CHECKING:
    from agent_sdk.runners.aicore_bedrock import BedrockInvokeLambdaRunner


class RunnerConfig(BaseModel):
    """Validated configuration for selecting and constructing an Agent SDK LLM runner.

    ``hide_input_in_errors`` keeps secrets out of pydantic validation error messages.

    Attributes:
        runner: Which Agent SDK runner to use. ``"lbi"`` is an alias for
            ``"lambda-bedrock-invoke"``, normalized by :meth:`normalize_runner`.
        lambda_arn: Lambda function name or ARN, required for the Bedrock invoke runner.
        region: AWS region for the Bedrock invoke runner.
        aws_profile: Named AWS profile to use, if not the default credential chain.
        web_adapter: Whether to use the SDK Lambda Web Adapter envelope.
        base_url: HTTPS base URL for the Anthropic-compatible runner; no credentials,
            query, or fragment allowed (validated by :meth:`valid_url`).
        api_key: API key for the Anthropic-compatible runner. Excluded from
            serialization/repr so it is never written into config or manifest output.
        extra_headers: Custom HTTP headers for the Anthropic-compatible runner. Excluded
            from serialization/repr; validated by :meth:`valid_headers`.
    """

    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)
    runner: Literal["lambda-bedrock-invoke", "lbi", "anthropic"] = "lambda-bedrock-invoke"
    lambda_arn: str | None = None
    region: str = "us-east-1"
    aws_profile: str | None = None
    web_adapter: bool = True
    base_url: str | None = None
    # Credentials are usable in memory, never serialized into config/manifests.
    api_key: SecretStr | None = Field(default=None, exclude=True, repr=False)
    extra_headers: dict[str, str] = Field(default_factory=dict, exclude=True, repr=False)

    @field_validator("runner")
    @classmethod
    def normalize_runner(cls, value):
        """Map the ``"lbi"`` alias to its canonical runner name.

        Args:
            value: The raw ``runner`` field value.

        Returns:
            str: ``"lambda-bedrock-invoke"`` if ``value`` was ``"lbi"``, else ``value``.
        """
        return "lambda-bedrock-invoke" if value == "lbi" else value

    @field_validator("base_url")
    @classmethod
    def valid_url(cls, value):
        """Reject any base URL that is not a bare HTTPS origin.

        Args:
            value: The raw ``base_url`` field value.

        Returns:
            str | None: ``value`` with any trailing slash stripped, or ``None`` unchanged.

        Raises:
            ValueError: If ``value`` is not HTTPS, has no hostname, carries credentials,
                a query string, a fragment, or contains control/whitespace characters.
        """
        if value is None:
            return value
        parsed = urlsplit(value)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or any(c.isspace() or ord(c) < 32 for c in value)
        ):
            raise ValueError("Base URL must be HTTPS without credentials, query or fragment")
        return value.rstrip("/")

    @field_validator("extra_headers")
    @classmethod
    def valid_headers(cls, headers):
        """Reject oversized, malformed, transport-owned, or duplicate HTTP headers.

        Args:
            headers: The raw ``extra_headers`` field value.

        Returns:
            dict[str, str]: ``headers`` unchanged, once validated.

        Raises:
            ValueError: If there are too many headers or they are too large in total, if
                any name/value is not valid HTTP header syntax, if a name collides with a
                header the transport must control (``host``, ``content-length``,
                ``transfer-encoding``), or if two names differ only by case.
        """
        if len(headers) > 100 or sum(len(k) + len(v) for k, v in headers.items()) > 16384:
            raise ValueError("Extra headers exceed size limits")
        for name, value in headers.items():
            if (
                not re.fullmatch(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+", name)
                or any(ord(c) < 32 or ord(c) > 126 for c in value)
                or name.lower() in {"host", "content-length", "transfer-encoding"}
            ):
                raise ValueError("Invalid header name/value or transport-owned header")
        if len({k.lower() for k in headers}) != len(headers):
            raise ValueError("Duplicate case-insensitive header names")
        return headers


def configured_runner(config: RunnerConfig, *, timeout: float = 60):
    """Only initialize the selected SDK runner; no automatic provider fallback.

    Args:
        config: Validated runner selection and credentials.
        timeout: Request timeout in seconds, used only by the Anthropic-compatible runner.

    Returns:
        The constructed Agent SDK runner instance (``BedrockInvokeLambdaRunner`` or
        ``AnthropicRunner``), ready to pass to
        :class:`~etl_parser.describe.engine.DescriptionEngine`.

    Raises:
        ValueError: If the Bedrock runner is selected without ``lambda_arn``, or the
            Anthropic runner is selected without both ``base_url`` and a non-blank
            ``api_key``.
        RuntimeError: If ``agent_sdk.runners.anthropic`` is not installed.
    """
    if config.runner == "lambda-bedrock-invoke":
        if not config.lambda_arn:
            raise ValueError("Bedrock runner requires lambda_arn")
        return bedrock_lambda_runner(
            config.lambda_arn,
            region=config.region,
            aws_profile=config.aws_profile,
            web_adapter=config.web_adapter,
        )
    if not config.base_url or not config.api_key or not config.api_key.get_secret_value().strip():
        raise ValueError("Anthropic runner requires base_url and api_key")
    try:
        from agent_sdk.runners.anthropic import AnthropicRunner
    except ImportError as exc:
        raise RuntimeError(
            "Install gdtc-agent-sdk from your trusted internal distribution "
            "with AnthropicRunner support"
        ) from exc
    return AnthropicRunner(
        api_key=config.api_key.get_secret_value(),
        base_url=config.base_url,
        extra_headers=dict(config.extra_headers),
        timeout=timeout,
        count_tokens_url=None,
        httpx_client_kwargs={"follow_redirects": False},
    )


async def close_runner(runner):
    """Close factory-owned HTTP clients; caller-injected runners remain caller-owned.

    Args:
        runner: The runner instance to close, typically one returned by
            :func:`configured_runner`. A no-op if it has no ``aclose`` method.
    """
    close = getattr(runner, "aclose", None)
    if close is not None:
        await close()


def bedrock_lambda_runner(
    lambda_arn: str,
    *,
    region: str = "us-east-1",
    aws_profile: str | None = None,
    web_adapter: bool = True,
) -> BedrockInvokeLambdaRunner:
    """Construct the SDK runner; transport and response handling belong to it.

    Args:
        lambda_arn: Lambda function name or ARN to invoke.
        region: AWS region the Lambda function lives in.
        aws_profile: Named AWS profile to use, if not the default credential chain.
        web_adapter: Whether to use the SDK Lambda Web Adapter envelope.

    Returns:
        BedrockInvokeLambdaRunner: The constructed runner.

    Raises:
        ValueError: If ``lambda_arn`` is blank.
        RuntimeError: If ``agent_sdk.runners.aicore_bedrock`` is not installed.
    """
    if not lambda_arn.strip():
        raise ValueError("A Lambda function name or ARN is required")
    try:
        from agent_sdk.runners.aicore_bedrock import BedrockInvokeLambdaRunner
    except ImportError as exc:
        raise RuntimeError(
            "Description generation requires your gdtc-agent-sdk (agent-sdk >=1.3.1,<2). "
            "Install it from your trusted internal distribution or source checkout."
        ) from exc
    return BedrockInvokeLambdaRunner(
        lambda_arn, region=region, aws_profile=aws_profile, web_adapter=web_adapter
    )
