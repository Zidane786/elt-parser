"""Lazy selection of the company's Agent SDK runners (no custom LLM transport).

Install ``agent-sdk`` from the trusted internal distribution or its source checkout.
Scanning does not need the SDK, AWS credentials, or an LLM.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator

if TYPE_CHECKING:
    from agent_sdk.runners.aicore_bedrock import BedrockInvokeLambdaRunner


class RunnerConfig(BaseModel):
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
        return "lambda-bedrock-invoke" if value == "lbi" else value

    @field_validator("base_url")
    @classmethod
    def valid_url(cls, value):
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
    """Only initialize the selected SDK runner; no automatic provider fallback."""
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
    """Close factory-owned HTTP clients; caller-injected runners remain caller-owned."""
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
    """Construct the SDK runner; transport and response handling belong to it."""
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
