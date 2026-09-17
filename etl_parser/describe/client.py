"""Lazy configuration of the company's Agent SDK Lambda invoke runner.

Install ``agent-sdk`` from the trusted internal distribution or its source checkout.
Scanning does not need the SDK, AWS credentials, or an LLM.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from agent_sdk.runners.aicore_bedrock import BedrockInvokeLambdaRunner


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
