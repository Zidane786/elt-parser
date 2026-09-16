"""Description clients are explicit caller choices; lineage never invokes them."""

from typing import Protocol


class LLMClient(Protocol):
    def complete(self, system: str, user: str) -> str: ...


class StubClient:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.calls = []

    def complete(self, system, user):
        self.calls.append((system, user))
        return next(self.responses)


class BedrockClient:
    def __init__(self, model_id, region=None):
        import boto3

        self.client = boto3.client("bedrock-runtime", region_name=region)
        self.model_id = model_id

    def complete(self, system, user):
        response = self.client.converse(
            modelId=self.model_id,
            system=[{"text": system}],
            messages=[{"role": "user", "content": [{"text": user}]}],
        )
        return "".join(block.get("text", "") for block in response["output"]["message"]["content"])
