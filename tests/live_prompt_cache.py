"""Opt-in live audit: uv run python -m tests.live_prompt_cache HISTORY --send."""

import argparse
import asyncio
import json
import os
from pathlib import Path
import tempfile
from typing import Optional
from uuid import uuid4

import aiohttp
import connect
from connect.providers.chatgpt import ChatGPTProvider
from connect.transport.http import HttpTransport
from connect.transport.sse import iter_sse_response
import pydantic
import pytest

from tests.api.test_prompt_cache_history import Prompt, reconstruct_history
from vectorops.runner.authentication.service import AuthenticationService


class UsageDetails(pydantic.BaseModel):
    cached_tokens: int = 0
    cache_write_tokens: int = 0


class Usage(pydantic.BaseModel):
    input_tokens: int = 0
    output_tokens: int = 0
    input_tokens_details: UsageDetails = pydantic.Field(default_factory=UsageDetails)


class Response(pydantic.BaseModel):
    id: str
    status: str
    model: Optional[str] = None
    service_tier: Optional[str] = None
    usage: Usage
    prompt_cache_diagnostics: Optional[dict[str, pydantic.JsonValue]] = None


def payload(
    prompt: Prompt,
    max_output_tokens: int,
    comparison: Optional[str] = None,
    cache_key: Optional[str] = None,
) -> dict[str, pydantic.JsonValue]:
    value = pydantic.TypeAdapter(dict[str, pydantic.JsonValue]).validate_json(
        prompt.settings
    )
    value["input"] = [
        pydantic.TypeAdapter(pydantic.JsonValue).validate_json(item)
        for item in prompt.items
    ]
    value["max_output_tokens"] = max_output_tokens
    value["store"] = False
    value["stream"] = True
    if comparison is not None:
        value["prompt_cache_options"] = {"comparison_response_id": comparison}
    if cache_key is not None:
        value["prompt_cache_key"] = cache_key
    return value


def recorded_input(prompt: Prompt) -> int:
    return sum(
        int(value)
        for key in ("input_tokens", "cache_read_tokens", "cache_write_tokens")
        if isinstance(value := prompt.recorded_usage.get(key), int)
    )


def select(prompts: list[Prompt], start: int, count: int, limit: int) -> list[Prompt]:
    if start < 1 or not 1 <= count <= 10 or start + count - 1 > len(prompts):
        raise ValueError(
            "Select 1-10 existing consecutive rounds using a 1-based start"
        )
    selected = prompts[start - 1 : start - 1 + count]
    if any(recorded_input(prompt) <= 0 for prompt in selected):
        raise ValueError(
            "Selected rounds require recorded input usage for the token limit"
        )
    if sum(recorded_input(prompt) for prompt in selected) > limit:
        raise ValueError("Selected rounds exceed the cumulative input-token limit")
    return selected


async def send(
    session: aiohttp.ClientSession, body: dict[str, pydantic.JsonValue]
) -> Response:
    """Read terminal metadata only; discard generated content and never execute tools."""
    async with session.post(
        "https://api.openai.com/v1/responses", json=body
    ) as response:
        if response.status != 200:
            raise RuntimeError(
                f"Responses API returned HTTP {response.status}; request ID {response.headers.get('x-request-id')}"
            )
        async for line in response.content:
            if not line.startswith(b"data: ") or line[6:].strip() == b"[DONE]":
                continue
            event = pydantic.TypeAdapter(dict[str, pydantic.JsonValue]).validate_json(
                line[6:]
            )
            if event.get("type") in (
                "response.completed",
                "response.incomplete",
                "response.failed",
            ):
                return Response.model_validate(event["response"])
            if event.get("type") == "error":
                raise RuntimeError("Responses API emitted a stream error")
        raise RuntimeError("Responses API stream ended without terminal usage")


async def send_chatgpt(
    session: aiohttp.ClientSession,
    body: dict[str, pydantic.JsonValue],
    profile: Optional[str],
) -> Response:
    """Use configured runner credentials and transport while retaining raw diagnostics."""
    model_name = body["model"]
    assert isinstance(model_name, str)
    model = connect.default_model_registry.resolve(model_name, "chatgpt")
    auth = await AuthenticationService().transport(f"chatgpt/{model_name}", profile)
    resolved = await auth.resolve(
        connect.AuthContext(
            provider="chatgpt", model=model_name, api_family=model.api_family
        )
    )
    options = connect.RequestOptions(
        auth=auth, headers=resolved.headers, idempotency_key=uuid4().hex
    )
    cache_key = body.get("prompt_cache_key")
    if isinstance(cache_key, str):
        options.provider_options["session_id"] = cache_key
    adapter = ChatGPTProvider()
    headers = {
        **adapter.build_headers(
            model, connect.GenerateRequest.model_construct(), options
        ),
        **resolved.headers,
    }
    http = HttpTransport(session=session)
    response = await http.stream(
        "POST",
        adapter.request_url(model),
        provider="chatgpt",
        model=model_name,
        api_family=model.api_family,
        auth=auth,
        headers=headers,
        json_body=body,
        timeout=180,
        expected_status=200,
    )
    async with response:
        async for frame in iter_sse_response(response):
            if frame.is_done or not frame.data:
                continue
            event = pydantic.TypeAdapter(dict[str, pydantic.JsonValue]).validate_json(
                frame.data
            )
            if event.get("type") in (
                "response.completed",
                "response.incomplete",
                "response.failed",
            ):
                return Response.model_validate(event["response"])
            if event.get("type") == "error":
                raise RuntimeError("ChatGPT emitted a stream error")
    raise RuntimeError("ChatGPT stream ended without terminal usage")


async def run(args: argparse.Namespace) -> None:
    with tempfile.TemporaryDirectory(prefix="prompt-cache-audit-") as directory:
        with pytest.MonkeyPatch.context() as monkeypatch:
            prompts = await reconstruct_history(
                args.history, monkeypatch, Path(directory)
            )
    selected = select(prompts, args.start, args.count, args.max_input_tokens)
    if not 1 <= args.max_output_tokens <= 4096:
        raise ValueError("Output-token cap must be between 1 and 4096")
    print(
        json.dumps(
            {
                "event": "preflight",
                "verified_rounds": len(prompts),
                "selected_steps": [prompt.label for prompt in selected],
                "recorded_input_tokens": sum(
                    recorded_input(prompt) for prompt in selected
                ),
                "max_output_tokens_per_call": args.max_output_tokens,
                "backend": args.backend,
                "send": args.send,
                "cache_key_supplied": args.cache_key is not None,
            }
        ),
        flush=True,
    )
    if not args.send:
        return
    key = os.environ.get("OPENAI_API_KEY")
    if args.backend == "openai" and not key:
        raise ValueError("OPENAI_API_KEY is required for --send")
    comparison: Optional[str] = None
    async with aiohttp.ClientSession(
        headers={"Authorization": f"Bearer {key}"} if args.backend == "openai" else {},
        timeout=aiohttp.ClientTimeout(total=180),
        read_bufsize=2**20,
    ) as session:
        for round_number, prompt in enumerate(selected, args.start):
            body = payload(prompt, args.max_output_tokens, comparison, args.cache_key)
            result = (
                await send_chatgpt(session, body, args.credential_profile)
                if args.backend == "chatgpt"
                else await send(session, body)
            )
            print(
                json.dumps(
                    {
                        "event": "round",
                        "round": round_number,
                        "step_id": prompt.label,
                        "historical_input_tokens": recorded_input(prompt),
                        "historical_cached_tokens": prompt.recorded_usage.get(
                            "cache_read_tokens", 0
                        ),
                        "comparison_response_id": comparison,
                        **result.model_dump(mode="json"),
                    }
                ),
                flush=True,
            )
            if result.status != "completed":
                raise RuntimeError(
                    "Response did not complete; stopping before another billed request"
                )
            comparison = result.id


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("history", type=Path)
    parser.add_argument("--backend", choices=("openai", "chatgpt"), default="openai")
    parser.add_argument(
        "--credential-profile", help="Configured ChatGPT credential profile"
    )
    parser.add_argument("--start", type=int, default=1)
    parser.add_argument("--count", type=int, default=3)
    parser.add_argument("--max-input-tokens", type=int, default=1_000_000)
    parser.add_argument("--max-output-tokens", type=int, default=2048)
    parser.add_argument(
        "--cache-key",
        help="Optional stable key; omitted by default, like the recorded workflow",
    )
    parser.add_argument(
        "--send",
        action="store_true",
        help="Authorize sending exact private history to OpenAI and incurring API charges",
    )
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
