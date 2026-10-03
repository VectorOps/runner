"""Provider I/O and complete streaming snapshots, independent of workflow state."""

from collections.abc import AsyncGenerator, Callable
from typing import Optional, Protocol

import connect
import pydantic

from vectorops.runner.api import composition as c
from vectorops.runner.api.llm.models import LLMResponseError, Settings
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.streaming import StreamItem


class Provider(Protocol):
    def stream(
        self,
        context: ActivityExecutionContext,
        settings: Settings,
        request: connect.GenerateRequest,
    ) -> AsyncGenerator[StreamItem, None]: ...


def validate_response(response: connect.AssistantMessage) -> None:
    calls = [block for block in response.content if block.type == "tool_call"]
    if response.finish_reason not in ("stop", "tool_call"):
        raise LLMResponseError(f"unsuccessful finish reason: {response.finish_reason}")
    if response.finish_reason == "tool_call" and not calls:
        raise LLMResponseError("tool-call finish reason without tool calls")
    if len({call.id for call in calls}) != len(calls):
        raise LLMResponseError("duplicate tool call IDs")


class ConnectProvider:
    def __init__(
        self,
        *,
        auth: Optional[connect.TransportAuth] = None,
        client_factory: Callable[[], connect.AsyncLLMClient] = connect.AsyncLLMClient,
    ) -> None:
        self._auth = auth
        self._client_factory = client_factory

    async def stream(
        self,
        context: ActivityExecutionContext,
        settings: Settings,
        request: connect.GenerateRequest,
    ) -> AsyncGenerator[StreamItem, None]:
        """Accumulate provider deltas; only response_end produces a final value."""
        auth = self._auth
        if auth is None:
            auth = connect.resolve_env_auth(settings.provider, model=settings.model)
        options = connect.RequestOptions(
            timeout=settings.timeout,
            stream_read_timeout=settings.stream_read_timeout,
            auth=auth,
            idempotency_key=context.step_id,
        )
        blocks: dict[int, dict[str, pydantic.JsonValue]] = {}
        snapshot: dict[str, pydantic.JsonValue] = {"content": []}
        async with self._client_factory() as client:
            stream = client.stream(
                settings.model, request, provider=settings.provider, options=options
            )
            async for event in stream:
                if event.type == "error":
                    raise connect.exception_from_error_info(event.error)
                if event.type == "response_end":
                    validate_response(event.response)
                    yield c.final(event.response.model_dump(mode="json"))
                    return
                if event.type == "response_start":
                    snapshot.update(
                        provider=event.provider,
                        model=event.model,
                        response_id=event.response_id,
                    )
                elif event.type in ("text_start", "reasoning_start"):
                    blocks[event.index] = {
                        "type": "text" if event.type == "text_start" else "reasoning",
                        "text": "",
                    }
                elif event.type in ("text_delta", "reasoning_delta"):
                    block = blocks.setdefault(
                        event.index,
                        {
                            "type": (
                                "text" if event.type == "text_delta" else "reasoning"
                            ),
                            "text": "",
                        },
                    )
                    text = block["text"]
                    if not isinstance(text, str):
                        raise LLMResponseError("invalid streaming text state")
                    block["text"] = text + event.delta
                elif event.type == "text_end":
                    blocks[event.index] = {"type": "text", "text": event.text}
                elif event.type == "reasoning_end":
                    blocks[event.index] = {
                        "type": "reasoning",
                        "text": event.text,
                        "signature": event.signature,
                        "redacted": event.redacted,
                    }
                elif event.type == "tool_call_start":
                    blocks[event.index] = {
                        "type": "tool_call",
                        "id": event.id,
                        "name": event.name,
                        "arguments_text": "",
                    }
                elif event.type == "tool_call_delta":
                    block = blocks.setdefault(
                        event.index,
                        {
                            "type": "tool_call",
                            "arguments_text": "",
                        },
                    )
                    arguments = block["arguments_text"]
                    if not isinstance(arguments, str):
                        raise LLMResponseError("invalid streaming tool state")
                    block["arguments_text"] = arguments + event.delta
                elif event.type == "tool_call_end":
                    blocks[event.index] = event.tool_call.model_dump(mode="json")
                elif event.type == "usage":
                    snapshot["usage"] = event.usage.model_dump(mode="json")
                snapshot["content"] = [dict(blocks[index]) for index in sorted(blocks)]
                yield c.delta(dict(snapshot))
        raise LLMResponseError("LLM stream ended without a terminal response")
