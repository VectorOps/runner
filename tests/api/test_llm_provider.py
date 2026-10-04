from collections.abc import AsyncGenerator
from typing import Optional, Union

import connect
import pytest

from vectorops.runner.api import composition as c, llm
from vectorops.runner.api.llm.models import LLMResponseError
from vectorops.runner.api.llm import retry
from vectorops.runner.runner.activity_context import ActivityExecutionContext


class Client(connect.AsyncLLMClient):
    def __init__(self, events: list[connect.StreamEvent]) -> None:
        super().__init__()
        self.events = events
        self.options: Optional[connect.RequestOptions] = None

    def stream(
        self,
        model: Union[str, connect.ModelSpec],
        request: connect.GenerateRequest,
        *,
        provider: Optional[str] = None,
        options: Optional[connect.RequestOptions] = None,
    ) -> connect.StreamHandle:
        self.options = options

        async def events() -> AsyncGenerator[connect.StreamEvent, None]:
            for event in self.events:
                yield event

        return connect.StreamHandle(events())


async def test_provider_emits_snapshots_and_preserves_final_metadata() -> None:
    response = connect.AssistantMessage(
        content=[connect.TextBlock(text="hello")],
        finish_reason="stop",
        protocol_state={"key": "opaque"},
    )
    client = Client(
        [
            connect.TextDeltaEvent(index=0, delta="hel"),
            connect.TextDeltaEvent(index=0, delta="lo"),
            connect.ResponseEndEvent(response=response),
        ]
    )
    backend = llm.ConnectProvider(auth=connect.NoAuth(), client_factory=lambda: client)
    items = [
        item
        async for item in backend.stream(
            ActivityExecutionContext("run", "workflow", "step"),
            llm.Settings(model="test"),
            connect.GenerateRequest(messages=[connect.UserMessage(content="hello")]),
        )
    ]
    assert items[0] == c.delta({"content": [{"type": "text", "text": "hel"}]})
    assert items[1] == c.delta({"content": [{"type": "text", "text": "hello"}]})
    assert items[2] == c.final(response.model_dump(mode="json"))
    assert client.options is not None and client.options.idempotency_key == "step"


async def test_missing_final_is_not_success() -> None:
    client = Client([connect.TextDeltaEvent(index=0, delta="partial")])
    backend = llm.ConnectProvider(auth=connect.NoAuth(), client_factory=lambda: client)
    with pytest.raises(LLMResponseError, match="terminal response"):
        async for _ in backend.stream(
            ActivityExecutionContext("run", "workflow", "step"),
            llm.Settings(model="test"),
            connect.GenerateRequest(messages=[connect.UserMessage(content="hello")]),
        ):
            pass


async def test_connect_error_event_retries_with_same_idempotency_key() -> None:
    clients = [
        Client(
            [
                connect.TextDeltaEvent(index=0, delta="discard"),
                connect.ErrorEvent(
                    error=connect.ErrorInfo(
                        code="rate_limit", message="busy", status_code=429
                    )
                ),
            ]
        ),
        Client(
            [
                connect.TextDeltaEvent(index=0, delta="fresh"),
                connect.ResponseEndEvent(
                    response=connect.AssistantMessage(
                        content=[connect.TextBlock(text="fresh")], finish_reason="stop"
                    )
                ),
            ]
        ),
    ]
    pending = iter(clients)
    backend = llm.ConnectProvider(
        auth=connect.NoAuth(), client_factory=lambda: next(pending)
    )
    items = [
        item
        async for item in retry.stream(
            backend,
            ActivityExecutionContext("exec", "workflow", "step"),
            llm.Settings(model="test"),
            connect.GenerateRequest(messages=[connect.UserMessage(content="hello")]),
            llm.RetryPolicy(initial_delay=0),
        )
    ]
    assert items[1] == c.delta({"content": []})
    assert items[2] == c.delta({"content": [{"type": "text", "text": "fresh"}]})
    assert all(
        client.options is not None and client.options.idempotency_key == "step"
        for client in clients
    )
