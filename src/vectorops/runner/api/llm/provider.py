"""Provider I/O and complete streaming snapshots, independent of workflow state."""

from collections.abc import AsyncGenerator, Callable
from contextlib import aclosing
from typing import Optional, Protocol

import connect

from vectorops.runner.api import composition as c
from vectorops.runner.api.llm import deadlines, snapshots
from vectorops.runner.api.llm.models import LLMResponseError, Settings
from vectorops.runner.authentication import models as auth_models
from vectorops.runner.authentication.service import AuthenticationService
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


class _Authentication:
    def __init__(self, auth: connect.TransportAuth) -> None:
        self.auth = auth

    async def resolve(
        self, context: Optional[connect.AuthContext] = None
    ) -> connect.ResolvedAuth:
        with deadlines.suspend():
            return await self.auth.resolve(context)

    async def refresh(self, context: Optional[connect.AuthContext] = None) -> bool:
        with deadlines.suspend():
            return await self.auth.refresh(context)


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
        try:
            async with aclosing(self._stream(context, settings, request)) as stream:
                async for item in stream:
                    yield item
        except connect.AuthenticationError:
            raise auth_models.AuthenticationError(
                "provider rejected authentication"
            ) from None

    async def _stream(
        self,
        context: ActivityExecutionContext,
        settings: Settings,
        request: connect.GenerateRequest,
    ) -> AsyncGenerator[StreamItem, None]:
        """Accumulate provider deltas; only response_end produces a final value."""
        auth = self._auth
        if auth is None:
            authentication = context.authentication or AuthenticationService()
            with deadlines.suspend():
                auth = await authentication.transport(
                    settings.provider,
                    settings.model,
                    settings.credential_profile or "default",
                    context.execution_id,
                )
        options = connect.RequestOptions(
            timeout=settings.timeout,
            stream_read_timeout=settings.stream_read_timeout,
            auth=_Authentication(auth),
            idempotency_key=context.step_id,
        )
        snapshot = snapshots.Accumulator()
        async with self._client_factory() as client:
            stream = client.stream(
                settings.model, request, provider=settings.provider, options=options
            )
            async for event in stream:
                if event.type == "error":
                    if event.error.status_code in (401, 403):
                        raise auth_models.AuthenticationError(
                            "provider rejected authentication"
                        )
                    raise connect.exception_from_error_info(event.error)
                if event.type == "response_end":
                    validate_response(event.response)
                    yield c.final(event.response.model_dump(mode="json"))
                    return
                yield c.delta(snapshot.update(event))
        raise LLMResponseError("LLM stream ended without a terminal response")
