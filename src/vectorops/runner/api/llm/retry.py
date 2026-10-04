"""Retry provider I/O inside one recorded activity, never workflow storage writes."""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import aclosing
from typing import Optional

import aiohttp
import connect
import pydantic

from vectorops.runner import diagnostics
from vectorops.runner.api.llm import deadlines, models, provider
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner import streaming


def is_transient(error: Exception) -> bool:
    if isinstance(error, connect.ConnectError):
        if isinstance(
            error,
            (
                connect.AuthenticationError,
                connect.ContextLengthError,
                connect.PermanentProviderError,
            ),
        ):
            return False
        return (
            isinstance(error, (connect.RateLimitError, connect.TransientProviderError))
            or error.error.retryable
            or error.error.status_code in (408, 429)
            or (
                error.error.status_code is not None
                and 500 <= error.error.status_code < 600
            )
        )
    if isinstance(
        error,
        (aiohttp.ClientConnectorCertificateError, aiohttp.ClientConnectorSSLError),
    ):
        return False
    return isinstance(
        error,
        (
            TimeoutError,
            ConnectionError,
            aiohttp.ClientConnectionError,
            aiohttp.ClientPayloadError,
        ),
    )


async def stream(
    backend: provider.Provider,
    context: ActivityExecutionContext,
    settings: models.Settings,
    request: connect.GenerateRequest,
    policy: models.RetryPolicy,
) -> AsyncGenerator[streaming.StreamItem, None]:
    """Discard failed attempts, retain one final response, and propagate cancellation.

    Each attempt receives detached inputs and the same activity identity. Only
    provider iteration is retried; errors from the consumer's persistence path
    cannot be mistaken for provider failures.
    """
    for attempt in range(policy.max_retries + 1):
        final: Optional[streaming.ActivityFinal[pydantic.JsonValue]] = None
        try:
            async with aclosing(
                deadlines.stream(
                    backend.stream(
                        context,
                        settings.model_copy(deep=True),
                        request.model_copy(deep=True),
                    ),
                    settings,
                )
            ) as items:
                async for item in items:
                    if final is not None:
                        raise models.LLMResponseError(
                            "LLM stream yielded after final response"
                        )
                    if isinstance(item, streaming.ActivityFinal):
                        response = connect.AssistantMessage.model_validate(item.value)
                        provider.validate_response(response)
                        final = streaming.ActivityFinal(
                            response.model_dump(mode="json")
                        )
                    elif isinstance(item, streaming.ActivityDelta):
                        yield item
                    else:
                        raise models.LLMResponseError("invalid LLM stream item")
            if final is None:
                raise models.LLMResponseError(
                    "LLM stream ended without a terminal response"
                )
        except Exception as exc:
            retryable = is_transient(exc)
            if not retryable or attempt == policy.max_retries:
                diagnostics.log(
                    "llm.request_failed",
                    level=diagnostics.Level.ERROR,
                    fields={
                        "attempt": attempt + 1,
                        "retryable": retryable,
                        "max_retries": policy.max_retries,
                    },
                    error=exc,
                )
                raise
            delay = min(policy.max_delay, policy.initial_delay * 2**attempt)
            diagnostics.log(
                "llm.request_retry",
                level=diagnostics.Level.WARNING,
                fields={
                    "attempt": attempt + 1,
                    "next_attempt": attempt + 2,
                    "delay_seconds": delay,
                },
                error=exc,
            )
            yield streaming.ActivityDelta({"content": []})
            await asyncio.sleep(delay)
            continue
        yield final
        return
