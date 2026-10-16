"""Provider streaming inside a durable activity, with explicit final-result adapters."""

from collections.abc import AsyncGenerator, Callable
from contextlib import aclosing
from typing import Optional, Union

import connect
import pydantic

from vectorops.runner.api import composition as c
from vectorops.runner.api.llm import models, observability, provider
from vectorops.runner.api.llm import retry
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner import streaming

Finalizer = Callable[
    [streaming.ActivityFinal[pydantic.JsonValue], ActivityExecutionContext],
    streaming.ActivityFinal[pydantic.JsonValue],
]


async def invoke(
    call: c.ActivityCall,
    input: Union[
        dict[str, pydantic.JsonValue], Callable[[], dict[str, pydantic.JsonValue]]
    ],
    backend: provider.Provider,
    settings: models.Settings,
    request: Union[connect.GenerateRequest, Callable[[], connect.GenerateRequest]],
    policy: models.RetryPolicy,
    finalize: Optional[Finalizer] = None,
) -> pydantic.JsonValue:
    async def stream(
        ctx: ActivityExecutionContext,
    ) -> AsyncGenerator[streaming.StreamItem, None]:
        selected = request() if callable(request) else request
        observation = observability.Observation(settings, selected)
        async with aclosing(
            retry.stream(backend, ctx, settings, selected, policy, observation)
        ) as items:
            async for item in items:
                if isinstance(item, streaming.ActivityFinal) and finalize is not None:
                    item = finalize(item, ctx)
                yield item

    return await call.stream(input=input, invoke=stream)
