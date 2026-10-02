"""Local async activities routed through the active workflow session."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import aclosing
from contextvars import ContextVar
from dataclasses import dataclass
import inspect
import typing

import pydantic

from vectorops.runner import models
from vectorops.runner.runner.context import WorkflowExecutionContext
from vectorops.runner.runner import streaming

P = typing.ParamSpec("P")
T = typing.TypeVar("T", bound=pydantic.JsonValue)
_CURRENT_CONTEXT: ContextVar[typing.Optional[WorkflowExecutionContext]] = ContextVar(
    "workflow_context", default=None
)


@dataclass(frozen=True)
class ActivityDefinition(typing.Generic[P, T]):
    func: Callable[P, Awaitable[T]]
    name: str
    kind: str = "activity"

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        context = _CURRENT_CONTEXT.get()
        if context is None:
            raise RuntimeError("activities require an active workflow context")
        bound = inspect.signature(self.func).bind(*args, **kwargs)
        bound.apply_defaults()
        operation = models.StepOperation(
            kind=self.kind, name=self.name, input=dict(bound.arguments)
        )

        async def invoke() -> pydantic.JsonValue:
            token = _CURRENT_CONTEXT.set(None)
            try:
                return await self.func(*args, **kwargs)
            finally:
                _CURRENT_CONTEXT.reset(token)

        return typing.cast(T, await context.run_activity(operation, invoke))


def activity(
    *, name: typing.Optional[str] = None, kind: str = "activity"
) -> Callable[[Callable[P, Awaitable[T]]], ActivityDefinition[P, T]]:
    def decorate(func: Callable[P, Awaitable[T]]) -> ActivityDefinition[P, T]:
        if not inspect.iscoroutinefunction(func):
            raise TypeError("activity implementations must be async functions")
        operation = models.StepOperation(kind=kind, name=name or func.__name__)
        return ActivityDefinition(func=func, name=operation.name, kind=operation.kind)

    return decorate


ActivityDelta = streaming.ActivityDelta
ActivityFinal = streaming.ActivityFinal


def delta(value: pydantic.JsonValue) -> ActivityDelta:
    return ActivityDelta(value)


def final(value: T) -> ActivityFinal[T]:
    return ActivityFinal(value)


@dataclass(frozen=True)
class StreamableActivityDefinition(typing.Generic[P, T]):
    func: Callable[
        P, AsyncGenerator[typing.Union[ActivityDelta, ActivityFinal[T]], None]
    ]
    name: str
    kind: str = "activity"

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        context = _CURRENT_CONTEXT.get()
        if context is None:
            raise RuntimeError("activities require an active workflow context")
        bound = inspect.signature(self.func).bind(*args, **kwargs)
        bound.apply_defaults()
        operation = models.StepOperation(
            kind=self.kind, name=self.name, input=dict(bound.arguments)
        )

        async def invoke() -> AsyncGenerator[streaming.StreamItem, None]:
            token = _CURRENT_CONTEXT.set(None)
            try:
                async with aclosing(self.func(*args, **kwargs)) as stream:
                    async for item in stream:
                        yield typing.cast(streaming.StreamItem, item)
            finally:
                _CURRENT_CONTEXT.reset(token)

        return typing.cast(T, await context.run_streaming_activity(operation, invoke))


def streamable_activity(
    *, name: typing.Optional[str] = None, kind: str = "activity"
) -> Callable[
    [Callable[P, AsyncGenerator[typing.Union[ActivityDelta, ActivityFinal[T]], None]]],
    StreamableActivityDefinition[P, T],
]:
    def decorate(
        func: Callable[
            P, AsyncGenerator[typing.Union[ActivityDelta, ActivityFinal[T]], None]
        ],
    ) -> StreamableActivityDefinition[P, T]:
        if not inspect.isasyncgenfunction(func):
            raise TypeError("streamable activities must be async generator functions")
        operation = models.StepOperation(kind=kind, name=name or func.__name__)
        return StreamableActivityDefinition(func, operation.name, operation.kind)

    return decorate
