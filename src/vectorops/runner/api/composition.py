"""Local async activities routed through the active workflow session."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from dataclasses import dataclass
import inspect
import typing

import pydantic

from vectorops.runner import models
from vectorops.runner.runner.context import WorkflowExecutionContext

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
