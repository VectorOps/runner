"""Local async activities routed through the active workflow session."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import aclosing
from contextvars import ContextVar
from dataclasses import dataclass
import inspect
from pathlib import Path
import typing

import pydantic

from vectorops.runner import models
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.context import (
    ChildWorkflowHandle,
    WorkflowExecutionContext,
)
from vectorops.runner.runner.errors import ActivityError
from vectorops.runner.runner import streaming

P = typing.ParamSpec("P")
T = typing.TypeVar("T", bound=pydantic.JsonValue)
_CURRENT_CONTEXT: ContextVar[typing.Optional[WorkflowExecutionContext]] = ContextVar(
    "workflow_context", default=None
)


@dataclass(frozen=True)
class ActivityDefinition(typing.Generic[P, T]):
    func: Callable[typing.Concatenate[ActivityExecutionContext, P], Awaitable[T]]
    name: str
    kind: str = "activity"

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        context = _CURRENT_CONTEXT.get()
        if context is None:
            raise RuntimeError("activities require an active workflow context")
        bound = inspect.signature(self.func).bind(None, *args, **kwargs)
        bound.apply_defaults()
        arguments = dict(bound.arguments)
        arguments.pop(next(iter(arguments)))
        operation = models.StepOperation(
            kind=self.kind, name=self.name, input=arguments
        )

        async def invoke(ctx: ActivityExecutionContext) -> pydantic.JsonValue:
            token = _CURRENT_CONTEXT.set(None)
            try:
                return await self.func(ctx, *args, **kwargs)
            finally:
                _CURRENT_CONTEXT.reset(token)

        return typing.cast(T, await context.run_activity(operation, invoke))


def activity(*, name: typing.Optional[str] = None, kind: str = "activity") -> Callable[
    [Callable[typing.Concatenate[ActivityExecutionContext, P], Awaitable[T]]],
    ActivityDefinition[P, T],
]:
    def decorate(
        func: Callable[typing.Concatenate[ActivityExecutionContext, P], Awaitable[T]],
    ) -> ActivityDefinition[P, T]:
        if not inspect.iscoroutinefunction(func):
            raise TypeError("activity implementations must be async functions")
        _validate_context_parameter(func)
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
        typing.Concatenate[ActivityExecutionContext, P],
        AsyncGenerator[typing.Union[ActivityDelta, ActivityFinal[T]], None],
    ]
    name: str
    kind: str = "activity"

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        context = _CURRENT_CONTEXT.get()
        if context is None:
            raise RuntimeError("activities require an active workflow context")
        bound = inspect.signature(self.func).bind(None, *args, **kwargs)
        bound.apply_defaults()
        arguments = dict(bound.arguments)
        arguments.pop(next(iter(arguments)))
        operation = models.StepOperation(
            kind=self.kind, name=self.name, input=arguments
        )

        async def invoke(
            ctx: ActivityExecutionContext,
        ) -> AsyncGenerator[streaming.StreamItem, None]:
            token = _CURRENT_CONTEXT.set(None)
            try:
                async with aclosing(self.func(ctx, *args, **kwargs)) as stream:
                    async for item in stream:
                        yield typing.cast(streaming.StreamItem, item)
            finally:
                _CURRENT_CONTEXT.reset(token)

        return typing.cast(T, await context.run_streaming_activity(operation, invoke))


def streamable_activity(
    *, name: typing.Optional[str] = None, kind: str = "activity"
) -> Callable[
    [
        Callable[
            typing.Concatenate[ActivityExecutionContext, P],
            AsyncGenerator[typing.Union[ActivityDelta, ActivityFinal[T]], None],
        ]
    ],
    StreamableActivityDefinition[P, T],
]:
    def decorate(
        func: Callable[
            typing.Concatenate[ActivityExecutionContext, P],
            AsyncGenerator[typing.Union[ActivityDelta, ActivityFinal[T]], None],
        ],
    ) -> StreamableActivityDefinition[P, T]:
        if not inspect.isasyncgenfunction(func):
            raise TypeError("streamable activities must be async generator functions")
        _validate_context_parameter(func)
        operation = models.StepOperation(kind=kind, name=name or func.__name__)
        return StreamableActivityDefinition(func, operation.name, operation.kind)

    return decorate


def _validate_context_parameter(func: Callable[..., object]) -> None:
    parameters = list(inspect.signature(func).parameters.values())
    if not parameters or parameters[0].kind not in (
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    ):
        raise TypeError("activity implementations require a first context parameter")


@dataclass(frozen=True)
class WorkflowDefinition(typing.Generic[P, T]):
    func: Callable[typing.Concatenate[WorkflowExecutionContext, P], Awaitable[T]]
    name: str

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        handle = await self.start(*args, **kwargs)
        results = await gather_workflows([handle])
        return typing.cast(T, results[0])

    async def start(self, *args: P.args, **kwargs: P.kwargs) -> ChildWorkflowHandle:
        """Establish a durable child invocation without awaiting its completion."""
        context = _CURRENT_CONTEXT.get()
        if context is None:
            raise RuntimeError("child workflows require an active workflow context")
        bound = inspect.signature(self.func).bind(None, *args, **kwargs)
        bound.apply_defaults()
        arguments = dict(bound.arguments)
        arguments.pop(next(iter(arguments)))
        configuration = models.Workflow(
            id="binding", definition_ref=self.name, input=arguments
        )
        source = inspect.getsourcefile(self.func)
        root = context.project.workflows_path.resolve()
        if source is not None and Path(source).resolve().is_relative_to(root):
            target = Path(source).resolve().relative_to(root).as_posix()
        else:
            if self.func.__module__.startswith("_runner_workflow_"):
                raise ValueError("workflow source must remain inside the workspace")
            target = f"python:{self.func.__module__}"
        return await context.spawn_workflow(
            target, self.func.__qualname__, input=configuration.input
        )


def workflow(*, name: typing.Optional[str] = None) -> Callable[
    [Callable[typing.Concatenate[WorkflowExecutionContext, P], Awaitable[T]]],
    WorkflowDefinition[P, T],
]:
    def decorate(
        func: Callable[typing.Concatenate[WorkflowExecutionContext, P], Awaitable[T]],
    ) -> WorkflowDefinition[P, T]:
        if not inspect.iscoroutinefunction(func):
            raise TypeError("workflow implementations must be async functions")
        parameters = list(inspect.signature(func).parameters.values())
        if not parameters or parameters[0].kind not in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ):
            raise TypeError(
                "workflow implementations require a first context parameter"
            )
        if not func.__qualname__.isidentifier():
            raise ValueError("persistent workflows must be module-level functions")
        if any(
            parameter.kind
            not in (
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.KEYWORD_ONLY,
            )
            for parameter in parameters[1:]
        ):
            raise TypeError("workflow inputs must be named parameters")
        if name is not None and not name.strip():
            raise ValueError("workflow name must not be empty")
        module_name = func.__module__
        if module_name.startswith("_runner_workflow_"):
            source = inspect.getsourcefile(func)
            if source is None:
                raise ValueError("workflow source must be discoverable")
            module_name = Path(source).stem
        return WorkflowDefinition(
            func, name if name is not None else f"{module_name}.{func.__qualname__}"
        )

    return decorate


async def gather_workflows(
    handles: list[ChildWorkflowHandle],
) -> list[pydantic.JsonValue]:
    """Join explicitly started children in invocation order and return their results."""
    context = _CURRENT_CONTEXT.get()
    if context is None:
        raise RuntimeError("joining workflows requires an active workflow context")
    outcomes = await context.join_workflows(handles)
    results: list[pydantic.JsonValue] = []
    for outcome in outcomes:
        if outcome.kind == "failed":
            raise ActivityError(outcome.error)
        results.append(outcome.result)
    return results
