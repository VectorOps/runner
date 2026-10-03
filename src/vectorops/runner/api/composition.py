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
from vectorops.runner.runner.context import WorkflowExecutionContext
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
    version: int = 1

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        context = _CURRENT_CONTEXT.get()
        if context is None:
            raise RuntimeError("activities require an active workflow context")
        bound = inspect.signature(self.func).bind(None, *args, **kwargs)
        bound.apply_defaults()
        arguments = dict(bound.arguments)
        arguments.pop(next(iter(arguments)))
        operation = models.StepOperation(
            kind=self.kind, name=self.name, input=arguments, version=self.version
        )

        async def invoke(ctx: ActivityExecutionContext) -> pydantic.JsonValue:
            token = _CURRENT_CONTEXT.set(None)
            try:
                return await self.func(ctx, *args, **kwargs)
            finally:
                _CURRENT_CONTEXT.reset(token)

        return typing.cast(T, await context.run_activity(operation, invoke))


def activity(
    *,
    name: typing.Optional[str] = None,
    kind: typing.Optional[str] = None,
    namespace: typing.Optional[str] = None,
    version: int = 1,
) -> Callable[
    [Callable[typing.Concatenate[ActivityExecutionContext, P], Awaitable[T]]],
    ActivityDefinition[P, T],
]:
    def decorate(
        func: Callable[typing.Concatenate[ActivityExecutionContext, P], Awaitable[T]],
    ) -> ActivityDefinition[P, T]:
        if not inspect.iscoroutinefunction(func):
            raise TypeError("activity implementations must be async functions")
        _validate_context_parameter(func)
        operation = models.StepOperation(
            kind=_contract_kind(func, kind, namespace, "activity"),
            name=name or func.__name__,
            version=version,
        )
        return ActivityDefinition(
            func, operation.name, operation.kind, operation.version
        )

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
    version: int = 1

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        context = _CURRENT_CONTEXT.get()
        if context is None:
            raise RuntimeError("activities require an active workflow context")
        bound = inspect.signature(self.func).bind(None, *args, **kwargs)
        bound.apply_defaults()
        arguments = dict(bound.arguments)
        arguments.pop(next(iter(arguments)))
        operation = models.StepOperation(
            kind=self.kind, name=self.name, input=arguments, version=self.version
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
    *,
    name: typing.Optional[str] = None,
    kind: typing.Optional[str] = None,
    namespace: typing.Optional[str] = None,
    version: int = 1,
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
        operation = models.StepOperation(
            kind=_contract_kind(func, kind, namespace, "activity"),
            name=name or func.__name__,
            version=version,
        )
        return StreamableActivityDefinition(
            func, operation.name, operation.kind, operation.version
        )

    return decorate


def _module_name(func: Callable[..., object]) -> str:
    if not func.__module__.startswith("_runner_workflow_"):
        return func.__module__
    module_name = func.__globals__.get("__runner_contract_module__")
    if isinstance(module_name, str):
        return module_name
    source = inspect.getsourcefile(func)
    if source is None:
        raise ValueError("workflow source must be discoverable")
    return Path(source).stem


def _contract_kind(
    func: Callable[..., object],
    kind: typing.Optional[str],
    namespace: typing.Optional[str],
    default: str,
) -> str:
    """Generate local contract names without using transient loader module IDs."""
    if kind is not None and namespace is not None:
        raise ValueError("kind and namespace are mutually exclusive")
    if kind is not None:
        if not kind.strip():
            raise ValueError("kind must not be empty")
        return kind
    if namespace is None:
        return default
    if not all(part.isidentifier() for part in namespace.split(".")):
        raise ValueError("namespace must contain dot-separated identifiers")
    if not func.__qualname__.isidentifier():
        raise ValueError(
            "generated kinds require a module-level callable or explicit kind"
        )
    return f"{namespace}.{_module_name(func)}.{func.__qualname__}"


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
    kind: str = "workflow"
    version: int = 1

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        context = _CURRENT_CONTEXT.get()
        if context is None:
            raise RuntimeError("child workflows require an active workflow context")
        bound = inspect.signature(self.func).bind(None, *args, **kwargs)
        bound.apply_defaults()
        arguments = dict(bound.arguments)
        arguments.pop(next(iter(arguments)))
        configuration = models.Workflow(
            id="binding",
            definition_ref=self.name,
            input=arguments,
            kind=self.kind,
            version=self.version,
        )
        source = inspect.getsourcefile(self.func)
        root = context.project.workflows_path.resolve()
        if source is not None and Path(source).resolve().is_relative_to(root):
            target = Path(source).resolve().relative_to(root).as_posix()
        else:
            if self.func.__module__.startswith("_runner_workflow_"):
                raise ValueError("workflow source must remain inside the workspace")
            target = f"python:{self.func.__module__}"
        handle = await context.spawn_workflow(
            target,
            self.func.__qualname__,
            input=configuration.input,
            kind=self.kind,
            version=self.version,
        )
        outcomes = await context.join_workflows([handle])
        outcome = outcomes[0]
        if outcome.kind == "failed":
            raise ActivityError(outcome.error)
        return typing.cast(T, outcome.result)


def workflow(
    *,
    name: typing.Optional[str] = None,
    kind: typing.Optional[str] = None,
    namespace: typing.Optional[str] = None,
    version: int = 1,
) -> Callable[
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
        module_name = _module_name(func)
        contract = models.Workflow(
            id="declaration",
            definition_ref="declaration",
            kind=_contract_kind(func, kind, namespace, "workflow"),
            version=version,
        )
        return WorkflowDefinition(
            func,
            name if name is not None else f"{module_name}.{func.__qualname__}",
            contract.kind,
            contract.version,
        )

    return decorate
