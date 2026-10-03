"""Typed tool bindings; orchestration and approvals remain outside the registry."""

from collections.abc import Awaitable, Callable
from typing import Generic, Protocol, TypeVar

import connect
import pydantic

from vectorops.runner.runner.activity_context import ActivityExecutionContext

A = TypeVar("A", bound=pydantic.BaseModel)


class ToolResponse(pydantic.BaseModel):
    content: list[connect.ToolResultContentBlock]
    is_error: bool = False


class ToolExecutionError(Exception):
    """An expected, model-visible tool error rather than an infrastructure failure."""


class ToolBinding(Protocol):
    @property
    def definition(self) -> connect.ToolSpec: ...

    @property
    def version(self) -> int: ...

    async def execute(
        self,
        context: ActivityExecutionContext,
        arguments: dict[str, pydantic.JsonValue],
    ) -> ToolResponse: ...


class Tool(Generic[A]):
    def __init__(
        self,
        name: str,
        description: str,
        arguments: type[A],
        invoke: Callable[[ActivityExecutionContext, A], Awaitable[ToolResponse]],
        *,
        version: int = 1,
    ) -> None:
        if version < 1:
            raise ValueError("tool version must be positive")
        self._definition = connect.ToolSpec.external(
            name=name,
            description=description,
            input_schema=arguments.model_json_schema(),
        )
        self._arguments = arguments
        self._invoke = invoke
        self._version = version

    @property
    def definition(self) -> connect.ToolSpec:
        return self._definition.model_copy(deep=True)

    @property
    def version(self) -> int:
        return self._version

    async def execute(
        self,
        context: ActivityExecutionContext,
        arguments: dict[str, pydantic.JsonValue],
    ) -> ToolResponse:
        try:
            validated = self._arguments.model_validate(arguments)
        except pydantic.ValidationError as exc:
            raise ToolExecutionError(str(exc)) from exc
        return await self._invoke(context, validated)
