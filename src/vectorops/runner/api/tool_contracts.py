"""Tool metadata and separate activity/workflow execution capabilities."""

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Sequence
import typing

import connect
import pydantic

from vectorops.runner import tool_authorization, tool_policy
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.context import WorkflowExecutionContext

A = typing.TypeVar("A", bound=pydantic.BaseModel)


ToolApproval = tool_authorization.ToolApproval
ToolApprovalResponse = tool_policy.ToolApprovalResponse
ToolApprovalRequest = tool_policy.ToolApprovalRequest


class ToolResponse(pydantic.BaseModel):
    content: list[connect.ToolResultContentBlock]
    is_error: bool = False


ToolExecutionError = tool_authorization.ToolExecutionError
ToolMetadata = tool_authorization.ToolMetadata


class ActivityToolBinding(ToolMetadata, typing.Protocol):
    async def execute(
        self,
        context: ActivityExecutionContext,
        arguments: dict[str, pydantic.JsonValue],
    ) -> ToolResponse: ...


class WorkflowToolBinding(ABC):
    """A tool whose implementation composes durable workflow operations."""

    @property
    @abstractmethod
    def definition(self) -> connect.ToolSpec: ...

    @property
    @abstractmethod
    def version(self) -> int: ...

    @property
    @abstractmethod
    def approval(self) -> typing.Optional[ToolApproval]: ...

    @abstractmethod
    def validate(self, arguments: dict[str, pydantic.JsonValue]) -> None: ...

    @abstractmethod
    async def execute_workflow(
        self,
        context: WorkflowExecutionContext,
        arguments: dict[str, pydantic.JsonValue],
    ) -> ToolResponse: ...


ToolBinding: typing.TypeAlias = typing.Union[ActivityToolBinding, WorkflowToolBinding]


class ToolSet(typing.Protocol):
    """Pure, immutable selection advanced deterministically by completed round outcomes."""

    @property
    def bindings(self) -> Sequence[ToolBinding]: ...

    def advance(self, results: Sequence[connect.ToolResultMessage]) -> "ToolSet": ...


class Tool(typing.Generic[A]):
    def __init__(
        self,
        name: str,
        description: str,
        arguments: type[A],
        invoke: Callable[[ActivityExecutionContext, A], Awaitable[ToolResponse]],
        *,
        version: int = 1,
        approval: typing.Optional[ToolApproval] = None,
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
        self._approval = approval

    @property
    def definition(self) -> connect.ToolSpec:
        return self._definition.model_copy(deep=True)

    @property
    def version(self) -> int:
        return self._version

    @property
    def approval(self) -> typing.Optional[ToolApproval]:
        return self._approval

    def _validated(self, arguments: dict[str, pydantic.JsonValue]) -> A:
        try:
            return self._arguments.model_validate(arguments)
        except pydantic.ValidationError as exc:
            raise ToolExecutionError(str(exc)) from exc

    def validate(self, arguments: dict[str, pydantic.JsonValue]) -> None:
        self._validated(arguments)

    async def execute(
        self,
        context: ActivityExecutionContext,
        arguments: dict[str, pydantic.JsonValue],
    ) -> ToolResponse:
        return await self._invoke(context, self._validated(arguments))
