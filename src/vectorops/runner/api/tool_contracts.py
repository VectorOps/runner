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
AuthContext = tool_policy.AuthContext


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
        auth_context: typing.Optional[Callable[[A], Awaitable[AuthContext]]] = None,
        contextual_auth_context: typing.Optional[
            Callable[[ActivityExecutionContext, A], Awaitable[AuthContext]]
        ] = None,
    ) -> None:
        if version < 1:
            raise ValueError("tool version must be positive")
        if auth_context is not None and contextual_auth_context is not None:
            raise ValueError("provide only one auth context callback")
        self._definition = connect.ToolSpec.external(
            name=name,
            description=description,
            input_schema=arguments.model_json_schema(),
        )
        self._arguments = arguments
        self._invoke = invoke
        self._version = version
        self._approval = approval
        self._auth_context = auth_context
        self._contextual_auth_context = contextual_auth_context

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

    async def auth_context(
        self, arguments: dict[str, pydantic.JsonValue]
    ) -> AuthContext:
        if self._contextual_auth_context is not None:
            raise ToolExecutionError("tool auth context requires an activity context")
        if self._auth_context is None:
            return AuthContext()
        return await self._auth_context(self._validated(arguments))

    async def auth_context_for_call(
        self,
        context: ActivityExecutionContext,
        arguments: dict[str, pydantic.JsonValue],
    ) -> AuthContext:
        if self._contextual_auth_context is not None:
            return await self._contextual_auth_context(
                context, self._validated(arguments)
            )
        return await self.auth_context(arguments)

    async def execute(
        self,
        context: ActivityExecutionContext,
        arguments: dict[str, pydantic.JsonValue],
    ) -> ToolResponse:
        return await self._invoke(context, self._validated(arguments))


class _AuthMetadata:
    def __init__(
        self, binding: ToolBinding, callback: tool_authorization.AuthContextCallback
    ) -> None:
        self._binding = binding
        self._callback = callback

    @property
    def definition(self) -> connect.ToolSpec:
        return self._binding.definition

    @property
    def version(self) -> int:
        return self._binding.version

    @property
    def approval(self) -> typing.Optional[ToolApproval]:
        return self._binding.approval

    def validate(self, arguments: dict[str, pydantic.JsonValue]) -> None:
        self._binding.validate(arguments)

    async def auth_context(
        self, arguments: dict[str, pydantic.JsonValue]
    ) -> AuthContext:
        return await self._callback(arguments)


class _AuthActivityBinding(_AuthMetadata):
    def __init__(
        self,
        binding: ActivityToolBinding,
        callback: tool_authorization.AuthContextCallback,
    ) -> None:
        super().__init__(binding, callback)
        self._activity = binding

    async def execute(
        self,
        context: ActivityExecutionContext,
        arguments: dict[str, pydantic.JsonValue],
    ) -> ToolResponse:
        return await self._activity.execute(context, arguments)


class _AuthWorkflowBinding(_AuthMetadata, WorkflowToolBinding):
    def __init__(
        self,
        binding: WorkflowToolBinding,
        callback: tool_authorization.AuthContextCallback,
    ) -> None:
        super().__init__(binding, callback)
        self._workflow = binding

    async def execute_workflow(
        self,
        context: WorkflowExecutionContext,
        arguments: dict[str, pydantic.JsonValue],
    ) -> ToolResponse:
        return await self._workflow.execute_workflow(context, arguments)


def with_auth_context(
    binding: ToolBinding, callback: tool_authorization.AuthContextCallback
) -> ToolBinding:
    if isinstance(binding, WorkflowToolBinding):
        return _AuthWorkflowBinding(binding, callback)
    return _AuthActivityBinding(binding, callback)
