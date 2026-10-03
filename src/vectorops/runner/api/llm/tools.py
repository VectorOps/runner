"""Typed tool bindings and workflow-side approval/execution composition."""

from collections.abc import Awaitable, Callable
import hashlib
import json
import typing

import connect
import pydantic

from vectorops.runner.api import composition as c, mailbox
from vectorops.runner.runner.activity_context import ActivityExecutionContext

A = typing.TypeVar("A", bound=pydantic.BaseModel)


class ToolApproval(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    channel: str = pydantic.Field(
        default="tool.approval", min_length=1, max_length=256, pattern=r"^\S+$"
    )


class ToolApprovalResponse(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    approved: bool = pydantic.Field(strict=True)
    reason: typing.Optional[str] = None


class ToolResponse(pydantic.BaseModel):
    content: list[connect.ToolResultContentBlock]
    is_error: bool = False


class ToolExecutionError(Exception):
    """An expected, model-visible tool error rather than an infrastructure failure."""


class ToolBinding(typing.Protocol):
    @property
    def definition(self) -> connect.ToolSpec: ...

    @property
    def version(self) -> int: ...

    @property
    def approval(self) -> typing.Optional[ToolApproval]: ...

    def validate(self, arguments: dict[str, pydantic.JsonValue]) -> None:
        """Validate deterministically before approval, without performing I/O."""
        ...

    async def execute(
        self,
        context: ActivityExecutionContext,
        arguments: dict[str, pydantic.JsonValue],
    ) -> ToolResponse: ...


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


def _digest(value: pydantic.JsonValue) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(encoded.encode()).hexdigest()


async def invoke_tool(
    binding: typing.Optional[ToolBinding],
    request: connect.ToolCallBlock,
    *,
    instance: typing.Optional[str] = None,
) -> connect.ToolResultMessage:
    """Validate, optionally await host approval, and execute a recorded tool call.

    Approval uses a JSON mailbox response with a strict ``approved`` boolean and
    optional ``reason``. Decisions apply to one call, not future invocations.
    The helper runs in workflow context; only the implementation runs in an
    activity. Expected validation errors and rejections return tool error results.
    """
    c.workflow_context()
    request = request.model_copy(deep=True)
    definition = binding.definition if binding is not None else None
    approval = binding.approval if binding is not None else None
    error: typing.Optional[str] = None
    try:
        if binding is None:
            raise ToolExecutionError(f"unknown tool: {request.name}")
        if definition is None or definition.name != request.name:
            raise ToolExecutionError(f"tool binding does not match: {request.name}")
        binding.validate(request.arguments)
    except ToolExecutionError as exc:
        error = str(exc)

    if error is None and approval is not None:
        answer = await mailbox.request_input(
            approval.channel,
            payload={
                "instance": instance,
                "call": request.model_dump(mode="json"),
                "tool": definition.model_dump(mode="json") if definition else None,
                "tool_version": binding.version if binding is not None else None,
            },
            response_type="json",
        )
        decision = ToolApprovalResponse.model_validate(answer)
        if not decision.approved:
            error = decision.reason or "Tool call rejected by host"

    return await _execute_tool(
        binding,
        request,
        error,
        {
            "instance": instance,
            "call_digest": _digest(request.model_dump(mode="json")),
            "tool_digest": (
                _digest(definition.model_dump(mode="json")) if definition else None
            ),
            "tool_version": binding.version if binding is not None else None,
            "approval": approval.model_dump(mode="json") if approval else None,
            "error": error,
        },
    )


@c.activity_wrapper(name="invoke_tool", kind="llm.tool", version=2)
async def _execute_tool(
    call: c.ActivityCall,
    binding: typing.Optional[ToolBinding],
    request: connect.ToolCallBlock,
    error: typing.Optional[str],
    input: dict[str, pydantic.JsonValue],
) -> connect.ToolResultMessage:
    async def invoke(ctx: ActivityExecutionContext) -> pydantic.JsonValue:
        try:
            if error is not None:
                raise ToolExecutionError(error)
            if binding is None:
                raise ToolExecutionError(f"unknown tool: {request.name}")
            response = await binding.execute(ctx, request.arguments)
        except ToolExecutionError as exc:
            response = ToolResponse(
                content=[connect.TextBlock(text=str(exc))], is_error=True
            )
        return connect.ToolResultMessage(
            tool_call_id=request.id,
            tool_name=request.name,
            content=response.content,
            is_error=response.is_error,
        ).model_dump(mode="json")

    result = await call.run(input=input, invoke=invoke)
    return connect.ToolResultMessage.model_validate(result)
