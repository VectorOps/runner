"""Typed tool bindings and workflow-side approval/execution composition."""

from collections.abc import Awaitable, Callable, Sequence
from abc import ABC, abstractmethod
import hashlib
import json
import typing

import connect
import pydantic

from vectorops.runner import diagnostics, tool_policy
from vectorops.runner.api import composition as c
from vectorops.runner.runner import mailbox as runtime_mailbox
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.context import WorkflowExecutionContext
from vectorops.runner.runner.errors import ActivityError

A = typing.TypeVar("A", bound=pydantic.BaseModel)


class ToolApproval(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    channel: str = pydantic.Field(
        default="tool.approval", min_length=1, max_length=256, pattern=r"^\S+$"
    )


ToolApprovalResponse = tool_policy.ToolApprovalResponse
ToolApprovalRequest = tool_policy.ToolApprovalRequest


class ToolResponse(pydantic.BaseModel):
    content: list[connect.ToolResultContentBlock]
    is_error: bool = False


class ToolExecutionError(Exception):
    """An explicit tool error, model-visible unless strict handling is enabled."""


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


class ToolSet(typing.Protocol):
    """Pure, immutable tool selection advanced only by completed round outcomes.

    Implementations must not perform I/O or mutate themselves in advance().
    Results include replayed outcomes; selection must be deterministic and return
    a new set so failed rounds and overlapping LLMs cannot leak activation state.
    """

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

    async def execute(
        self,
        context: ActivityExecutionContext,
        arguments: dict[str, pydantic.JsonValue],
    ) -> ToolResponse:
        raise RuntimeError("workflow tools must execute in workflow context")

    @abstractmethod
    async def execute_workflow(
        self,
        context: WorkflowExecutionContext,
        arguments: dict[str, pydantic.JsonValue],
    ) -> ToolResponse: ...


def _digest(value: pydantic.JsonValue) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(encoded.encode()).hexdigest()


async def invoke_tool(
    binding: typing.Optional[ToolBinding],
    request: connect.ToolCallBlock,
    *,
    instance: typing.Optional[str] = None,
    raise_errors: bool = False,
) -> connect.ToolResultMessage:
    """Validate, optionally await host approval, and execute a recorded tool call.

    Approval uses a JSON mailbox response with a strict ``approved`` boolean and
    optional ``reason`` and ``scope``. Workflow scope grants future calls by name
    within that workflow instance; the default scope approves only this call.
    The helper runs in workflow context; validation, the runtime policy gate,
    and implementation run in one activity for new workflows. Legacy histories
    retain binding-local approval. Tool exceptions return their unchanged text by default. Cancellation,
    replay mismatches, storage failures, and approval transport errors propagate.
    """
    if isinstance(binding, WorkflowToolBinding):
        return await _workflow_tool(
            binding, request.model_copy(deep=True), instance, raise_errors
        )
    if c.workflow_context().workflow.tool_policy_version is not None:
        return await _gated_tool(
            binding, request.model_copy(deep=True), instance, raise_errors
        )
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
    except Exception as exc:
        error = str(exc)

    if error is None and approval is not None:
        answer = await _request_approval(
            approval.channel,
            payload={
                "instance": instance,
                "call": request.model_dump(mode="json"),
                "tool": definition.model_dump(mode="json") if definition else None,
                "tool_version": binding.version if binding is not None else None,
            },
        )
        try:
            decision = ToolApprovalResponse.model_validate(answer)
            if not decision.approved:
                error = decision.reason or "Tool call rejected by host"
        except pydantic.ValidationError as exc:
            error = str(exc)

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
            **({"raise_errors": True} if raise_errors else {}),
        },
        raise_errors,
    )


@c.activity_wrapper(name="request_input", kind=runtime_mailbox.KIND)
async def _request_approval(
    call: c.ActivityCall, channel: str, payload: pydantic.JsonValue
) -> pydantic.JsonValue:
    """Preserve legacy activity identity while typing newly registered approvals."""

    async def invoke(ctx: ActivityExecutionContext) -> pydantic.JsonValue:
        if ctx.mailbox is None:
            raise RuntimeError("tool approval requires a persistent workflow runner")
        existing = await ctx.mailbox.existing(ctx.step_id)
        spec = (
            existing.specification()
            if existing is not None
            else ToolApprovalRequest.model_validate(payload).input_spec(channel)
        )
        return await ctx.mailbox.wait(ctx.step_id, spec)

    return await call.run(
        input={"channel": channel, "payload": payload, "response_type": "json"},
        invoke=invoke,
    )


@c.activity_wrapper(name="invoke_tool", kind="llm.tool", version=2)
async def _execute_tool(
    call: c.ActivityCall,
    binding: typing.Optional[ToolBinding],
    request: connect.ToolCallBlock,
    error: typing.Optional[str],
    input: dict[str, pydantic.JsonValue],
    raise_errors: bool,
) -> connect.ToolResultMessage:
    async def invoke(ctx: ActivityExecutionContext) -> pydantic.JsonValue:
        try:
            if error is not None:
                raise ToolExecutionError(error)
            if binding is None:
                raise ToolExecutionError(f"unknown tool: {request.name}")
            response = await binding.execute(ctx, request.arguments)
            result = connect.ToolResultMessage(
                tool_call_id=request.id,
                tool_name=request.name,
                content=response.content,
                is_error=response.is_error,
            ).model_dump(mode="json")
            if response.is_error:
                diagnostics.log(
                    "llm.tool_error",
                    level=diagnostics.Level.ERROR,
                    fields={"tool": request.name, "call_id": request.id},
                )
            return result
        except Exception as exc:
            diagnostics.log(
                "llm.tool_failed",
                level=diagnostics.Level.ERROR,
                fields={"tool": request.name, "call_id": request.id},
                error=exc,
            )
            if raise_errors:
                raise
            return connect.ToolResultMessage(
                tool_call_id=request.id,
                tool_name=request.name,
                content=[connect.TextBlock(text=str(exc))],
                is_error=True,
            ).model_dump(mode="json")

    result = await call.run(input=input, invoke=invoke)
    return connect.ToolResultMessage.model_validate(result)


@c.activity_wrapper(name="invoke_tool", kind="llm.tool", version=3)
async def _gated_tool(
    call: c.ActivityCall,
    binding: typing.Optional[ToolBinding],
    request: connect.ToolCallBlock,
    instance: typing.Optional[str],
    raise_errors: bool,
    gate_only: bool = False,
) -> connect.ToolResultMessage:
    """Gate only live tool execution; replay returns the recorded result directly."""
    project = c.workflow_context().project
    definition = binding.definition if binding is not None else None
    digest = _digest(definition.model_dump(mode="json")) if definition else None

    async def invoke(ctx: ActivityExecutionContext) -> pydantic.JsonValue:
        error: typing.Optional[Exception] = None
        decision = tool_policy.Decision(action="require_confirmation")
        existing = (
            await ctx.mailbox.existing(ctx.step_id) if ctx.mailbox is not None else None
        )
        try:
            if binding is None:
                raise ToolExecutionError(f"unknown tool: {request.name}")
            if definition is None or definition.name != request.name:
                raise ToolExecutionError(f"tool binding does not match: {request.name}")
            binding.validate(request.arguments)
            if existing is None:
                decision = await project.tool_policy.evaluate(
                    project.config_path,
                    tool_policy.ToolCall(
                        name=request.name,
                        version=binding.version,
                        arguments=request.arguments,
                        definition_digest=digest or "",
                        execution_id=ctx.execution_id,
                        workflow_id=ctx.workflow_id,
                        step_id=ctx.step_id,
                    ),
                )
            if decision.action == "deny":
                raise ToolExecutionError(
                    decision.reason or "Tool call denied by policy"
                )
        except Exception as exc:
            error = exc

        if (
            error is None
            and existing is None
            and decision.action == "require_confirmation"
            and ctx.mailbox is not None
            and await ctx.mailbox.tool_approved(ctx.workflow_id, request.name)
        ):
            decision = tool_policy.Decision(action="allow")

        if error is None and decision.action == "require_confirmation":
            if ctx.mailbox is None:
                raise RuntimeError(
                    "tool approval requires a persistent workflow runner"
                )
            approval = binding.approval if binding is not None else None
            if existing is not None:
                spec = existing.specification()
            else:
                assert binding is not None and definition is not None
                spec = ToolApprovalRequest(
                    instance=instance,
                    call=request,
                    tool=definition,
                    tool_version=binding.version,
                ).input_spec(approval.channel if approval else "tool.approval")
            answer = await ctx.mailbox.wait(ctx.step_id, spec)
            try:
                response = ToolApprovalResponse.model_validate(answer)
                if not response.approved:
                    raise ToolExecutionError(
                        response.reason or "Tool call rejected by host"
                    )
            except Exception as exc:
                error = exc

        try:
            if error is not None:
                raise error
            assert binding is not None
            if gate_only:
                return connect.ToolResultMessage(
                    tool_call_id=request.id,
                    tool_name=request.name,
                    content=[],
                ).model_dump(mode="json")
            result = await binding.execute(ctx, request.arguments)
            if result.is_error:
                diagnostics.log(
                    "llm.tool_error",
                    level=diagnostics.Level.ERROR,
                    fields={"tool": request.name, "call_id": request.id},
                )
            return connect.ToolResultMessage(
                tool_call_id=request.id,
                tool_name=request.name,
                content=result.content,
                is_error=result.is_error,
            ).model_dump(mode="json")
        except Exception as exc:
            diagnostics.log(
                "llm.tool_failed",
                level=diagnostics.Level.ERROR,
                fields={"tool": request.name, "call_id": request.id},
                error=exc,
            )
            if raise_errors:
                raise
            return connect.ToolResultMessage(
                tool_call_id=request.id,
                tool_name=request.name,
                content=[connect.TextBlock(text=str(exc))],
                is_error=True,
            ).model_dump(mode="json")

    result = await call.run(
        input={
            "instance": instance,
            "call_digest": _digest(request.model_dump(mode="json")),
            "tool_digest": digest,
            "tool_version": binding.version if binding is not None else None,
            **({"gate_only": True} if gate_only else {}),
            **({"raise_errors": True} if raise_errors else {}),
        },
        invoke=invoke,
    )
    return connect.ToolResultMessage.model_validate(result)


async def _workflow_tool(
    binding: WorkflowToolBinding,
    request: connect.ToolCallBlock,
    instance: typing.Optional[str],
    raise_errors: bool,
) -> connect.ToolResultMessage:
    """Replay the approval gate while leaving child operations in workflow history."""
    gate = await _gated_tool(binding, request, instance, raise_errors, gate_only=True)
    if gate.is_error:
        return gate
    try:
        result = await binding.execute_workflow(c.workflow_context(), request.arguments)
    except (ActivityError, ToolExecutionError) as exc:
        if raise_errors:
            raise
        result = ToolResponse(content=[connect.TextBlock(text=str(exc))], is_error=True)
    return connect.ToolResultMessage(
        tool_call_id=request.id,
        tool_name=request.name,
        content=result.content,
        is_error=result.is_error,
    )
