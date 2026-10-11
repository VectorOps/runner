"""Workflow-side approval and durable execution of tool capabilities."""

import hashlib
import json
import typing

import connect
import pydantic

from vectorops.runner import diagnostics
from vectorops.runner.authentication.models import AuthenticationError
from vectorops.runner.api import composition as c
from vectorops.runner.api import tool_contracts, tool_approval
from vectorops.runner.runner import mailbox as runtime_mailbox
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.errors import ActivityError

Tool = tool_contracts.Tool
ToolBinding = tool_contracts.ToolBinding
ToolSet = tool_contracts.ToolSet
ActivityToolBinding = tool_contracts.ActivityToolBinding
WorkflowToolBinding = tool_contracts.WorkflowToolBinding
ToolApproval = tool_contracts.ToolApproval
ToolApprovalRequest = tool_contracts.ToolApprovalRequest
ToolApprovalResponse = tool_contracts.ToolApprovalResponse
ToolExecutionError = tool_contracts.ToolExecutionError
ToolResponse = tool_contracts.ToolResponse


def _result(
    request: connect.ToolCallBlock, response: ToolResponse
) -> connect.ToolResultMessage:
    return connect.ToolResultMessage(
        tool_call_id=request.id,
        tool_name=request.name,
        content=response.content,
        is_error=response.is_error,
    )


def _failure(
    request: connect.ToolCallBlock, error: Exception
) -> connect.ToolResultMessage:
    return _result(
        request,
        ToolResponse(content=[connect.TextBlock(text=str(error))], is_error=True),
    )


def _log_result(request: connect.ToolCallBlock, response: ToolResponse) -> None:
    if response.is_error:
        diagnostics.log(
            "llm.tool_error",
            level=diagnostics.Level.ERROR,
            fields={"tool": request.name, "call_id": request.id},
        )


def _log_failure(request: connect.ToolCallBlock, error: Exception) -> None:
    diagnostics.log(
        "llm.tool_failed",
        level=diagnostics.Level.ERROR,
        fields={"tool": request.name, "call_id": request.id},
        error=error,
    )


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
    binding: typing.Optional[ActivityToolBinding],
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
            _log_result(request, response)
            return _result(request, response).model_dump(mode="json")
        except AuthenticationError:
            raise
        except Exception as exc:
            _log_failure(request, exc)
            if raise_errors:
                raise
            return _failure(request, exc).model_dump(mode="json")

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
        error = await tool_approval.authorize(
            project,
            ctx,
            binding,
            request,
            definition,
            digest,
            instance,
        )
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
            if isinstance(binding, WorkflowToolBinding):
                raise RuntimeError("workflow tools require workflow execution")
            result = await binding.execute(ctx, request.arguments)
            _log_result(request, result)
            return _result(request, result).model_dump(mode="json")
        except AuthenticationError:
            raise
        except Exception as exc:
            _log_failure(request, exc)
            if raise_errors:
                raise
            return _failure(request, exc).model_dump(mode="json")

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
        return _failure(request, exc)
    return _result(request, result)
