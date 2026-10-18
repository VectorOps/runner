"""Workflow-side approval and durable execution of tool capabilities."""

import typing

import connect
import pydantic

from vectorops.runner import diagnostics
from vectorops.runner import global_config
from vectorops.runner import models
from vectorops.runner.authentication.models import (
    AuthenticationError,
    CredentialStorageError,
)
from vectorops.runner.api import composition as c
from vectorops.runner.api import tool_contracts
from vectorops.runner.tool_authorization import (
    ToolAuthorizationService,
    current,
    digest as _digest,
)
from vectorops.runner.runner import mailbox as runtime_mailbox
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner import errors as runtime_errors

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


async def invoke_tool(
    binding: typing.Optional[ToolBinding],
    request: connect.ToolCallBlock,
    *,
    instance: typing.Optional[str] = None,
    raise_errors: bool = False,
) -> connect.ToolResultMessage:
    """Validate, optionally await host approval, and execute a recorded tool call.

    Approval responses carry a decision, scope, and optional property mask.
    Workflow and workspace scopes create rules; call scope affects only this call.
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
        ToolAuthorizationService.validate(binding, request, definition)
    except Exception as exc:
        error = str(exc)

    if error is None and approval is not None:
        answer = await _request_approval(
            approval.channel,
            binding=binding,
            payload={
                "instance": instance,
                "call": request.model_dump(mode="json"),
                "tool": definition.model_dump(mode="json") if definition else None,
                "tool_version": binding.version if binding is not None else None,
            },
        )
        try:
            ToolAuthorizationService.validate_response(answer)
        except (pydantic.ValidationError, ToolExecutionError) as exc:
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
    call: c.ActivityCall,
    channel: str,
    payload: pydantic.JsonValue,
    binding: typing.Optional[ToolBinding] = None,
) -> pydantic.JsonValue:
    """Preserve legacy activity identity while typing newly registered approvals."""

    async def invoke(ctx: ActivityExecutionContext) -> pydantic.JsonValue:
        return await ToolAuthorizationService.request_approval(
            ctx, channel, payload, binding
        )

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
        except (AuthenticationError, runtime_errors.AuthorizationPersistenceError):
            raise
        except Exception as exc:
            _log_failure(request, exc)
            if raise_errors:
                raise
            return _failure(request, exc).model_dump(mode="json")
        await ToolAuthorizationService.record_authorization(ctx)
        return (await _execute_binding(binding, ctx, request, raise_errors)).model_dump(
            mode="json"
        )

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
    result = await _run_gated_tool(
        call, binding, request, instance, raise_errors, gate_only
    )
    return connect.ToolResultMessage.model_validate(result)


async def _run_gated_tool(
    call: c.ActivityCall,
    binding: typing.Optional[ToolBinding],
    request: connect.ToolCallBlock,
    instance: typing.Optional[str],
    raise_errors: bool,
    gate_only: bool,
) -> pydantic.JsonValue:
    definition = binding.definition if binding is not None else None
    digest = _digest(definition.model_dump(mode="json")) if definition else None

    async def invoke(ctx: ActivityExecutionContext) -> pydantic.JsonValue:
        denial = await _authorize_call(ctx, binding, request, instance, raise_errors)
        if denial is not None:
            return denial.model_dump(mode="json")
        if gate_only:
            return connect.ToolResultMessage(
                tool_call_id=request.id, tool_name=request.name, content=[]
            ).model_dump(mode="json")
        return (await _execute_binding(binding, ctx, request, raise_errors)).model_dump(
            mode="json"
        )

    return await call.run(
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


async def _authorize_call(
    context: ActivityExecutionContext,
    binding: typing.Optional[ToolBinding],
    request: connect.ToolCallBlock,
    instance: typing.Optional[str],
    raise_errors: bool,
) -> typing.Optional[connect.ToolResultMessage]:
    authorization = await current().authorize(context, binding, request, instance)
    error = authorization.error
    if error is not None:
        _log_failure(request, error)
        if raise_errors:
            raise error
        return _failure(request, error)
    return None


async def _execute_binding(
    binding: typing.Optional[ToolBinding],
    context: typing.Optional[ActivityExecutionContext],
    request: connect.ToolCallBlock,
    raise_errors: bool,
) -> connect.ToolResultMessage:
    try:
        if binding is None:
            raise ToolExecutionError(f"unknown tool: {request.name}")
        if isinstance(binding, WorkflowToolBinding):
            response = await binding.execute_workflow(
                c.workflow_context(), request.arguments
            )
        else:
            if context is None:
                raise RuntimeError("activity tools require an activity context")
            response = await binding.execute(context, request.arguments)
        _log_result(request, response)
        return _result(request, response)
    except (
        AuthenticationError,
        CredentialStorageError,
        global_config.ConfigurationError,
        runtime_errors.AuthorizationPersistenceError,
        runtime_errors.ReplayMismatchError,
    ):
        raise
    except Exception as exc:
        _log_failure(request, exc)
        if raise_errors:
            raise
        return _failure(request, exc)


async def _workflow_tool(
    binding: WorkflowToolBinding,
    request: connect.ToolCallBlock,
    instance: typing.Optional[str],
    raise_errors: bool,
) -> connect.ToolResultMessage:
    """Record one tool invocation while replaying its workflow implementation."""
    context = c.workflow_context()
    definition = binding.definition
    digest = _digest(definition.model_dump(mode="json"))
    legacy = False

    def replay_when(step: models.WorkflowStep) -> bool:
        nonlocal legacy
        legacy = (
            isinstance(step.operation, models.StepOperation)
            and step.operation.input.get("gate_only") is True
        )
        return not legacy and (
            step.outcome.kind == "pending" or step.authorized is True
        )

    async def invoke(
        ctx: ActivityExecutionContext, step: models.WorkflowStep
    ) -> pydantic.JsonValue:
        if (
            isinstance(step.operation, models.StepOperation)
            and step.operation.input.get("gate_only") is True
        ):
            denial = await _authorize_call(
                ctx, binding, request, instance, raise_errors
            )
            if denial is not None:
                return denial.model_dump(mode="json")
            return connect.ToolResultMessage(
                tool_call_id=request.id, tool_name=request.name, content=[]
            ).model_dump(mode="json")
        if step.outcome.kind == "pending":
            denial = await _authorize_call(
                ctx, binding, request, instance, raise_errors
            )
            if denial is not None:
                return denial.model_dump(mode="json")
            return (
                await _execute_binding(binding, ctx, request, raise_errors)
            ).model_dump(mode="json")
        try:
            await _execute_binding(binding, ctx, request, raise_errors)
        except (
            AuthenticationError,
            CredentialStorageError,
            global_config.ConfigurationError,
            runtime_errors.AuthorizationPersistenceError,
            runtime_errors.ReplayMismatchError,
        ):
            raise
        except Exception:
            if step.outcome.kind != "failed":
                raise
        if step.outcome.kind == "failed":
            raise runtime_errors.ActivityError(step.outcome.error)
        if step.outcome.kind != "completed":
            raise runtime_errors.ReplayMismatchError("invalid recorded tool outcome")
        return step.outcome.result

    recorded = await c.run_recorded_invocation(
        models.StepOperation(
            kind="llm.tool",
            name="invoke_tool",
            version=3,
            input={
                "instance": instance,
                "call_digest": _digest(request.model_dump(mode="json")),
                "tool_digest": digest,
                "tool_version": binding.version,
                **({"raise_errors": True} if raise_errors else {}),
            },
        ),
        invoke,
        replay_when=replay_when,
    )
    result = connect.ToolResultMessage.model_validate(recorded)
    if legacy and not result.is_error:
        return await _execute_binding(binding, None, request, raise_errors)
    return result
