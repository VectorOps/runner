"""Live tool authorization coordinated with durable mailbox confirmation."""

from typing import Optional

import connect

from vectorops.runner import tool_policy
from vectorops.runner.api import tool_contracts as contracts
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.project import RunnerProject


async def authorize(
    project: RunnerProject,
    context: ActivityExecutionContext,
    binding: Optional[contracts.ToolBinding],
    request: connect.ToolCallBlock,
    definition: Optional[connect.ToolSpec],
    definition_digest: Optional[str],
    instance: Optional[str],
) -> Optional[Exception]:
    """Return model-visible denial errors; let mailbox infrastructure failures propagate.

    Existing confirmation requests retain their original decision. Otherwise,
    validation and mandatory policy checks precede workflow grants and host input.
    """
    decision = tool_policy.Decision(action="require_confirmation")
    existing = (
        await context.mailbox.existing(context.step_id)
        if context.mailbox is not None
        else None
    )
    try:
        if binding is None:
            raise contracts.ToolExecutionError(f"unknown tool: {request.name}")
        if definition is None or definition.name != request.name:
            raise contracts.ToolExecutionError(
                f"tool binding does not match: {request.name}"
            )
        binding.validate(request.arguments)
        if existing is None:
            decision = await project.tool_policy.evaluate(
                project.configuration.get_validated(
                    "tools.approval", tool_policy.ApprovalSettings
                ),
                tool_policy.ToolCall(
                    name=request.name,
                    version=binding.version,
                    arguments=request.arguments,
                    definition_digest=definition_digest or "",
                    execution_id=context.execution_id,
                    workflow_id=context.workflow_id,
                    step_id=context.step_id,
                ),
            )
        if decision.action == "deny":
            raise contracts.ToolExecutionError(
                decision.reason or "Tool call denied by policy"
            )
    except Exception as exc:
        return exc

    if (
        existing is None
        and decision.action == "require_confirmation"
        and context.mailbox is not None
        and await context.mailbox.tool_approved(context.workflow_id, request.name)
    ):
        decision = tool_policy.Decision(action="allow")

    if decision.action == "require_confirmation":
        if context.mailbox is None:
            raise RuntimeError("tool approval requires a persistent workflow runner")
        approval = binding.approval if binding is not None else None
        if existing is not None:
            spec = existing.specification()
        else:
            assert binding is not None and definition is not None
            spec = tool_policy.ToolApprovalRequest(
                instance=instance,
                call=request,
                tool=definition,
                tool_version=binding.version,
            ).input_spec(approval.channel if approval else "tool.approval")
        answer = await context.mailbox.wait(context.step_id, spec)
        try:
            response = tool_policy.ToolApprovalResponse.model_validate(answer)
            if not response.approved:
                raise contracts.ToolExecutionError(
                    response.reason or "Tool call rejected by host"
                )
        except Exception as exc:
            return exc
    return None
