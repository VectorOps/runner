"""Workflow-local task plans reconstructed from recorded updates during replay."""

import json
from typing import Optional

import connect
import pydantic

from vectorops.runner.api import plan, tool_contracts as tools
from vectorops.runner.api.tool_registry import register_tool
from vectorops.runner.runner.context import WorkflowExecutionContext

Todo = plan.Todo
PlanArguments = plan.PlanArguments


class UpdatePlanTool(tools.WorkflowToolBinding):
    def __init__(self, *, approval: Optional[tools.ToolApproval] = None) -> None:
        self._approval = approval
        self._plan = plan.Plan()

    @property
    def definition(self) -> connect.ToolSpec:
        return connect.ToolSpec.external(
            name="update_plan",
            description="Update the current workflow's plan using stable todo ids. Merge updates existing ids "
            "without changing order and appends new tasks. New tasks and replacements require titles. "
            "At most one task may be in_progress. Reuse one binding per workflow plan.",
            input_schema=PlanArguments.model_json_schema(),
        )

    @property
    def version(self) -> int:
        return 1

    @property
    def approval(self) -> Optional[tools.ToolApproval]:
        return self._approval

    @property
    def todos(self) -> list[Todo]:
        return self._plan.todos

    def _validated(self, arguments: dict[str, pydantic.JsonValue]) -> PlanArguments:
        try:
            return PlanArguments.model_validate(arguments)
        except pydantic.ValidationError as exc:
            raise tools.ToolExecutionError(str(exc)) from exc

    def validate(self, arguments: dict[str, pydantic.JsonValue]) -> None:
        self._validated(arguments)

    async def execute_workflow(
        self,
        context: WorkflowExecutionContext,
        arguments: dict[str, pydantic.JsonValue],
    ) -> tools.ToolResponse:
        parsed = self._validated(arguments)
        try:
            snapshot = await self._plan.update(context, parsed)
        except plan.PlanError as exc:
            raise tools.ToolExecutionError(str(exc)) from exc
        return tools.ToolResponse(
            content=[
                connect.TextBlock(
                    text=json.dumps(
                        {"todos": [todo.model_dump(mode="json") for todo in snapshot]}
                    )
                )
            ]
        )


@register_tool("update_plan")
def update_plan_tool(
    *, approval: Optional[tools.ToolApproval] = None
) -> UpdatePlanTool:
    """Create one plan binding inside each workflow; reuse it across tool rounds."""
    return UpdatePlanTool(approval=approval)
