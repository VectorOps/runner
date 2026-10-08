"""Workflow-local task plans reconstructed from recorded updates during replay."""

import asyncio
import json
from typing import Literal, Optional

import connect
import pydantic

from vectorops.runner.api import composition as c
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.context import WorkflowExecutionContext
from . import tools


class Todo(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True, strict=True)

    id: str = pydantic.Field(min_length=1, pattern=r"\S")
    status: Literal["pending", "in_progress", "completed"]
    title: Optional[str] = pydantic.Field(default=None, min_length=1, pattern=r"\S")


class PlanArguments(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", strict=True)

    todos: list[Todo] = pydantic.Field(min_length=1)
    merge: bool = True

    @pydantic.model_validator(mode="after")
    def validate_todos(self) -> "PlanArguments":
        if len({todo.id for todo in self.todos}) != len(self.todos):
            raise ValueError("Todo ids must be unique")
        if not self.merge and any(todo.title is None for todo in self.todos):
            raise ValueError("Replacement todos require titles")
        return self


class UpdatePlanTool(tools.WorkflowToolBinding):
    def __init__(self, *, approval: Optional[tools.ToolApproval] = None) -> None:
        self._context = c.workflow_context()
        self._approval = approval
        self._todos: list[Todo] = []
        self._lock = asyncio.Lock()

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
        return list(self._todos)

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
        if context is not self._context:
            raise tools.ToolExecutionError(
                "Plan bindings cannot be shared across workflows"
            )
        parsed = self._validated(arguments)
        async with self._lock:
            updated = {todo.id: todo for todo in self._todos} if parsed.merge else {}
            for todo in parsed.todos:
                title = todo.title
                if title is None:
                    previous = updated.get(todo.id)
                    if previous is None:
                        raise tools.ToolExecutionError(
                            f"Title required for new todo: {todo.id}"
                        )
                    title = previous.title
                updated[todo.id] = todo.model_copy(update={"title": title})
            if sum(todo.status == "in_progress" for todo in updated.values()) > 1:
                raise tools.ToolExecutionError("Only one task can be in_progress")
            snapshot = await self._record(list(updated.values()))
            self._todos = snapshot
            return tools.ToolResponse(
                content=[
                    connect.TextBlock(
                        text=json.dumps(
                            {
                                "todos": [
                                    todo.model_dump(mode="json") for todo in snapshot
                                ]
                            }
                        )
                    )
                ]
            )

    @c.activity_wrapper(name="update_plan", kind="agent.plan", version=1)
    async def _record(self, call: c.ActivityCall, todos: list[Todo]) -> list[Todo]:
        payload: pydantic.JsonValue = [todo.model_dump(mode="json") for todo in todos]

        async def invoke(context: ActivityExecutionContext) -> pydantic.JsonValue:
            return payload

        snapshot = await call.run(input={"todos": payload}, invoke=invoke)
        return pydantic.TypeAdapter(list[Todo]).validate_python(snapshot)


def update_plan_tool(
    *, approval: Optional[tools.ToolApproval] = None
) -> UpdatePlanTool:
    """Create one plan binding inside each workflow; reuse it across tool rounds."""
    return UpdatePlanTool(approval=approval)
