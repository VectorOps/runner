from typing import Optional

import connect
import pydantic

from tests.fixtures import probe
from vectorops.runner.api import composition, tools
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.context import WorkflowExecutionContext


class Arguments(pydantic.BaseModel):
    value: int = 1


async def execute(ctx: ActivityExecutionContext, args: Arguments) -> tools.ToolResponse:
    probe.current.contexts.append(ctx)
    probe.current.calls.append(args.value)
    return tools.ToolResponse(content=[connect.TextBlock(text="done")])


@composition.activity(kind="test.safety.execute")
async def recorded_execute(
    ctx: ActivityExecutionContext, value: int
) -> pydantic.JsonValue:
    return (await execute(ctx, Arguments(value=value))).model_dump(mode="json")


class WorkflowTool(tools.WorkflowToolBinding):
    @property
    def definition(self) -> connect.ToolSpec:
        return connect.ToolSpec.external(
            name="inspect",
            description="Inspect a public resource",
            input_schema=Arguments.model_json_schema(),
        )

    @property
    def version(self) -> int:
        return 1

    @property
    def approval(self) -> Optional[tools.ToolApproval]:
        return None

    def validate(self, arguments: dict[str, pydantic.JsonValue]) -> None:
        Arguments.model_validate(arguments)

    async def execute_workflow(
        self, ctx: WorkflowExecutionContext, arguments: dict[str, pydantic.JsonValue]
    ) -> tools.ToolResponse:
        return tools.ToolResponse.model_validate(
            await recorded_execute(Arguments.model_validate(arguments).value)
        )


async def main(
    ctx: WorkflowExecutionContext, workflow_tool: bool = False
) -> pydantic.JsonValue:
    return (
        await tools.invoke_tool(
            (
                WorkflowTool()
                if workflow_tool
                else tools.Tool(
                    "inspect", "Inspect a public resource", Arguments, execute
                )
            ),
            connect.ToolCallBlock(id="inspect", name="inspect", arguments={"value": 1}),
        )
    ).model_dump(mode="json")
