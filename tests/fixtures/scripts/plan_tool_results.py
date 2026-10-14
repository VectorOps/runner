from vectorops.runner.api import tools
from vectorops.runner.runner.context import WorkflowExecutionContext
import connect
import pydantic


async def main(ctx: WorkflowExecutionContext) -> pydantic.JsonValue:
    binding = tools.update_plan_tool()
    results: list[pydantic.JsonValue] = []
    for identifier, arguments in (
        (
            "first",
            {
                "merge": False,
                "todos": [
                    {"id": "a", "title": "First", "status": "in_progress"},
                    {"id": "b", "title": "Second", "status": "pending"},
                ],
            },
        ),
        ("second", {"todos": [{"id": "a", "status": "completed"}]}),
    ):
        result = await tools.invoke_tool(
            binding,
            connect.ToolCallBlock(
                id=identifier, name="update_plan", arguments=arguments
            ),
        )
        results.append(result.model_dump(mode="json"))
    return results
