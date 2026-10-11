import connect
import pydantic

from vectorops.runner.api import tools


async def main(
    ctx: object, reference: str = "child:double", value: int = 3
) -> pydantic.JsonValue:
    tool = await tools.subagent_tool(["child:double"])
    result = await tools.invoke_tool(
        tool,
        connect.ToolCallBlock(
            id="call",
            name="run_agent",
            arguments={"reference": reference, "input": {"value": value}},
        ),
    )
    return result.model_dump(mode="json")
