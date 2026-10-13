import connect
import pydantic
from vectorops.runner.api import llm


async def main(ctx: object) -> pydantic.JsonValue:
    tool = await llm.subagent_tool(["builtin:agent"])
    return (
        await llm.invoke_tool(
            tool,
            connect.ToolCallBlock(
                id="agent", name="run_agent", arguments={"reference": "builtin:agent"}
            ),
        )
    ).model_dump(mode="json")
