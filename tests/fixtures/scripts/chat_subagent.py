import connect
import pydantic
from vectorops.runner.api import llm


async def main(ctx: object) -> pydantic.JsonValue:
    tool = await llm.subagent_tool(["builtin:chat"])
    return (
        await llm.invoke_tool(
            tool,
            connect.ToolCallBlock(
                id="chat", name="run_agent", arguments={"reference": "builtin:chat"}
            ),
        )
    ).model_dump(mode="json")
