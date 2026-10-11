import connect
import pydantic
import typing

from vectorops.runner.api import tools


async def main(
    ctx: object,
    reference: str = "child:double",
    value: int = 3,
    description: typing.Optional[str] = None,
) -> pydantic.JsonValue:
    tool = await tools.subagent_tool(["child:double"])
    result = await tools.invoke_tool(
        tool,
        connect.ToolCallBlock(
            id="call",
            name="run_agent",
            arguments={
                "reference": reference,
                "input": {"value": value},
                **({"description": description} if description is not None else {}),
            },
        ),
    )
    return result.model_dump(mode="json")
