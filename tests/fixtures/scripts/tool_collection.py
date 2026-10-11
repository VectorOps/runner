import connect
import pydantic

from vectorops.runner.api import tools


async def main(ctx: object) -> pydantic.JsonValue:
    bindings = await tools.load_collection("remote")
    assert len(bindings) == 1
    result = await tools.invoke_tool(
        bindings[0],
        connect.ToolCallBlock(
            id="echo", name=bindings[0].definition.name, arguments={"value": "hello"}
        ),
    )
    return result.model_dump(mode="json")
