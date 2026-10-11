from vectorops.runner.api import llm


async def main(ctx: object, name: str) -> str:
    if name == "run_agent":
        tool = await llm.subagent_tool(["child:double"])
    else:
        tool = await llm.subagent_tool(["child:double"], name=name)
    return tool.definition.name
