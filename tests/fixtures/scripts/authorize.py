from vectorops.runner.api.composition import activity
from vectorops.runner.runner.activity_context import ActivityExecutionContext


@activity()
async def authorize(ctx: ActivityExecutionContext) -> str:
    assert ctx.authentication is not None
    await ctx.authentication.transport(None, "chatgpt/X", execution_id=ctx.execution_id)
    return "authorized"


async def main(ctx: object) -> str:
    return await authorize()
