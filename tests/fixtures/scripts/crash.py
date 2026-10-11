import asyncio
from pathlib import Path
from vectorops.runner.api.composition import activity
from vectorops.runner.runner.activity_context import ActivityExecutionContext


@activity()
async def blocked(ctx: ActivityExecutionContext) -> int:
    workspace = Path(__file__).parent.parent
    (workspace / "identity").write_text(ctx.step_id)
    while not (workspace / "release").exists():
        await asyncio.sleep(0.01)
    return 42


async def workflow(ctx: object) -> int:
    return await blocked()
