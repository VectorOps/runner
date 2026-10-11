import asyncio
from pathlib import Path
from vectorops.runner.api.composition import activity
from vectorops.runner.runner.activity_context import ActivityExecutionContext


@activity()
async def work(ctx: ActivityExecutionContext, name: str) -> str:
    print(f"started:{name}:{ctx.step_id}", flush=True)
    if name == "A":
        release = Path(__file__).parent.parent / "release-a"
        while not await asyncio.to_thread(release.exists):
            await asyncio.sleep(0.01)
    return name


async def main(ctx: object) -> list[str]:
    a = asyncio.create_task(work("A"))
    b = asyncio.create_task(work("B"))
    result_b = await b
    print("delivered:B", flush=True)
    return [await a, result_b]
