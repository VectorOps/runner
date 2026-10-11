from collections.abc import AsyncGenerator

from vectorops.runner.api import composition as c


@c.streamable_activity()
async def progress(
    ctx: object,
) -> AsyncGenerator[c.ActivityDelta | c.ActivityFinal[str], None]:
    yield c.delta("first")
    yield c.delta("second")
    yield c.final("done")


async def main(ctx: object) -> str:
    return await progress()
