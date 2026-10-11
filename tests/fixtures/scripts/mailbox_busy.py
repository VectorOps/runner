import asyncio


async def busy(ctx: object) -> None:
    await asyncio.sleep(60)
