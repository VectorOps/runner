from vectorops.runner.api.composition import workflow


@workflow(name="Double", description="Double a number", kind="math.double", version=2)
async def double(ctx: object, value: int, scale: int = 2) -> int:
    return value * scale


async def plain(ctx: object, text: str = "hello") -> str:
    return text
