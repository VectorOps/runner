from vectorops.runner.api.composition import workflow


@workflow(description="Double a number")
async def double(ctx: object, value: int) -> int:
    if value < 0:
        raise ValueError("negative input")
    return value * 2
