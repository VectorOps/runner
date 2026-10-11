from vectorops.runner.api.composition import workflow


@workflow()
async def child(ctx: object, value: int = 1) -> int:
    return value
