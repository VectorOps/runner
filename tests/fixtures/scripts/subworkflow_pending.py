from vectorops.runner.api.composition import workflow
from vectorops.runner.api.mailbox import request_input


@workflow()
async def double(ctx: object, value: int) -> int:
    await request_input("continue", "Ready?")
    return value * 2
