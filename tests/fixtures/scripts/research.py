from vectorops.runner.api.composition import workflow


@workflow(description="Custom researcher")
async def research(ctx: object, task: str) -> str:
    return "evidence"
