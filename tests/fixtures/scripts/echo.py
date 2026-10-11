import pydantic


async def workflow(ctx: object, value: pydantic.JsonValue) -> pydantic.JsonValue:
    return value
