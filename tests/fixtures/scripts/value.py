import pydantic


async def main(ctx: object, value: pydantic.JsonValue = 7) -> pydantic.JsonValue:
    return value
