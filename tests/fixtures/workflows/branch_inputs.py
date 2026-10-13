import pydantic

from vectorops.runner.api.mailbox import request_input


async def structured(ctx: object) -> pydantic.JsonValue:
    return await request_input(
        "chat",
        response_type="json",
        response_schema={
            "type": "object",
            "properties": {"text": {"type": "string"}},
            "required": ["text"],
            "additionalProperties": False,
        },
    )


async def two_inputs(ctx: object) -> pydantic.JsonValue:
    first = await request_input("chat")
    second = await request_input("chat", payload=first)
    return [first, second]
