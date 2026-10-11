import pydantic

from vectorops.runner.api.mailbox import request_input


async def main(ctx: object) -> pydantic.JsonValue:
    return await request_input(
        "test",
        {"prompt": "Answer?"},
        response_type="json",
        request_type="test.v1",
        request_schema={"type": "object"},
        response_schema={"type": "integer"},
    )
