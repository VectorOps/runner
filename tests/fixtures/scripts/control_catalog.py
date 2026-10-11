import pydantic

from vectorops.runner.api.composition import workflow
from vectorops.runner.api.mailbox import request_input
from tests.fixtures.library_workflows import child as imported


@workflow(description="Wait in a child")
async def main(ctx: object) -> pydantic.JsonValue:
    return await child()


@workflow(description="Ask for text")
async def child(ctx: object) -> pydantic.JsonValue:
    return await request_input("ghidra.input", "Waiting for Ghidra input...")


async def plain(ctx: object) -> int:
    return 42


@workflow()
async def approval(ctx: object) -> pydantic.JsonValue:
    return await request_input("approval", "Approve?", response_type="json")
