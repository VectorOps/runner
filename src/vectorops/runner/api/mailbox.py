"""Built-in durable input activity, independent of host presentation or transport."""

import typing

import pydantic

from vectorops.runner import input_schema
from vectorops.runner.api import composition as c
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner import mailbox


@c.activity_wrapper(kind=mailbox.KIND)
async def request_input(
    call: c.ActivityCall,
    channel: str,
    payload: pydantic.JsonValue = None,
    *,
    response_type: typing.Literal["text", "json"] = "text",
    request_type: typing.Optional[str] = None,
    request_schema: typing.Optional[input_schema.Schema] = None,
    response_schema: typing.Optional[input_schema.Schema] = None,
) -> pydantic.JsonValue:
    """Validate both directions using optional durable JSON Schema 2020-12 contracts."""
    arguments: dict[str, pydantic.JsonValue] = {
        "channel": channel,
        "payload": payload,
        "response_type": response_type,
    }
    for name, value in (
        ("request_type", request_type),
        ("request_schema", request_schema),
        ("response_schema", response_schema),
    ):
        if value is not None:
            arguments[name] = value

    async def invoke(ctx: ActivityExecutionContext) -> pydantic.JsonValue:
        spec = mailbox.InputSpec.model_validate(arguments)
        if ctx.mailbox is None:
            raise RuntimeError("input requests require a persistent workflow runner")
        return await ctx.mailbox.wait(ctx.step_id, spec)

    return await call.run(input=arguments, invoke=invoke)
