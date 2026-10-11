"""Built-in durable input activity, independent of host presentation or transport."""

import typing

import pydantic

from vectorops.runner import input_schema
from vectorops.runner.api import composition as c
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner import mailbox


async def request_input(
    channel: str,
    payload: pydantic.JsonValue = None,
    *,
    response_type: typing.Literal["text", "json"] = "text",
    request_type: typing.Optional[str] = None,
    request_schema: typing.Optional[input_schema.Schema] = None,
    response_schema: typing.Optional[input_schema.Schema] = None,
) -> pydantic.JsonValue:
    """Validate both directions using optional durable JSON Schema 2020-12 contracts."""
    invoke = (
        _typed_input
        if request_schema is not None or response_schema is not None
        else _untyped_input
    )
    return await invoke(
        channel, payload, response_type, request_type, request_schema, response_schema
    )


async def _request_input(
    call: c.ActivityCall,
    channel: str,
    payload: pydantic.JsonValue,
    response_type: typing.Literal["text", "json"],
    request_type: typing.Optional[str],
    request_schema: typing.Optional[input_schema.Schema],
    response_schema: typing.Optional[input_schema.Schema],
) -> pydantic.JsonValue:
    arguments: dict[str, pydantic.JsonValue] = {
        "channel": channel,
        "payload": payload,
        "response_type": response_type,
    }
    if request_type is not None:
        arguments["request_type"] = request_type
    if request_schema is not None or response_schema is not None:
        arguments["contract_digest"] = mailbox.InputSpec.contract_digest(
            request_schema, response_schema
        )

    async def invoke(ctx: ActivityExecutionContext) -> pydantic.JsonValue:
        spec = mailbox.InputSpec(
            channel=channel,
            payload=payload,
            response_type=response_type,
            request_type=request_type,
            request_schema=request_schema,
            response_schema=response_schema,
        )
        if ctx.mailbox is None:
            raise RuntimeError("input requests require a persistent workflow runner")
        return await ctx.mailbox.wait(ctx.step_id, spec)

    return await call.run(input=arguments, invoke=invoke)


_untyped_input = c.activity_wrapper(name="request_input", kind=mailbox.KIND)(
    _request_input
)
_typed_input = c.activity_wrapper(name="request_input", kind=mailbox.KIND, version=2)(
    _request_input
)
