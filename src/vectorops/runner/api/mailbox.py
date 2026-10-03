"""Built-in durable input activity, independent of host presentation or transport."""

import typing

import pydantic

from vectorops.runner.api.composition import activity
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner import mailbox


@activity(kind=mailbox.KIND)
async def request_input(
    ctx: ActivityExecutionContext,
    channel: str,
    payload: pydantic.JsonValue = None,
    *,
    response_type: typing.Literal["text", "json"] = "text",
) -> pydantic.JsonValue:
    """Wait for a host to answer this recorded request on the named channel."""
    spec = mailbox.InputSpec(
        channel=channel, payload=payload, response_type=response_type
    )
    if ctx.mailbox is None:
        raise RuntimeError("input requests require a persistent workflow runner")
    return await ctx.mailbox.wait(ctx.step_id, spec)
