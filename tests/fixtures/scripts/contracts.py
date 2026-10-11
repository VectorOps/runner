from collections.abc import AsyncGenerator

import pydantic

from vectorops.runner.api import composition as c


@c.activity(namespace="customer.acme", version=2)
async def prepare(ctx: object, text: pydantic.JsonValue) -> pydantic.JsonValue:
    return text


@c.streamable_activity(kind="vocode.llm.invoke", version=3)
async def invoke(
    ctx: object, text: pydantic.JsonValue
) -> AsyncGenerator[c.ActivityDelta | c.ActivityFinal[pydantic.JsonValue], None]:
    yield c.delta({"text": "partial"})
    yield c.delta({"text": text})
    yield c.final({"text": text})


@c.workflow(namespace="customer.acme", version=4)
async def turn(ctx: object, text: pydantic.JsonValue) -> pydantic.JsonValue:
    return await invoke(await prepare(text))


@c.workflow(kind="customer.acme.chat", version=2)
async def workflow(ctx: object) -> pydantic.JsonValue:
    return await turn("hello")
