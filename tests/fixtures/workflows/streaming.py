from collections.abc import AsyncGenerator
import typing

import pydantic

from tests.fixtures import probe
from vectorops.runner.api import composition as c
from vectorops.runner.runner.context import WorkflowExecutionContext


@c.streamable_activity(name="stream", kind="test.stream")
async def stream(
    mode: str = "normal", count: int = 2
) -> AsyncGenerator[c.ActivityDelta | c.ActivityFinal[pydantic.JsonValue], None]:
    probe.current.calls.append(count)
    try:
        for index in range(count):
            yield c.delta({"index": index})
            if mode == "blocked":
                probe.current.started.put_nowait(index)
                await probe.current.gate.wait()
        if mode == "missing_final":
            return
        if mode == "failure":
            raise ValueError("stream failed")
        if mode == "bad_delta":
            yield c.delta(typing.cast(pydantic.JsonValue, object()))
        if mode == "unsupported":
            yield typing.cast(c.ActivityDelta, object())
        if mode == "bad_final":
            yield c.final(typing.cast(pydantic.JsonValue, object()))
        elif mode == "null":
            yield c.final(None)
        else:
            yield c.final({"count": count})
        if mode == "duplicate_final":
            yield c.final("second")
        if mode == "after_final":
            yield c.delta("too late")
        if mode == "failure_after_final":
            raise ValueError("failed after final")
    finally:
        probe.current.cancelled += 1


async def workflow(
    ctx: WorkflowExecutionContext, mode: str = "normal", count: int = 2
) -> pydantic.JsonValue:
    return await stream(mode, count)


async def parent(ctx: WorkflowExecutionContext) -> pydantic.JsonValue:
    handle = await ctx.spawn_workflow("streaming")
    outcomes = await ctx.join_workflows([handle])
    if outcomes[0].kind != "completed":
        raise AssertionError("child failed")
    return outcomes[0].result
