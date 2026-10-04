"""Workflows exercising root-owned resources and shared target attachments."""

import asyncio
from dataclasses import dataclass, field
import sys

from vectorops.runner import models
from vectorops.runner.environment.processes import models as processes
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.context import WorkflowExecutionContext


@dataclass
class Probe:
    contexts: list[ActivityExecutionContext] = field(default_factory=list)
    events: list[str] = field(default_factory=list)
    started: asyncio.Queue[int] = field(default_factory=asyncio.Queue)
    gate: asyncio.Event = field(default_factory=asyncio.Event)
    waiting: asyncio.Event = field(default_factory=asyncio.Event)


probe = Probe()


async def launch(ctx: ActivityExecutionContext) -> str:
    assert ctx.environment is not None and ctx.resources is not None
    probe.contexts.append(ctx)

    async def cleanup() -> None:
        assert ctx.environment is not None
        assert await ctx.environment.list_processes()
        probe.events.append("resource.closed")

    ctx.resources.register("test resource", cleanup)
    result = await ctx.environment.exec(
        processes.ExecRequest(
            command=processes.ArgvCommand(
                argv=[
                    sys.executable,
                    "-u",
                    "-c",
                    "import os, time; print(os.getpid(), flush=True); time.sleep(60)",
                ]
            ),
            yield_timeout_ms=0,
        )
    )
    data = b""
    while b"\n" not in data:
        result = await ctx.environment.poll(
            processes.PollRequest(
                process_id=result.process_id, cursor=result.cursor, wait_timeout_ms=100
            )
        )
        data += b"".join(chunk.data for chunk in result.chunks)
    await probe.started.put(int(data.strip()))
    return result.process_id


async def child(ctx: WorkflowExecutionContext) -> str:
    result = await ctx.run_activity(
        models.StepOperation(kind="activity", name="launch"), launch
    )
    assert isinstance(result, str)
    return result


async def verify_shared(ctx: ActivityExecutionContext) -> None:
    assert ctx.environment is probe.contexts[0].environment
    assert ctx.resources is probe.contexts[0].resources
    assert ctx.environment is not None and await ctx.environment.list_processes()
    assert probe.events == []


async def workflow(ctx: WorkflowExecutionContext, mode: str = "success") -> str:
    if mode == "child":
        handle = await ctx.spawn_workflow(
            "python:tests.fixtures.resource_workflows", "child"
        )
        await ctx.join_workflows([handle])
        await ctx.run_activity(
            models.StepOperation(kind="activity", name="verify"), verify_shared
        )
    else:
        await child(ctx)
    if mode == "failure":
        raise ValueError("root failed")
    if mode == "wait":
        try:
            probe.waiting.set()
            await probe.gate.wait()
        finally:
            probe.events.append("workflow.stopped")
    return "done"
