import asyncio
from dataclasses import dataclass, field
import sys
from typing import Literal, Optional
from uuid import UUID

import connect
import pydantic

from vectorops.runner.api import composition, tools
from vectorops.runner.environment.processes import models as processes
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.context import WorkflowExecutionContext

REFERENCE = "python:tests.fixtures.worktree_cases:"


@dataclass
class Probe:
    contexts: dict[str, ActivityExecutionContext] = field(default_factory=dict)
    events: list[str] = field(default_factory=list)
    started: asyncio.Queue[str] = field(default_factory=asyncio.Queue)
    gates: dict[str, asyncio.Event] = field(default_factory=dict)
    cleanup_started: asyncio.Event = field(default_factory=asyncio.Event)
    cleanup_gate: asyncio.Event = field(default_factory=asyncio.Event)


current = Probe()


async def cwd(context: ActivityExecutionContext) -> str:
    assert context.environment is not None
    result = await context.environment.processes.exec(
        processes.ExecRequest(
            command=processes.ArgvCommand(
                argv=[sys.executable, "-c", "import os; print(os.getcwd())"]
            ),
            yield_timeout_ms=1000,
        )
    )
    output = b"".join(chunk.data for chunk in result.chunks)
    async with asyncio.timeout(5):
        while not result.output_complete:
            result = await context.environment.processes.poll(
                processes.PollRequest(
                    process_id=result.process_id,
                    cursor=result.cursor,
                    wait_timeout_ms=100,
                )
            )
            output += b"".join(chunk.data for chunk in result.chunks)
    assert result.returncode == 0
    return output.decode().strip()


@composition.activity(kind="test.worktree.observe")
async def observe(
    ctx: ActivityExecutionContext, label: str, block: bool = False
) -> str:
    assert ctx.environment is not None
    current.contexts[label] = ctx
    current.events.append(label)
    if block:
        current.started.put_nowait(label)
        await current.gates[label].wait()
    return str(ctx.environment.root)


async def leaf(
    ctx: WorkflowExecutionContext, label: str = "leaf", block: bool = False
) -> str:
    return await observe(label, block)


async def middle(
    ctx: WorkflowExecutionContext, grandchild: Optional[str] = None
) -> dict[str, str]:
    before = await observe("child-before")
    handle = await ctx.spawn_workflow(
        REFERENCE + "leaf", worktree_id=UUID(grandchild) if grandchild else None
    )
    outcome = (await ctx.join_workflows([handle]))[0]
    assert outcome.kind == "completed" and isinstance(outcome.result, str)
    after = await observe("child-after")
    return {"child-before": before, "leaf": outcome.result, "child-after": after}


async def matrix(
    ctx: WorkflowExecutionContext,
    child: Optional[str] = None,
    grandchild: Optional[str] = None,
) -> dict[str, str]:
    before = await observe("parent-before")
    handle = await ctx.spawn_workflow(
        REFERENCE + "middle",
        input={"grandchild": grandchild},
        worktree_id=UUID(child) if child else None,
    )
    outcome = (await ctx.join_workflows([handle]))[0]
    assert outcome.kind == "completed"
    children = pydantic.TypeAdapter(dict[str, str]).validate_python(outcome.result)
    after = await observe("parent-after")
    return {"parent-before": before, **children, "parent-after": after}


async def siblings(
    ctx: WorkflowExecutionContext, worktrees: list[str]
) -> list[pydantic.JsonValue]:
    await observe("parent-before")
    handles = [
        await ctx.spawn_workflow(
            REFERENCE + "leaf",
            input={"label": f"child-{index}", "block": True},
            worktree_id=UUID(identifier),
        )
        for index, identifier in enumerate(worktrees)
    ]
    outcomes = await ctx.join_workflows(handles)
    await observe("parent-after")
    return [outcome.result for outcome in outcomes if outcome.kind == "completed"]


async def waiting_parent(ctx: WorkflowExecutionContext, worktree_id: str) -> str:
    await observe("parent-before")
    handle = await ctx.spawn_workflow(
        REFERENCE + "leaf", input={"block": True}, worktree_id=UUID(worktree_id)
    )
    outcome = (await ctx.join_workflows([handle]))[0]
    assert outcome.kind == "completed" and isinstance(outcome.result, str)
    return outcome.result


@composition.activity(kind="test.worktree.parent-resource")
async def parent_resource(ctx: ActivityExecutionContext, after: bool = False) -> None:
    assert ctx.resources is not None
    label = "parent-after" if after else "parent-before"
    current.contexts[label] = ctx
    assert ctx.environment is not None and await cwd(ctx) == str(ctx.environment.root)
    current.events.append(label)
    if not after:

        async def cleanup() -> None:
            assert ctx.environment is not None and await cwd(ctx) == str(
                ctx.environment.root
            )
            current.events.append("parent-cleanup")

        ctx.resources.register("parent probe", cleanup)


@composition.activity(kind="test.worktree.child-resource")
async def child_resource(ctx: ActivityExecutionContext, mode: str) -> None:
    assert ctx.environment is not None and ctx.resources is not None
    current.contexts["child"] = ctx
    session = await ctx.environment.processes.exec(
        processes.ExecRequest(
            command=processes.ArgvCommand(
                argv=[
                    sys.executable,
                    "-u",
                    "-c",
                    "import time; print('ready'); time.sleep(60)",
                ]
            ),
            yield_timeout_ms=0,
        )
    )
    async with asyncio.timeout(5):
        output = b""
        while b"ready" not in output:
            session = await ctx.environment.processes.poll(
                processes.PollRequest(
                    process_id=session.process_id,
                    cursor=session.cursor,
                    wait_timeout_ms=100,
                )
            )
            output += b"".join(chunk.data for chunk in session.chunks)

    async def remaining_cleanup() -> None:
        current.events.append("child-remaining-cleanup")

    async def cleanup() -> None:
        assert ctx.environment is not None
        current.events.append("child-cleanup-enter")
        current.cleanup_started.set()
        if mode == "repeated_cancel":
            await current.cleanup_gate.wait()
        running = await ctx.environment.processes.list_processes()
        assert any(
            item.process_id == session.process_id and item.state == "running"
            for item in running
        )
        assert await cwd(ctx) == str(ctx.environment.root)
        current.events.append("child-cleanup-used-environment")
        if mode == "cleanup_failure":
            raise ValueError("child cleanup failed")

    ctx.resources.register("remaining child cleanup", remaining_cleanup)
    ctx.resources.register("child probe", cleanup)
    current.started.put_nowait("child")
    if mode == "failure":
        raise ValueError("child activity failed")
    if mode in ("cancel", "repeated_cancel"):
        await current.gates["child"].wait()


async def resource_child(ctx: WorkflowExecutionContext, mode: str) -> None:
    await child_resource(mode)


async def resource_parent(
    ctx: WorkflowExecutionContext, worktree_id: str, mode: str
) -> str:
    await parent_resource()
    handle = await ctx.spawn_workflow(
        REFERENCE + "resource_child",
        input={"mode": mode},
        worktree_id=UUID(worktree_id),
    )
    outcome = (await ctx.join_workflows([handle], failure_policy="collect"))[0]
    await parent_resource(after=True)
    return outcome.kind


async def tool_parent(
    ctx: WorkflowExecutionContext,
    worktree_id: str,
    factory: Literal["run_agent", "subworkflow"],
) -> pydantic.JsonValue:
    binding = (
        await tools.subagent_tool([REFERENCE + "leaf"])
        if factory == "run_agent"
        else await tools.subworkflow_tool([REFERENCE + "leaf"])
    )
    result = await tools.invoke_tool(
        binding,
        connect.ToolCallBlock(
            id="child",
            name=binding.definition.name,
            arguments={
                "reference": REFERENCE + "leaf",
                "input": {"label": "tool-child"},
                "worktree_id": worktree_id,
            },
        ),
    )
    return result.model_dump(mode="json")
