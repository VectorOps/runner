import asyncio
import sys
from uuid import UUID

import connect
import pydantic

from tests.fixtures import worktree_cases as cases
from vectorops.runner.api import composition, tools
from vectorops.runner.api.mailbox import request_input
from vectorops.runner.environment.processes import models as processes
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.context import WorkflowExecutionContext

REFERENCE = "python:tests.fixtures.worktree_deletion:"


@composition.activity()
async def setup(ctx: ActivityExecutionContext, cleanup_wait: bool = False) -> None:
    assert ctx.resources is not None and ctx.environment is not None
    cases.current.contexts[ctx.workflow_id] = ctx

    async def cleanup() -> None:
        assert ctx.environment is not None
        assert await asyncio.to_thread(ctx.environment.root.is_dir)
        cases.current.cleanup_started.set()
        if cleanup_wait:
            await cases.current.cleanup_gate.wait()
        cases.current.events.append("cleaned:" + ctx.workflow_id)

    ctx.resources.register("deletion probe", cleanup)


async def paused(
    ctx: WorkflowExecutionContext, after_input: bool = False, cleanup_wait: bool = False
) -> pydantic.JsonValue:
    await setup(cleanup_wait)
    value = await request_input("worktree", "continue")
    if after_input:
        await cases.observe("after-input", True)
    return value


async def child_paused(
    ctx: WorkflowExecutionContext, worktree_id: str
) -> pydantic.JsonValue:
    handle = await ctx.spawn_workflow(
        REFERENCE + "paused", worktree_id=UUID(worktree_id)
    )
    outcome = (await ctx.join_workflows([handle]))[0]
    assert outcome.kind == "completed"
    return outcome.result


async def finished_child_paused(
    ctx: WorkflowExecutionContext, worktree_id: str
) -> pydantic.JsonValue:
    handle = await ctx.spawn_workflow(
        cases.REFERENCE + "leaf", worktree_id=UUID(worktree_id)
    )
    await ctx.join_workflows([handle])
    return await request_input("worktree", "continue")


async def selected_child(ctx: WorkflowExecutionContext) -> pydantic.JsonValue:
    identifier = await request_input("selection", "worktree")
    assert isinstance(identifier, str)
    return await finished_child_paused(ctx, identifier)


async def running(ctx: WorkflowExecutionContext, cleanup_wait: bool = False) -> None:
    await setup(cleanup_wait)
    await cases.observe("running", True)


async def failed(ctx: WorkflowExecutionContext) -> None:
    raise ValueError("expected failure")


@composition.activity()
async def process(
    ctx: ActivityExecutionContext, worktree_id: str, completed: bool = False
) -> None:
    assert ctx.targets is not None and ctx.environment is not None
    result = await ctx.targets.exec(
        processes.ExecRequest(
            command=processes.ArgvCommand(
                argv=[
                    sys.executable,
                    "-c",
                    "pass" if completed else "import time; time.sleep(60)",
                ]
            ),
            yield_timeout_ms=1000 if completed else 0,
        ),
        UUID(worktree_id),
    )
    if completed:
        manager = ctx.targets.process_manager(result.process_id, ctx.environment)
        while not result.output_complete:
            result = await manager.poll(
                processes.PollRequest(process_id=result.process_id, wait_timeout_ms=100)
            )
        assert result.returncode == 0


async def process_paused(
    ctx: WorkflowExecutionContext, worktree_id: str, completed: bool = False
) -> pydantic.JsonValue:
    await process(worktree_id, completed)
    return await request_input("worktree", "continue")


async def delete_tool(
    ctx: WorkflowExecutionContext,
    worktree_id: str,
    child_first: bool = False,
    force: bool = False,
) -> pydantic.JsonValue:
    if child_first:
        handle = await ctx.spawn_workflow(
            cases.REFERENCE + "leaf", worktree_id=UUID(worktree_id)
        )
        await ctx.join_workflows([handle])
    response = await tools.invoke_tool(
        tools.worktree_delete_tool(),
        connect.ToolCallBlock(
            id="delete",
            name="worktree_delete",
            arguments={"worktree_id": worktree_id, "force": force},
        ),
    )
    return response.model_dump(mode="json")
