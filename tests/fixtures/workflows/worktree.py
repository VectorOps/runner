from uuid import UUID

from tests.fixtures import probe
from vectorops.runner.api.composition import activity
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.context import WorkflowExecutionContext


@activity()
async def root(ctx: ActivityExecutionContext, block: bool = False) -> str:
    assert ctx.environment is not None
    probe.current.contexts.append(ctx)
    if block:
        probe.current.started.put_nowait(1)
        await probe.current.gate.wait()
    return str(ctx.environment.root)


async def leaf(ctx: WorkflowExecutionContext, block: bool = False) -> str:
    return await root(block)


async def child(ctx: WorkflowExecutionContext, block: bool = False) -> list[str]:
    own = await root()
    handle = await ctx.spawn_workflow("worktree:leaf", input={"block": block})
    outcomes = await ctx.join_workflows([handle])
    assert outcomes[0].kind == "completed" and isinstance(outcomes[0].result, str)
    return [own, outcomes[0].result]


async def parent(
    ctx: WorkflowExecutionContext, worktree_id: str, block: bool = False
) -> list[str]:
    before = await root()
    handle = await ctx.spawn_workflow(
        "worktree:child", input={"block": block}, worktree_id=UUID(worktree_id)
    )
    outcomes = await ctx.join_workflows([handle])
    assert outcomes[0].kind == "completed" and isinstance(outcomes[0].result, list)
    after = await root()
    return [before, str(outcomes[0].result[0]), str(outcomes[0].result[1]), after]
