import asyncio
from unittest.mock import patch
from uuid import UUID
from typing import Optional

import pydantic
import pytest

from tests.fixtures import worktree_cases as cases
from tests.fixtures.history import MemoryHistory
from vectorops.runner.runner.context import WorkflowExecutionContext
from vectorops.runner.runner.errors import ReplayMismatchError
from vectorops.runner.runner.executor import WorkflowExecutor
from vectorops.runner.runner.resolver import WorkflowEntryPoint
from vectorops.runner.runner.project import RunnerProject
from vectorops.runner.runner.workflow import WorkflowRunner
from vectorops.runner.worktrees import models as m


@pytest.mark.parametrize("change", ["replace", "remove", "add"])
async def test_replay_rejects_changed_child_worktree_selection(
    worktree_project: RunnerProject, worktree_probe: cases.Probe, change: str
) -> None:
    manager = worktree_project.worktrees
    first = await manager.create(m.Create(name="First"), key="first")
    second = await manager.create(m.Create(name="Second"), key="second")
    selection: Optional[UUID] = None if change == "add" else first.id

    async def workflow(ctx: WorkflowExecutionContext) -> pydantic.JsonValue:
        handle = await ctx.spawn_workflow(
            cases.REFERENCE + "leaf", worktree_id=selection
        )
        outcome = (await ctx.join_workflows([handle]))[0]
        assert outcome.kind == "completed"
        return outcome.result

    original = WorkflowExecutor._definition

    async def definition(
        executor: WorkflowExecutor, reference: str
    ) -> WorkflowEntryPoint:
        return (
            workflow
            if reference == cases.REFERENCE + "matrix"
            else await original(executor, reference)
        )

    history = MemoryHistory()
    runner = WorkflowRunner(worktree_project.root, history, project=worktree_project)
    with patch.object(WorkflowExecutor, "_definition", definition):
        await runner.run(cases.REFERENCE + "matrix", workflow_id="parent")
        selection = None if change == "remove" else second.id
        with patch.object(
            manager,
            "resolve",
            side_effect=AssertionError("changed selection must fail before attachment"),
        ):
            with pytest.raises(
                ReplayMismatchError, match="child configuration differs"
            ):
                await runner.run(
                    cases.REFERENCE + "matrix",
                    continue_from_step_id=history.heads["parent"],
                )


async def test_completed_child_replays_after_worktree_deletion(
    worktree_project: RunnerProject, worktree_probe: cases.Probe
) -> None:
    manager = worktree_project.worktrees
    record = await manager.create(m.Create(name="Completed"), key="completed")
    runner = WorkflowRunner(worktree_project.root, project=worktree_project)
    result = await runner.run(
        cases.REFERENCE + "matrix", input={"child": str(record.id)}
    )
    identifier = str(runner.last_execution_id)
    await manager.delete(m.Delete(worktree_id=record.id))
    worktree_probe.events.clear()
    with patch.object(
        manager,
        "resolve",
        side_effect=AssertionError("completed child must not reattach"),
    ):
        assert await runner.resume(identifier) == result
    assert worktree_probe.events == []


async def test_incomplete_child_does_not_fall_back_after_deletion(
    worktree_project: RunnerProject, worktree_probe: cases.Probe
) -> None:
    manager = worktree_project.worktrees
    record = await manager.create(m.Create(name="Interrupted"), key="interrupted")
    worktree_probe.gates["leaf"] = asyncio.Event()
    runner = WorkflowRunner(worktree_project.root, project=worktree_project)
    task = asyncio.create_task(
        runner.run(
            cases.REFERENCE + "waiting_parent", input={"worktree_id": str(record.id)}
        )
    )
    try:
        assert await asyncio.wait_for(worktree_probe.started.get(), 5) == "leaf"
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    identifier = str(runner.last_execution_id)
    await manager.delete(m.Delete(worktree_id=record.id))
    worktree_probe.events.clear()
    worktree_probe.gates["leaf"].set()
    with pytest.raises(m.WorktreeError, match="not ready: deleted"):
        await WorkflowRunner(worktree_project.root).resume(identifier)
    assert worktree_probe.events == []
