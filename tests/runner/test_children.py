import asyncio

import pytest

from tests.fixtures.history import MemoryHistory
from tests.fixtures.probe import ExecutionProbe
from vectorops.runner import models
from vectorops.runner.runner.errors import ActivityError, ReplayMismatchError
from vectorops.runner.runner.workflow import WorkflowRunner


async def test_parallel_children_preserve_invocation_order(
    runner: WorkflowRunner, history: MemoryHistory, execution_probe: ExecutionProbe
) -> None:
    task = asyncio.create_task(
        runner.run(
            "children",
            "parent",
            input={"values": [2, 1], "block": True},
            workflow_id="root",
        )
    )
    assert {
        await asyncio.wait_for(execution_probe.started.get(), 2),
        await asyncio.wait_for(execution_probe.started.get(), 2),
    } == {1, 2}
    assert execution_probe.peak_active == 2
    execution_probe.gate.set()
    assert await asyncio.wait_for(task, 2) == [4, 2]
    path = history.path(history.heads["root"])
    assert [step.operation.kind for step in path] == [
        "workflow.spawn",
        "workflow.spawn",
        "workflow.join",
        "workflow.resume",
        "workflow.complete",
    ]
    assert path[2].outcome.kind == "joined"
    assert [item.spawn_step_id for item in path[2].outcome.child_completions] == [
        path[0].id,
        path[1].id,
    ]


@pytest.mark.parametrize("callable_name", ["sequential", "nested"])
async def test_sequential_and_nested_children(
    runner: WorkflowRunner,
    history: MemoryHistory,
    execution_probe: ExecutionProbe,
    callable_name: str,
) -> None:
    assert await runner.run("children", callable_name, workflow_id="root") == [2, 4]
    before = list(history.appended)
    assert await runner.run(
        "children", callable_name, continue_from_step_id=history.heads["root"]
    ) == [2, 4]
    assert history.appended == before
    assert execution_probe.calls == [1, 2]


async def test_collect_failed_children(
    runner: WorkflowRunner, history: MemoryHistory
) -> None:
    expected = [4, {"error": "child -1 failed"}, 2]
    assert (
        await runner.run(
            "children",
            "parent",
            input={"values": [2, -1, 1], "policy": "collect"},
            workflow_id="root",
        )
        == expected
    )
    assert (
        await runner.run(
            "children", "parent", continue_from_step_id=history.heads["root"]
        )
        == expected
    )


async def test_propagates_first_failure_in_invocation_order(
    runner: WorkflowRunner, history: MemoryHistory
) -> None:
    with pytest.raises(ActivityError, match="child -2 failed"):
        await runner.run(
            "children", "parent", input={"values": [-2, -1]}, workflow_id="root"
        )
    with pytest.raises(ActivityError, match="child -2 failed"):
        await runner.run(
            "children", "parent", continue_from_step_id=history.heads["root"]
        )


async def test_parent_can_catch_child_failure(
    runner: WorkflowRunner, history: MemoryHistory
) -> None:
    assert (
        await runner.run("children", "catch", workflow_id="root") == "child -1 failed"
    )
    assert (
        await runner.run(
            "children", "catch", continue_from_step_id=history.heads["root"]
        )
        == "child -1 failed"
    )


async def test_resume_spawn_with_explicit_child_head(
    runner: WorkflowRunner, history: MemoryHistory, execution_probe: ExecutionProbe
) -> None:
    await runner.run("children", "sequential", workflow_id="root")
    parent_path = history.path(history.heads["root"])
    spawn = models.WorkflowSpawnOperation.model_validate(
        parent_path[0].operation.model_dump()
    )
    child_path = history.path(history.heads[spawn.child_workflow_id])
    before = len(execution_probe.calls)
    assert await runner.run(
        "children",
        "sequential",
        continue_from_step_id=parent_path[0].id,
        child_heads={spawn.child_workflow_id: child_path[0].id},
    ) == [2, 4]
    assert len(execution_probe.calls) == before + 1
    assert history.steps[child_path[-1].id] == child_path[-1]


async def test_invalid_child_completion_reference(
    runner: WorkflowRunner, history: MemoryHistory
) -> None:
    await runner.run("children", "sequential", workflow_id="root")
    parent_path = history.path(history.heads["root"])
    join = parent_path[1]
    assert join.outcome.kind == "joined"
    reference = join.outcome.child_completions[0]
    completion = history.steps[reference.completion_step_id]
    history.steps[completion.id] = completion.model_copy(
        update={"workflow_id": "wrong"}
    )
    with pytest.raises(ReplayMismatchError):
        await runner.run(
            "children", "sequential", continue_from_step_id=parent_path[-1].id
        )


async def test_deep_partial_recovery_forks_each_level_without_replaying_effects(
    runner: WorkflowRunner, history: MemoryHistory, execution_probe: ExecutionProbe
) -> None:
    assert await runner.run("children", "deep_parent", workflow_id="root") == 4
    root_path = history.path(history.heads["root"])
    child_spawn = models.WorkflowSpawnOperation.model_validate(
        root_path[0].operation.model_dump()
    )
    child_id = child_spawn.child_workflow_id
    child_path = history.path(history.heads[child_id])
    leaf_spawn = models.WorkflowSpawnOperation.model_validate(
        child_path[0].operation.model_dump()
    )
    leaf_id = leaf_spawn.child_workflow_id
    leaf_path = history.path(history.heads[leaf_id])
    original_steps = dict(history.steps)
    original_workflows = dict(history.workflows)
    assert execution_probe.calls == [1, 2]
    execution_probe.calls.clear()

    assert (
        await runner.run(
            "children",
            "deep_parent",
            continue_from_step_id=root_path[0].id,
            child_heads={child_id: child_path[0].id, leaf_id: leaf_path[0].id},
        )
        == 4
    )

    assert execution_probe.calls == [2]
    assert history.consumed == [
        root_path[0].id,
        child_path[0].id,
        leaf_path[0].id,
    ]
    assert history.closed_paths == 3
    assert history.workflows == original_workflows
    assert all(
        history.steps[step_id] == step for step_id, step in original_steps.items()
    )
    assert len(history.steps) == len(original_steps) + 10

    fork_paths = {
        "root": history.path(history.heads["root"]),
        child_id: history.path(history.heads[child_id]),
        leaf_id: history.path(history.heads[leaf_id]),
    }
    for workflow_id, original in (
        ("root", root_path),
        (child_id, child_path),
        (leaf_id, leaf_path),
    ):
        fork = fork_paths[workflow_id]
        assert fork[0] == original[0]
        assert fork[1].id != original[1].id
        assert fork[1].previous_step_id == original[0].id
        assert fork[-1].id != original[-1].id

    root_join = fork_paths["root"][1].outcome
    child_join = fork_paths[child_id][1].outcome
    assert root_join.kind == "joined"
    assert child_join.kind == "joined"
    assert root_join.child_completions[0].completion_step_id == history.heads[child_id]
    assert child_join.child_completions[0].completion_step_id == history.heads[leaf_id]

    appended = list(history.appended)
    execution_probe.calls.clear()
    assert (
        await runner.run(
            "children", "deep_parent", continue_from_step_id=history.heads["root"]
        )
        == 4
    )
    assert history.appended == appended
    assert execution_probe.calls == []


async def test_parent_cancellation_cleans_children(
    runner: WorkflowRunner, history: MemoryHistory, execution_probe: ExecutionProbe
) -> None:
    task = asyncio.create_task(
        runner.run(
            "children",
            "parent",
            input={"values": [1, 2], "block": True},
            workflow_id="root",
        )
    )
    await asyncio.wait_for(execution_probe.started.get(), 2)
    await asyncio.wait_for(execution_probe.started.get(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 2)
    assert execution_probe.active == 0
    assert all(
        step.operation.kind == "workflow.spawn"
        or step.outcome.kind in ("pending", "waiting")
        for step in history.steps.values()
    )
