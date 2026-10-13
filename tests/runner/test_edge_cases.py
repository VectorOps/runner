import asyncio
from pathlib import Path
import shutil

import pytest

from tests.fixtures.history import MemoryHistory
from tests.fixtures.probe import ExecutionProbe
from vectorops.runner.runner.errors import ActivityError
from vectorops.runner.runner.workflow import WorkflowRunner


@pytest.mark.parametrize(
    "callable_name",
    [
        "invalid_activity_input",
        "invalid_workflow_result",
        "empty_join",
        "duplicate_join",
    ],
)
async def test_invalid_workflow_values_and_join_arguments(
    runner: WorkflowRunner,
    history: MemoryHistory,
    callable_name: str,
) -> None:
    with pytest.raises(ActivityError, match="ValidationError"):
        await runner.run(f"edge_cases:{callable_name}", workflow_id="root")
    completion = history.steps[history.heads["root"]]
    assert completion.outcome.kind == "failed"


async def test_repeated_join_reuses_child_completion(
    runner: WorkflowRunner, history: MemoryHistory, execution_probe: ExecutionProbe
) -> None:
    assert await runner.run("edge_cases:repeated_join", workflow_id="root") == 4
    path = history.path(history.heads["root"])
    joins = [step for step in path if step.operation.kind == "workflow.join"]
    assert joins[0].outcome == joins[1].outcome
    assert execution_probe.calls == [2]
    assert (
        await runner.run("edge_cases:repeated_join", continue_from_step_id=path[-1].id)
        == 4
    )
    assert execution_probe.calls == [2]


@pytest.mark.parametrize("callable_name", ["no_join", "failed_parent"])
async def test_unjoined_children_are_cancelled_at_parent_exit(
    runner: WorkflowRunner,
    execution_probe: ExecutionProbe,
    callable_name: str,
) -> None:
    before = set(asyncio.all_tasks())
    if callable_name == "no_join":
        assert (
            await asyncio.wait_for(runner.run(f"edge_cases:{callable_name}"), 2)
            == "parent finished"
        )
    else:
        with pytest.raises(ActivityError, match="parent failed"):
            await asyncio.wait_for(runner.run(f"edge_cases:{callable_name}"), 2)
    assert execution_probe.active == 0
    assert execution_probe.cancelled == 1
    assert not set(asyncio.all_tasks()) - before


async def test_workflow_cancelled_error_is_not_recorded_as_failure(
    runner: WorkflowRunner, history: MemoryHistory
) -> None:
    with pytest.raises(asyncio.CancelledError):
        await runner.run("edge_cases:cancelled_workflow")
    assert not history.steps


async def test_concurrent_root_sessions_isolate_activity_context(
    runner: WorkflowRunner, history: MemoryHistory, execution_probe: ExecutionProbe
) -> None:
    results = await asyncio.gather(
        runner.run("activities:workflow", input={"value": 10}, workflow_id="a"),
        runner.run("activities:workflow", input={"value": 20}, workflow_id="b"),
    )
    assert results == [12, 22]
    for workflow_id in ("a", "b"):
        path = history.path(history.heads[workflow_id])
        assert len(path) == 3
        assert all(step.workflow_id == workflow_id for step in path)


async def test_wrong_workflow_arguments_are_rejected_before_execution(
    runner: WorkflowRunner, history: MemoryHistory
) -> None:
    with pytest.raises(TypeError):
        await runner.run("activities:workflow", input={"unknown": True})
    assert len(history.steps) == 1
    completion = next(iter(history.steps.values()))
    assert completion.operation.kind == "workflow.complete"
    assert completion.outcome.kind == "failed"
    assert completion.outcome.error.type == "TypeError"
    assert len(history.workflows) == 1


async def test_runner_uses_workspace_configuration_overrides(
    workspace: Path, history: MemoryHistory, execution_probe: ExecutionProbe
) -> None:
    shutil.move(str(workspace / "workflows"), str(workspace / "python"))
    (workspace / "config.toml").write_text(
        'workflows_directory = "python"\ndata_directory = "state"\n'
    )
    result = await WorkflowRunner(workspace, history).run(
        "activities:empty", workflow_id="root"
    )
    assert result == {"id": "root", "data": str(workspace / "state")}
