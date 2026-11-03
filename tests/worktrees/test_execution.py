import asyncio
import json
from pathlib import Path

import pytest

from tests.fixtures.probe import ExecutionProbe
from vectorops.runner.runner.project import RunnerProject
from vectorops.runner.runner.workflow import WorkflowRunner
from vectorops.runner.worktrees import models as m


async def test_root_child_inheritance_replay_and_reuse(
    workspace: Path, repository: Path, execution_probe: ExecutionProbe
) -> None:
    (workspace / "config.toml").write_text(
        f'[environment]\nkind = "local"\nbase_project_path = {json.dumps(str(repository))}\n'
    )
    project = await RunnerProject.open(workspace)
    record = await project.worktrees.create(m.Create(name="Child task"), key="child")
    runner = WorkflowRunner(workspace, project=project)
    expected = [str(repository), record.path, record.path, str(repository)]
    assert (
        await runner.run("worktree:parent", input={"worktree_id": str(record.id)})
        == expected
    )
    execution_id = str(runner.last_execution_id)
    assert (await runner.execution_summary(execution_id)).worktree_id is None
    assert len(execution_probe.contexts) == 4
    assert (
        execution_probe.contexts[1].environment
        is execution_probe.contexts[2].environment
    )
    assert (
        execution_probe.contexts[0].environment
        is execution_probe.contexts[3].environment
    )
    assert (
        execution_probe.contexts[1].resources is execution_probe.contexts[2].resources
    )
    assert (
        execution_probe.contexts[1].resources
        is not execution_probe.contexts[0].resources
    )
    assert await WorkflowRunner(workspace).resume(execution_id) == expected
    assert len(execution_probe.contexts) == 4
    assert await runner.run("worktree:leaf", worktree_id=record.id) == record.path
    root_record = await runner.execution_state(str(runner.last_execution_id))
    assert (
        root_record.target is not None and root_record.target.worktree_id == record.id
    )
    summary = await runner.execution_summary(root_record.id)
    assert summary.worktree_id == str(record.id)
    assert summary.model_dump(mode="json")["worktree_id"] == str(record.id)
    reopened = WorkflowRunner(workspace)
    await reopened.initialize_usage()
    assert (await reopened.execution_summary(root_record.id)).worktree_id == str(
        record.id
    )
    assert await project.worktrees.delete(m.Delete(worktree_id=record.id))


async def test_child_resume_restores_target_and_deletion_is_blocked(
    workspace: Path, repository: Path, execution_probe: ExecutionProbe
) -> None:
    (workspace / "config.toml").write_text(
        f'[environment]\nkind = "local"\nbase_project_path = {json.dumps(str(repository))}\n'
    )
    project = await RunnerProject.open(workspace)
    record = await project.worktrees.create(
        m.Create(name="Resumable child"), key="child"
    )
    runner = WorkflowRunner(workspace, project=project)
    task = asyncio.create_task(
        runner.run(
            "worktree:parent", input={"worktree_id": str(record.id), "block": True}
        )
    )
    try:
        await asyncio.wait_for(execution_probe.started.get(), 5)
        with pytest.raises(m.WorktreeError, match="in use"):
            await project.worktrees.delete(m.Delete(worktree_id=record.id, force=True))
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    execution_id = str(runner.last_execution_id)
    execution_probe.gate.set()
    reopened = WorkflowRunner(workspace)
    assert await reopened.resume(execution_id) == [
        str(repository),
        record.path,
        record.path,
        str(repository),
    ]
    assert (await project.worktrees.get(m.Get(worktree_id=record.id))).status == "ready"
