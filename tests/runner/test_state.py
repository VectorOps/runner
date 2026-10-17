import asyncio
from pathlib import Path
import sys
from unittest.mock import AsyncMock, patch

import pydantic
import pytest

from tests.fixtures.probe import ExecutionProbe
from vectorops.runner import models
from vectorops.runner.runner import execution_state, state
from vectorops.runner.runner.cached_history import CachedHistory
from vectorops.runner.runner.checkpoint import ExecutionCheckpointer
from vectorops.runner.runner.errors import ActivityError
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner.runner.state_store import RunnerStateStore
from vectorops.runner.runner.workflow import WorkflowRunner


def database(workspace: Path, execution_id: str) -> SQLiteHistory:
    return SQLiteHistory(
        workspace / "data/executions" / execution_id / "history.sqlite3"
    )


def test_global_state_tracks_execution_discovery() -> None:
    pointer = state.RunnerState(last_execution_id="execution")
    assert pointer.model_dump() == {
        "schema_version": 1,
        "last_execution_id": "execution",
        "active_execution_ids": [],
    }
    assert state.RunnerState.model_validate_json(pointer.model_dump_json()) == pointer
    with pytest.raises(pydantic.ValidationError):
        state.RunnerState.model_validate({"executions": {}})
    with pytest.raises(pydantic.ValidationError):
        state.RunnerState(last_execution_id="../escape")


@pytest.mark.parametrize(
    "patch", [{"id": "../escape"}, {"status": "unknown"}, {"root_workflow_id": ""}]
)
def test_execution_state_validation(patch: dict[str, object]) -> None:
    with pytest.raises(pydantic.ValidationError):
        execution_state.ExecutionRecord.model_validate(
            {"id": "execution", "root_workflow_id": "root", **patch}
        )
    with pytest.raises(pydantic.ValidationError):
        execution_state.ExecutionCheckpoint(heads={"root": ""})


async def test_atomic_pointer_save_and_project_ownership(tmp_path: Path) -> None:
    store = RunnerStateStore(tmp_path)
    assert await store.load() == state.RunnerState()
    with pytest.raises(RuntimeError, match="ownership"):
        await store.save(state.RunnerState())
    async with store:
        await store.save(state.RunnerState(last_execution_id="execution"))
        assert (await store.load()).model_dump() == {
            "schema_version": 1,
            "last_execution_id": "execution",
            "active_execution_ids": [],
        }
        with pytest.raises(RuntimeError, match="another runner"):
            async with RunnerStateStore(tmp_path):
                pass
    async with RunnerStateStore(tmp_path) as reopened:
        assert (await reopened.load()).last_execution_id == "execution"
    assert not list(tmp_path.glob(".runner-state-*"))


async def test_corrupt_pointer_is_not_silently_reset(tmp_path: Path) -> None:
    store = RunnerStateStore(tmp_path)
    await store.database.save_document("runner", "not json")
    with pytest.raises(pydantic.ValidationError):
        await store.load()


async def test_executions_are_isolated_and_old_history_survives(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    runner = WorkflowRunner(workspace)
    await runner.run("activities:workflow", workflow_id="root")
    first_id = str(runner.last_execution_id)
    first = await runner.execution_state(first_id)
    original = await database(workspace, first_id).get_step(
        str(first.completion_step_id)
    )
    await runner.run("activities:workflow", workflow_id="root")
    second_id = str(runner.last_execution_id)
    assert first_id != second_id
    assert (await runner.project_state()).last_execution_id == second_id
    assert (await runner.execution_state(first_id)).status == "completed"
    assert await database(workspace, first_id).get_step(original.id) == original
    assert await WorkflowRunner(workspace).resume(first_id) == 2
    assert execution_probe.calls == [0, 1, 0, 1]


async def test_history_resumes_without_global_state(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    runner = WorkflowRunner(workspace)
    assert await runner.run("children:deep_parent", workflow_id="root") == 4
    execution_id = str(runner.last_execution_id)
    (workspace / "data/workspace.sqlite3").unlink()
    reopened = WorkflowRunner(workspace)
    assert (await reopened.project_state()).last_execution_id is None
    assert await reopened.resume(execution_id) == 4
    assert execution_probe.calls == [1, 2]
    assert (await reopened.project_state()).last_execution_id == execution_id


async def test_cancelled_stream_retains_latest_head_in_database(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    runner = WorkflowRunner(workspace)
    task = asyncio.create_task(
        runner.run("streaming:workflow", input={"mode": "blocked"}, workflow_id="root")
    )
    await asyncio.wait_for(execution_probe.started.get(), 2)
    execution_id = str(runner.last_execution_id)
    assert (await runner.execution_state(execution_id)).status == "running"
    assert (await runner.project_state()).active_execution_ids == [execution_id]
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    execution = await runner.execution_state(execution_id)
    assert execution.status == "cancelled"
    head = execution.checkpoint.heads["root"]
    assert (
        await database(workspace, execution_id).get_step(head)
    ).outcome == models.StepStreaming(state={"index": 0})
    pointer = await runner.project_state()
    assert pointer.last_execution_id == execution_id
    assert pointer.active_execution_ids == []
    execution_probe.gate.set()
    assert await WorkflowRunner(workspace).resume() == {"count": 2}
    assert (await runner.execution_state(execution_id)).status == "completed"


async def test_nested_recovery_uses_database_child_heads(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    runner = WorkflowRunner(workspace)
    task = asyncio.create_task(
        runner.run("children:parent", input={"values": [1, 2], "block": True})
    )
    await asyncio.wait_for(execution_probe.started.get(), 2)
    await asyncio.wait_for(execution_probe.started.get(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    execution_probe.gate.set()
    assert await WorkflowRunner(workspace).resume() == [2, 4]


async def test_nested_completed_heads_are_self_contained(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    runner = WorkflowRunner(workspace)
    assert await runner.run("children:deep_parent", workflow_id="root") == 4
    execution = await runner.execution_state(str(runner.last_execution_id))
    assert len(execution.checkpoint.heads) == 3
    history = database(workspace, execution.id)
    for workflow_id, step_id in execution.checkpoint.heads.items():
        terminal = await history.get_step(step_id)
        assert terminal.workflow_id == workflow_id
        assert terminal.operation.kind == "workflow.complete"
    assert await WorkflowRunner(workspace).resume() == 4
    assert execution_probe.calls == [1, 2]


async def test_stop_cascades_to_children_and_can_resume_from_history(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    runner = WorkflowRunner(workspace)
    task = asyncio.create_task(
        runner.run("children:parent", input={"values": [1, 2], "block": True})
    )
    for _ in range(2):
        await asyncio.wait_for(execution_probe.started.get(), 2)
    execution_id = str(runner.last_execution_id)
    runner.stop()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 2)
    assert execution_probe.active == 0
    execution = await runner.execution_state(execution_id)
    assert execution.status == "cancelled"
    assert len(execution.checkpoint.heads) == 3
    execution_probe.gate.set()
    assert await WorkflowRunner(workspace).resume(execution_id) == [2, 4]
    assert (await runner.execution_state(execution_id)).status == "completed"


async def test_failed_execution_status_lives_only_in_database(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    runner = WorkflowRunner(workspace)
    with pytest.raises(ActivityError):
        await runner.run("activities:unhandled")
    execution = await runner.execution_state(str(runner.last_execution_id))
    assert execution.status == "failed"
    assert execution.completion_step_id is not None
    assert await runner.recovery_candidates() == []
    assert set((await runner.project_state()).model_dump()) == {
        "schema_version",
        "last_execution_id",
        "active_execution_ids",
    }


async def test_restart_discovery_reads_status_from_database(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    runner = WorkflowRunner(workspace)
    await runner.run("activities:workflow", workflow_id="root")
    execution_id = str(runner.last_execution_id)
    history = database(workspace, execution_id)
    record = await history.get_execution()
    await history.save_execution(record.model_copy(update={"status": "running"}))
    reopened = WorkflowRunner(workspace)
    assert [candidate.id for candidate in await reopened.recovery_candidates()] == [
        execution_id
    ]
    assert execution_probe.calls == [0, 1]
    assert await reopened.resume() == 2
    assert execution_probe.calls == [0, 1]


async def test_environment_setup_failure_records_completion(workspace: Path) -> None:
    runner = WorkflowRunner(workspace)
    with patch(
        "vectorops.runner.runner.workflow.LocalEnvironment.open",
        AsyncMock(side_effect=RuntimeError("environment unavailable")),
    ):
        with pytest.raises(RuntimeError, match="environment unavailable"):
            await runner.run("activities:workflow", workflow_id="root")
    record = await runner.execution_state(str(runner.last_execution_id))
    assert record.status == "failed"
    assert record.completion_step_id is not None
    completion = await database(workspace, record.id).get_step(
        record.completion_step_id
    )
    assert completion.outcome.kind == "failed"
    assert completion.outcome.error.message == "environment unavailable"


async def test_process_crash_recovers_database_head_before_periodic_checkpoint(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "tests.fixtures.process_worker",
        str(workspace),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        assert process.stdout is not None
        execution_id = (
            (await asyncio.wait_for(process.stdout.readline(), 10)).decode().strip()
        )
        assert execution_id
    finally:
        if process.returncode is None:
            process.kill()
        await process.communicate()
    runner = WorkflowRunner(workspace)
    assert [candidate.id for candidate in await runner.recovery_candidates()] == [
        execution_id
    ]
    execution_probe.gate.set()
    assert await runner.resume() == {"count": 2}
    assert (await runner.execution_state(execution_id)).status == "completed"


async def test_fork_preserves_old_history_and_changes_database_head(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    runner = WorkflowRunner(workspace)
    await runner.run("activities:workflow", workflow_id="root")
    execution = await runner.execution_state(str(runner.last_execution_id))
    history = database(workspace, execution.id)
    path = [
        step
        async for step in history.replay_path("root", str(execution.completion_step_id))
    ]
    assert (
        await runner.run(
            "activities:workflow",
            execution_id=execution.id,
            continue_from_step_id=path[0].id,
        )
        == 2
    )
    fork = await history.get_execution()
    assert fork.completion_step_id != execution.completion_step_id
    assert await history.get_step(path[-1].id) == path[-1]
    assert execution_probe.calls == [0, 1, 1]


async def test_cleanup_leaves_other_histories_and_clears_pointer_only_if_selected(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    runner = WorkflowRunner(workspace)
    await runner.run("activities:workflow")
    first_id = str(runner.last_execution_id)
    await runner.run("activities:workflow")
    second_id = str(runner.last_execution_id)
    await runner.remove_execution(first_id)
    assert not database(workspace, first_id).path.parent.exists()
    assert (await runner.project_state()).last_execution_id == second_id
    await runner.remove_execution(second_id)
    assert (await runner.project_state()).last_execution_id is None


async def test_running_history_is_not_deleted(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    runner = WorkflowRunner(workspace)
    await runner.run("activities:workflow")
    execution_id = str(runner.last_execution_id)
    history = database(workspace, execution_id)
    await history.save_execution(
        (await history.get_execution()).model_copy(update={"status": "running"})
    )
    with pytest.raises(ValueError, match="active"):
        await runner.remove_execution(execution_id)
    assert history.path.exists()


async def test_second_runner_cannot_mutate_active_project(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    task = asyncio.create_task(WorkflowRunner(workspace).run("activities:wait"))
    await asyncio.wait_for(execution_probe.started.get(), 2)
    try:
        with pytest.raises(RuntimeError, match="another runner"):
            await WorkflowRunner(workspace).run("activities:workflow")
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


async def test_missing_history_is_not_recreated(tmp_path: Path) -> None:
    async with RunnerStateStore(tmp_path / "data") as store:
        await store.save(state.RunnerState(last_execution_id="missing"))
    with pytest.raises(FileNotFoundError):
        await WorkflowRunner(tmp_path).resume()
    assert not database(tmp_path, "missing").path.exists()


async def test_periodic_state_flush_stays_inside_history_database(
    tmp_path: Path,
) -> None:
    history_database = SQLiteHistory(tmp_path / "history.sqlite3")
    record = execution_state.ExecutionRecord(id="execution", root_workflow_id="root")
    async with CachedHistory(history_database, flush_interval=60) as history:
        await history.save_workflow(
            models.Workflow(id="root", definition_ref="file:workflow")
        )
        checkpointer = ExecutionCheckpointer(
            history_database, history, record, interval=0.01
        )
        await checkpointer.persist()
        step = models.WorkflowStep(
            id="step",
            workflow_id="root",
            operation=models.StepOperation(kind="activity", name="work"),
            outcome=models.StepStreaming(),
        )
        await history.append_step(step)
        await history.update_step(
            step.model_copy(update={"outcome": models.StepStreaming(state=7)})
        )
        async with checkpointer.tracking():
            checkpointer.record("root", "step")
            for _ in range(100):
                if (
                    await history_database.get_step("step")
                ).outcome == models.StepStreaming(state=7):
                    break
                await asyncio.sleep(0.005)
            else:
                raise AssertionError("state was not flushed")
            assert (await history_database.get_execution()).checkpoint.heads == {
                "root": "step"
            }
    assert not (tmp_path / "runner-state.json").exists()


async def test_database_heads_advance_atomically_without_checkpoint_refresh(
    tmp_path: Path,
) -> None:
    history = SQLiteHistory(tmp_path / "history.sqlite3")
    await history.save_workflow(
        models.Workflow(id="root", definition_ref="file:workflow")
    )
    await history.save_execution(
        execution_state.ExecutionRecord(id="execution", root_workflow_id="root")
    )
    step = models.WorkflowStep(
        id="step",
        workflow_id="root",
        operation=models.StepOperation(kind="activity", name="work"),
        outcome=models.StepCompleted(result=1),
    )
    await history.append_step(step)
    reopened = SQLiteHistory(history.path)
    assert (await reopened.get_execution()).checkpoint.heads == {"root": "step"}
    stale = execution_state.ExecutionRecord(id="execution", root_workflow_id="root")
    await reopened.save_execution(stale)
    assert (await reopened.get_execution()).checkpoint.heads == {"root": "step"}
    with pytest.raises(ValueError, match="identity"):
        await reopened.save_execution(
            execution_state.ExecutionRecord(id="other", root_workflow_id="root")
        )


async def test_empty_pointer_cannot_resume(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="no last execution"):
        await WorkflowRunner(tmp_path).resume()


async def test_persistent_continuation_requires_execution_identity(
    workspace: Path,
) -> None:
    with pytest.raises(ValueError, match="execution ID"):
        await WorkflowRunner(workspace).run(
            "activities:workflow", continue_from_step_id="unknown"
        )
