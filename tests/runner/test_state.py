import asyncio
import sys
from pathlib import Path
import json

import pydantic
import pytest

from tests.fixtures.probe import ExecutionProbe
from vectorops.runner import models
from vectorops.runner.runner import state
from vectorops.runner.runner.checkpoint import ExecutionCheckpointer
from vectorops.runner.runner.cached_history import CachedHistory
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner.runner.state_store import RunnerStateStore
from vectorops.runner.runner.workflow import WorkflowRunner
from vectorops.runner.runner.errors import ActivityError


async def test_failed_execution_status_and_terminal_reference(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    runner = WorkflowRunner(workspace)
    with pytest.raises(ActivityError):
        await runner.run("activities", "unhandled")
    execution = (await runner.project_state()).executions[str(runner.last_execution_id)]
    assert execution.status == "failed"
    assert execution.completion_step_id is not None
    assert await runner.recovery_candidates() == []


async def test_abrupt_process_exit_retains_recoverable_execution(
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
    assert await runner.resume(execution_id) == {"count": 2}
    assert (await runner.project_state()).executions[execution_id].status == "completed"


async def test_nested_cancelled_execution_resumes_with_recorded_child_heads(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    runner = WorkflowRunner(workspace)
    task = asyncio.create_task(
        runner.run("children", "parent", input={"values": [1, 2], "block": True})
    )
    await asyncio.wait_for(execution_probe.started.get(), 2)
    await asyncio.wait_for(execution_probe.started.get(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    execution_id = str(runner.last_execution_id)
    execution = (await runner.project_state()).executions[execution_id]
    assert execution.status == "cancelled"
    assert execution.root_workflow_id in execution.checkpoint.heads
    execution_probe.gate.set()
    assert await WorkflowRunner(workspace).resume(execution_id) == [2, 4]


async def test_persistent_fork_keeps_original_completion(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    runner = WorkflowRunner(workspace)
    await runner.run("activities", workflow_id="root")
    execution = (await runner.project_state()).executions[str(runner.last_execution_id)]
    database = SQLiteHistory(workspace / "data" / execution.history_path)
    path = [
        step
        async for step in database.replay_path(
            "root", str(execution.completion_step_id)
        )
    ]
    assert (
        await runner.run(
            "activities", execution_id=execution.id, continue_from_step_id=path[0].id
        )
        == 2
    )
    fork = (await runner.project_state()).executions[execution.id]
    assert fork.completion_step_id != execution.completion_step_id
    assert await database.get_step(path[-1].id) == path[-1]
    assert execution_probe.calls == [0, 1, 1]


async def test_missing_execution_history_is_not_recreated(tmp_path: Path) -> None:
    async with RunnerStateStore(tmp_path / "data") as store:
        await store.save(state.RunnerState(executions={"execution": record()}))
    with pytest.raises(FileNotFoundError):
        await WorkflowRunner(tmp_path).run("activities", execution_id="execution")
    assert not (tmp_path / "data" / record().history_path).exists()


def record(identifier: str = "execution") -> state.ExecutionRecord:
    return state.ExecutionRecord(
        id=identifier,
        root_workflow_id="root",
        history_path=f"executions/{identifier}/history.sqlite3",
    )


@pytest.mark.parametrize(
    "patch",
    [
        {"id": "../escape"},
        {"history_path": "../outside"},
        {"history_path": "executions/other/history.sqlite3"},
        {"status": "unknown"},
    ],
)
def test_execution_models_reject_invalid_identity_and_paths(
    patch: dict[str, object],
) -> None:
    with pytest.raises(pydantic.ValidationError):
        state.ExecutionRecord.model_validate({**record().model_dump(), **patch})


def test_runner_manifest_json_roundtrip_and_registry_validation() -> None:
    manifest = state.RunnerState(executions={"execution": record()})
    assert state.RunnerState.model_validate_json(manifest.model_dump_json()) == manifest
    with pytest.raises(pydantic.ValidationError):
        state.RunnerState(executions={"wrong": record()})
    with pytest.raises(pydantic.ValidationError):
        state.ExecutionCheckpoint(heads={"root": ""})


async def test_manifest_atomic_save_load_and_ownership(tmp_path: Path) -> None:
    store = RunnerStateStore(tmp_path)
    assert await store.load() == state.RunnerState()
    with pytest.raises(RuntimeError, match="ownership"):
        await store.save(state.RunnerState())
    async with store:
        execution = record()
        await store.save(state.RunnerState(executions={"execution": execution}))
        assert (
            json.loads((tmp_path / "runner-state.json").read_text())["executions"][
                "execution"
            ]["status"]
            == "running"
        )
        with pytest.raises(RuntimeError, match="another runner"):
            async with RunnerStateStore(tmp_path):
                pass
    async with RunnerStateStore(tmp_path) as reopened:
        assert (await reopened.load()).executions["execution"] == execution
    assert not list(tmp_path.glob(".runner-state-*"))


async def test_corrupt_manifest_is_not_silently_reset(tmp_path: Path) -> None:
    (tmp_path / "runner-state.json").write_text("not json")
    with pytest.raises(pydantic.ValidationError):
        await RunnerStateStore(tmp_path).load()


async def test_executions_have_distinct_history_and_retain_old_records(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    runner = WorkflowRunner(workspace)
    await runner.run("activities", workflow_id="root")
    first_id = str(runner.last_execution_id)
    first = (await runner.project_state()).executions[first_id]
    original = await SQLiteHistory(workspace / "data" / first.history_path).get_step(
        str(first.completion_step_id)
    )
    await runner.run("activities", workflow_id="root")
    second_id = str(runner.last_execution_id)
    manifest = await runner.project_state()
    assert first_id != second_id
    assert len(manifest.executions) == 2
    assert manifest.executions[first_id] == first
    assert manifest.executions[second_id].history_path != first.history_path
    assert (
        await SQLiteHistory(workspace / "data" / first.history_path).get_step(
            original.id
        )
        == original
    )
    assert await WorkflowRunner(workspace).resume(first_id) == 2
    assert execution_probe.calls == [0, 1, 0, 1]


async def test_cancelled_execution_checkpoints_latest_stream_and_resumes(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    runner = WorkflowRunner(workspace)
    task = asyncio.create_task(
        runner.run("streaming", input={"mode": "blocked"}, workflow_id="root")
    )
    await asyncio.wait_for(execution_probe.started.get(), 2)
    execution_id = str(runner.last_execution_id)
    assert (await runner.project_state()).executions[execution_id].status == "running"
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    manifest = await runner.project_state()
    execution = manifest.executions[execution_id]
    assert execution.status == "cancelled"
    head = execution.checkpoint.heads["root"]
    assert (
        await SQLiteHistory(workspace / "data" / execution.history_path).get_step(head)
    ).outcome == models.StepStreaming(state={"index": 0})
    execution_probe.gate.set()
    assert await WorkflowRunner(workspace).resume(execution_id) == {"count": 2}
    assert (await runner.project_state()).executions[execution_id].status == "completed"


async def test_restart_discovery_does_not_execute_workflows(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    runner = WorkflowRunner(workspace)
    await runner.run("activities", workflow_id="root")
    execution_id = str(runner.last_execution_id)
    async with RunnerStateStore(workspace / "data") as store:
        manifest = await store.load()
        interrupted = manifest.executions[execution_id].model_copy(
            update={"status": "running"}
        )
        await store.save(state.RunnerState(executions={execution_id: interrupted}))
    reopened = WorkflowRunner(workspace)
    candidates = await reopened.recovery_candidates()
    assert [candidate.id for candidate in candidates] == [execution_id]
    assert execution_probe.calls == [0, 1]
    assert await reopened.resume(execution_id) == 2
    assert execution_probe.calls == [0, 1]


async def test_nested_execution_checkpoint_tracks_child_heads(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    runner = WorkflowRunner(workspace)
    assert await runner.run("children", "deep_parent", workflow_id="root") == 4
    execution = (await runner.project_state()).executions[str(runner.last_execution_id)]
    assert len(execution.checkpoint.heads) == 3
    database = SQLiteHistory(workspace / "data" / execution.history_path)
    for workflow_id, head in execution.checkpoint.heads.items():
        terminal = await database.get_step(head)
        assert terminal.workflow_id == workflow_id
        assert terminal.operation.kind == "workflow.complete"
    assert await WorkflowRunner(workspace).resume(execution.id) == 4
    assert execution_probe.calls == [1, 2]


async def test_terminal_cleanup_removes_only_selected_execution(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    runner = WorkflowRunner(workspace)
    await runner.run("activities")
    first_id = str(runner.last_execution_id)
    first = (await runner.project_state()).executions[first_id]
    await runner.run("activities")
    second_id = str(runner.last_execution_id)
    await runner.remove_execution(first_id)
    assert not (workspace / "data" / first.history_path).parent.exists()
    assert set((await runner.project_state()).executions) == {second_id}


async def test_running_history_is_not_deleted(tmp_path: Path) -> None:
    async with RunnerStateStore(tmp_path) as store:
        await store.save(state.RunnerState(executions={"execution": record()}))
        with pytest.raises(ValueError, match="active"):
            await store.remove("execution")
        assert "execution" in (await store.load()).executions


async def test_second_runner_cannot_mutate_active_project(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    runner = WorkflowRunner(workspace)
    task = asyncio.create_task(runner.run("activities", "wait"))
    await asyncio.wait_for(execution_probe.started.get(), 2)
    try:
        with pytest.raises(RuntimeError, match="another runner"):
            await WorkflowRunner(workspace).run("activities")
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


async def test_periodic_checkpoint_flushes_history_before_manifest(
    tmp_path: Path,
) -> None:
    database = SQLiteHistory(tmp_path / record().history_path)
    async with (
        RunnerStateStore(tmp_path) as store,
        CachedHistory(database, flush_interval=60) as history,
    ):
        await history.save_workflow(
            models.Workflow(id="root", definition_ref="file:workflow")
        )
        checkpointer = ExecutionCheckpointer(store, history, record(), interval=0.01)
        await checkpointer.persist()
        step = models.WorkflowStep(
            id="step",
            workflow_id="root",
            operation=models.StepOperation(kind="activity", name="work"),
            outcome=models.StepStreaming(state=0),
        )
        await history.append_step(step)
        await history.update_step(
            step.model_copy(update={"outcome": models.StepStreaming(state=7)})
        )
        async with checkpointer.tracking():
            checkpointer.record("root", step.id)
            for _ in range(100):
                manifest = await store.load()
                if manifest.executions["execution"].checkpoint.heads:
                    break
                await asyncio.sleep(0.005)
            else:
                raise AssertionError("checkpoint did not refresh")
            assert (
                await SQLiteHistory(database.path).get_step("step")
            ).outcome == models.StepStreaming(state=7)


async def test_persistent_continuation_requires_execution_identity(
    workspace: Path,
) -> None:
    with pytest.raises(ValueError, match="execution ID"):
        await WorkflowRunner(workspace).run(
            "activities", continue_from_step_id="unknown"
        )
