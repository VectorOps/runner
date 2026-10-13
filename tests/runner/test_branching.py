import asyncio
from pathlib import Path
import shutil

import pytest

from tests.fixtures.probe import ExecutionProbe
from vectorops.runner import models
from vectorops.runner.control.service import RunnerService
from vectorops.runner.runner import branching, mailbox
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner.runner.workflow import WorkflowRunner


async def path(runner: WorkflowRunner, identifier: str) -> list[models.WorkflowStep]:
    record = await runner.execution_state(identifier)
    project = await runner.project()
    history = SQLiteHistory(
        project.data_path / "executions" / identifier / "history.sqlite3"
    )
    return [
        step
        async for step in history.replay_path(
            record.root_workflow_id, record.checkpoint.heads[record.root_workflow_id]
        )
    ]


@pytest.mark.parametrize("index", [0, 1, 2])
async def test_exclusive_restart_preserves_history(
    workspace: Path, execution_probe: ExecutionProbe, index: int
) -> None:
    runner = WorkflowRunner(workspace)
    await runner.run("activities:workflow", workflow_id="root")
    identifier = str(runner.last_execution_id)
    original = await path(runner, identifier)
    request = branching.Branch(
        execution_id=identifier,
        step_id=original[index].id,
        idempotency_key="branch",
        action=branching.Restart(),
    )
    accepted = await runner.accept_branch(request)
    assert await runner.accept_branch(request) == accepted
    assert await runner.resume(identifier) == 2
    selected = await path(runner, identifier)
    assert selected[:index] == original[:index]
    assert selected[index].id != original[index].id
    history = SQLiteHistory(
        workspace / "data/executions" / identifier / "history.sqlite3"
    )
    assert [await history.get_step(step.id) for step in original] == original
    assert (await runner.accept_branch(request)).id == accepted.id
    with pytest.raises(branching.BranchConflict, match="idempotency"):
        await runner.accept_branch(request.model_copy(update={"step_id": "other"}))


async def test_cancelled_retry_gets_fresh_identity(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    runner = WorkflowRunner(workspace)
    task = asyncio.create_task(runner.run("activities:wait"))
    await asyncio.wait_for(execution_probe.started.get(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    identifier = str(runner.last_execution_id)
    original = await path(runner, identifier)
    await runner.accept_branch(
        branching.Branch(
            execution_id=identifier,
            step_id=original[0].id,
            idempotency_key="retry",
            action=branching.Restart(),
        )
    )
    execution_probe.gate.set()
    await runner.resume(identifier)
    assert (await path(runner, identifier))[0].id != original[0].id


async def wait_status(service: RunnerService, identifier: str, expected: str) -> None:
    async with asyncio.timeout(5):
        while True:
            record = await service.runner.execution_state(identifier)
            if record.status == expected:
                return
            await asyncio.sleep(0.01)


async def test_synthetic_input_and_active_branch(workspace: Path) -> None:
    shutil.copyfile(
        Path(__file__).parents[1] / "fixtures/scripts/mailbox.py",
        workspace / "workflows/mailbox.py",
    )
    async with RunnerService(workspace) as service:
        accepted = await service.dispatch(
            "executions.start",
            {
                "reference": "mailbox:workflow",
                "idempotency_key": "start",
            },
        )
        assert isinstance(accepted, dict)
        identifier = str(accepted["execution_id"])
        async with asyncio.timeout(5):
            while True:
                try:
                    requests = await service.runner.pending_inputs(
                        mailbox.RequestQuery(execution_id=identifier)
                    )
                    if requests.requests:
                        break
                except (FileNotFoundError, KeyError):
                    pass
                await asyncio.sleep(0.01)
        original = requests.requests[0]
        request = branching.Branch(
            execution_id=identifier,
            step_id=original.request_id,
            idempotency_key="edit",
            action=branching.InsertInput(response="new answer"),
        )
        receipt = await service.dispatch(
            "executions.branch", request.model_dump(mode="json")
        )
        assert (
            await service.dispatch("executions.branch", request.model_dump(mode="json"))
            == receipt
        )
        await wait_status(service, identifier, "completed")
        selected = await path(service.runner, identifier)
        assert selected[0].id != original.request_id
        assert selected[0].outcome == models.StepCompleted(result="new answer")
        assert selected[-1].outcome == models.StepCompleted(result="new answer")
        assert (
            await service.runner.input_request(
                mailbox.RequestRef(
                    execution_id=identifier,
                    request_id=selected[0].id,
                )
            )
        ).status == "answered"
        assert (
            await service.runner.input_request(
                mailbox.RequestRef(
                    execution_id=identifier,
                    request_id=original.request_id,
                )
            )
        ).status == "cancelled"

        history = SQLiteHistory(
            workspace / "data/executions" / identifier / "history.sqlite3"
        )
        original_steps = list(selected)
        old_input = await history.get_input(selected[0].id)
        second = branching.Branch(
            execution_id=identifier,
            step_id=selected[0].id,
            idempotency_key="edit-again",
            action=branching.InsertInput(response="third answer"),
        )
        await service.dispatch("executions.branch", second.model_dump(mode="json"))
        async with asyncio.timeout(5):
            while (await path(service.runner, identifier))[-1].id == original_steps[
                -1
            ].id:
                await asyncio.sleep(0.01)
        await wait_status(service, identifier, "completed")
        assert (await path(service.runner, identifier))[
            -1
        ].outcome == models.StepCompleted(result="third answer")
        assert await history.get_input(old_input.request_id) == old_input
        assert [
            await history.get_step(step.id) for step in original_steps
        ] == original_steps


@pytest.mark.parametrize("index", [0, 1, 2])
async def test_child_spawn_join_and_completion_boundaries(
    workspace: Path, execution_probe: ExecutionProbe, index: int
) -> None:
    runner = WorkflowRunner(workspace)
    await runner.run("children:deep_parent", workflow_id="root")
    identifier = str(runner.last_execution_id)
    original = await path(runner, identifier)
    assert [step.operation.kind for step in original] == [
        "workflow.spawn",
        "workflow.join",
        "workflow.complete",
    ]
    calls = list(execution_probe.calls)
    await runner.accept_branch(
        branching.Branch(
            execution_id=identifier,
            step_id=original[index].id,
            idempotency_key="child",
            action=branching.Restart(),
        )
    )
    assert await runner.resume(identifier) == 4
    selected = await path(runner, identifier)
    assert selected[:index] == original[:index]
    assert selected[index].id != original[index].id
    assert execution_probe.calls == (calls * 2 if index == 0 else calls)
    history = SQLiteHistory(
        workspace / "data/executions" / identifier / "history.sqlite3"
    )
    assert [await history.get_step(step.id) for step in original] == original


async def test_child_target_and_unfinished_join_rejected(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    runner = WorkflowRunner(workspace)
    task = asyncio.create_task(
        runner.run("children:parent", input={"values": [1], "block": True})
    )
    await asyncio.wait_for(execution_probe.started.get(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    identifier = str(runner.last_execution_id)
    record = await runner.execution_state(identifier)
    original = await path(runner, identifier)
    child_head = next(
        head
        for workflow, head in record.checkpoint.heads.items()
        if workflow != record.root_workflow_id
    )
    for target in (child_head, original[-1].id):
        with pytest.raises(branching.BranchConflict):
            await runner.accept_branch(
                branching.Branch(
                    execution_id=identifier,
                    step_id=target,
                    idempotency_key="invalid",
                    action=branching.Restart(),
                )
            )
    assert await runner.execution_state(identifier) == record


async def test_failed_operation_retry_preserves_failure(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    from vectorops.runner.runner.errors import ActivityError

    runner = WorkflowRunner(workspace)
    with pytest.raises(ActivityError):
        await runner.run("activities:unhandled")
    identifier = str(runner.last_execution_id)
    original = await path(runner, identifier)
    await runner.accept_branch(
        branching.Branch(
            execution_id=identifier,
            step_id=original[0].id,
            idempotency_key="retry",
            action=branching.Restart(),
        )
    )
    with pytest.raises(ActivityError):
        await runner.resume(identifier)
    assert (await path(runner, identifier))[0].id != original[0].id
    history = SQLiteHistory(
        workspace / "data/executions" / identifier / "history.sqlite3"
    )
    assert await history.get_step(original[0].id) == original[0]


@pytest.mark.parametrize("applied", [False, True])
async def test_accepted_branch_recovers_from_completed_execution(
    workspace: Path, execution_probe: ExecutionProbe, applied: bool
) -> None:
    runner = WorkflowRunner(workspace)
    await runner.run("activities:workflow")
    identifier = str(runner.last_execution_id)
    original = await path(runner, identifier)
    await runner.accept_branch(
        branching.Branch(
            execution_id=identifier,
            step_id=original[0].id,
            idempotency_key="recover",
            action=branching.Restart(),
        )
    )
    if applied:
        history = SQLiteHistory(
            workspace / "data/executions" / identifier / "history.sqlite3"
        )
        operation = await history.apply_branch()
        assert operation is not None and operation.status == "applied"
    assert [item.id for item in await runner.recovery_candidates()] == [identifier]
    async with RunnerService(workspace) as service:
        async with asyncio.timeout(5):
            while len(execution_probe.calls) < 4:
                await asyncio.sleep(0.01)
        await wait_status(service, identifier, "completed")
        assert (await path(service.runner, identifier))[0].id != original[0].id
