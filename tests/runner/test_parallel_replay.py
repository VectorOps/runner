import asyncio
from collections.abc import Mapping
from pathlib import Path
from typing import Optional

import pydantic
import pytest

from tests.fixtures.history import MemoryHistory
from tests.fixtures.probe import ExecutionProbe
from vectorops.runner import models
from vectorops.runner.runner import context
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.errors import ActivityError
from vectorops.runner.runner.session import ExecutionSession
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner.runner.workflow import WorkflowRunner


@pytest.mark.parametrize("mode", ["children", "activities", "mixed"])
async def test_branch_delivery_order_survives_replay(
    workspace: Path, execution_probe: ExecutionProbe, mode: str
) -> None:
    execution_probe.gates = {1: asyncio.Event(), 2: asyncio.Event()}
    runner = WorkflowRunner(workspace)
    task = asyncio.create_task(
        runner.run("composed:branches", input={"mode": mode}, workflow_id="root")
    )
    assert {
        await asyncio.wait_for(execution_probe.started.get(), 2),
        await asyncio.wait_for(execution_probe.started.get(), 2),
    } == {1, 2}
    execution_probe.gates[2].set()
    assert await asyncio.wait_for(execution_probe.finished.get(), 2) == 2
    assert await asyncio.wait_for(execution_probe.finished.get(), 2) == 14
    execution_probe.gates[1].set()
    assert await asyncio.wait_for(task, 2) == [24, 28]
    before = list(execution_probe.calls)
    assert await asyncio.wait_for(WorkflowRunner(workspace).resume(), 2) == [24, 28]
    assert execution_probe.calls == before


async def test_nested_fanout_executes_leaves_concurrently_and_replays(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    runner = WorkflowRunner(workspace)
    task = asyncio.create_task(runner.run("composed:fanout", workflow_id="root"))
    started = {
        await asyncio.wait_for(execution_probe.started.get(), 2) for _ in range(4)
    }
    assert started == {4, 5, 6, 7}
    assert execution_probe.peak_active == 4
    execution_probe.gate.set()
    assert await asyncio.wait_for(task, 2) == 176
    before = list(execution_probe.calls)
    assert await asyncio.wait_for(WorkflowRunner(workspace).resume(), 2) == 176
    assert execution_probe.calls == before


async def test_multiple_pending_activities_recover_with_stable_identities(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    runner = WorkflowRunner(workspace)
    task = asyncio.create_task(
        runner.run("composed:parallel_activities", workflow_id="root")
    )
    for _ in range(2):
        await asyncio.wait_for(execution_probe.started.get(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    execution_id = str(runner.last_execution_id)
    database = SQLiteHistory(
        workspace / "data/executions" / execution_id / "history.sqlite3"
    )
    record = await database.get_execution()
    pending = [
        step
        async for step in database.replay_path("root", record.checkpoint.heads["root"])
    ]
    assert len(pending) == 2
    assert all(step.outcome.kind == "pending" for step in pending)
    execution_probe.gate.set()
    assert await asyncio.wait_for(
        WorkflowRunner(workspace).resume(execution_id), 2
    ) == [2, 4]
    for step in pending:
        assert (await database.get_step(step.id)).outcome.kind == "completed"
    before = list(execution_probe.calls)
    assert await asyncio.wait_for(
        WorkflowRunner(workspace).resume(execution_id), 2
    ) == [2, 4]
    assert execution_probe.calls == before


async def test_mixed_streaming_and_child_calls_replay(
    runner: WorkflowRunner, history: MemoryHistory, execution_probe: ExecutionProbe
) -> None:
    assert await runner.run("composed:mixed_streaming", workflow_id="root") == [
        1,
        4,
        3,
    ]
    before = list(execution_probe.calls)
    assert await runner.run(
        "composed:mixed_streaming", continue_from_step_id=history.heads["root"]
    ) == [1, 4, 3]
    assert execution_probe.calls == before


async def test_shutdown_settles_all_queued_submitters(history: MemoryHistory) -> None:
    async def unused_child(
        configuration: models.Workflow,
        *,
        continue_from_step_id: Optional[str] = None,
        child_heads: Optional[Mapping[str, str]] = None,
    ) -> models.WorkflowStep:
        raise AssertionError("no children")

    session = ExecutionSession(
        history,
        models.Workflow(id="root", definition_ref="test"),
        unused_child,
        continue_from_step_id=None,
        child_heads={},
    )
    submitted: list[asyncio.Task[context.OperationResponse]] = []

    async def blocked(ctx: ActivityExecutionContext) -> pydantic.JsonValue:
        await asyncio.Event().wait()
        return None

    async def invoke() -> pydantic.JsonValue:
        for value in range(100):
            submitted.append(
                asyncio.create_task(
                    session.submit(
                        context.ActivityRequest(
                            models.StepOperation(
                                kind="activity", name="blocked", input={"value": value}
                            ),
                            blocked,
                        )
                    )
                )
            )
        await asyncio.sleep(0)
        return "done"

    before = set(asyncio.all_tasks())
    assert (
        await asyncio.wait_for(session.run(invoke), 2)
    ).outcome == models.StepCompleted(result="done")
    await asyncio.sleep(0)
    assert all(task.done() for task in submitted)
    assert session.queue.empty()
    assert not set(asyncio.all_tasks()) - before


def test_resume_record_validation_and_legacy_defaults() -> None:
    step = models.WorkflowStep(
        id="resume",
        workflow_id="root",
        operation=models.WorkflowResumeOperation(step_id="activity"),
        outcome=models.StepCompleted(),
    )
    assert models.WorkflowStep.model_validate_json(step.model_dump_json()) == step
    assert not step.deferred_delivery
    with pytest.raises(pydantic.ValidationError):
        models.WorkflowStep.model_validate(
            {**step.model_dump(), "deferred_delivery": True}
        )
    with pytest.raises(pydantic.ValidationError):
        models.WorkflowStep.model_validate({**step.model_dump(), "id": "activity"})


@pytest.mark.parametrize("callable_name", ["fail_fast", "group_failure"])
async def test_parallel_failure_replays_without_restarting_abandoned_children(
    workspace: Path, execution_probe: ExecutionProbe, callable_name: str
) -> None:
    runner = WorkflowRunner(workspace)
    with pytest.raises(ActivityError):
        await asyncio.wait_for(runner.run(f"composed:{callable_name}"), 2)
    before = list(execution_probe.calls)
    with pytest.raises(ActivityError):
        await asyncio.wait_for(WorkflowRunner(workspace).resume(), 2)
    assert execution_probe.calls == before
    assert execution_probe.active == 0


async def test_first_completed_replays_the_same_winner(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    execution_probe.gates = {1: asyncio.Event(), 2: asyncio.Event()}
    runner = WorkflowRunner(workspace)
    task = asyncio.create_task(runner.run("composed:first_completed"))
    for _ in range(2):
        await asyncio.wait_for(execution_probe.started.get(), 2)
    execution_probe.gates[2].set()
    assert await asyncio.wait_for(task, 2) == 8
    before = list(execution_probe.calls)
    assert await asyncio.wait_for(WorkflowRunner(workspace).resume(), 2) == 8
    assert execution_probe.calls == before
    assert execution_probe.active == 0


async def test_interrupted_nested_fanout_recovers(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    runner = WorkflowRunner(workspace)
    task = asyncio.create_task(runner.run("composed:fanout"))
    for _ in range(4):
        await asyncio.wait_for(execution_probe.started.get(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert execution_probe.active == 0
    execution_probe.gate.set()
    assert await asyncio.wait_for(WorkflowRunner(workspace).resume(), 2) == 176
    before = list(execution_probe.calls)
    assert await asyncio.wait_for(WorkflowRunner(workspace).resume(), 2) == 176
    assert execution_probe.calls == before
