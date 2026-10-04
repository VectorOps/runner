import asyncio
from pathlib import Path

import pydantic
import pytest

from tests.fixtures.history import MemoryHistory
from tests.fixtures.probe import ExecutionProbe
from vectorops.runner import models as m
from vectorops.runner.runner.events import StepEvent
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner.runner.workflow import WorkflowRunner
from vectorops.runner.runner.errors import ActivityError


@pytest.mark.parametrize("callable_name", ["gathered", "tasked", "grouped"])
async def test_asyncio_primitives_wait_for_concurrent_children(
    runner: WorkflowRunner,
    history: MemoryHistory,
    execution_probe: ExecutionProbe,
    callable_name: str,
) -> None:
    execution_probe.gates = {1: asyncio.Event(), 2: asyncio.Event()}
    task = asyncio.create_task(
        runner.run(f"composed:{callable_name}", workflow_id="root")
    )
    assert {
        await asyncio.wait_for(execution_probe.started.get(), 2),
        await asyncio.wait_for(execution_probe.started.get(), 2),
    } == {1, 2}
    assert execution_probe.peak_active == 2
    execution_probe.gates[1].set()
    assert await asyncio.wait_for(execution_probe.finished.get(), 2) == 1
    assert not task.done()
    execution_probe.gates[2].set()
    assert await asyncio.wait_for(task, 2) == [4, 2]
    path = history.path(history.heads["root"])
    assert sum(step.operation.kind == "workflow.join" for step in path) == 2
    assert all(step.outcome.kind != "waiting" for step in path)
    before = list(history.appended)
    assert await runner.run(
        f"composed:{callable_name}", continue_from_step_id=path[-1].id
    ) == [4, 2]
    assert history.appended == before
    assert sorted(execution_probe.calls) == [1, 2]


async def test_gather_collects_independent_child_errors(
    runner: WorkflowRunner, history: MemoryHistory, execution_probe: ExecutionProbe
) -> None:
    assert await runner.run("composed:collected", workflow_id="root") == [
        "ValueError: negative child",
        "4",
    ]
    assert await runner.run(
        "composed:collected", continue_from_step_id=history.heads["root"]
    ) == ["ValueError: negative child", "4"]
    assert sorted(execution_probe.calls) == [-1, 2]


async def test_concurrent_streaming_children_emit_updates_and_replay(
    workspace: Path, history: MemoryHistory, execution_probe: ExecutionProbe
) -> None:
    events: list[StepEvent] = []
    runner = WorkflowRunner(workspace, history, event_sink=events.append)
    assert await runner.run("composed:streamed_gather", workflow_id="root") == [2, 1]
    assert any(event.kind == "step.updated" for event in events)
    before = list(events)
    assert await runner.run(
        "composed:streamed_gather", continue_from_step_id=history.heads["root"]
    ) == [2, 1]
    assert events == before


async def test_persistent_multiple_pending_joins_recover_without_head_regression(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    joins: asyncio.Queue[str] = asyncio.Queue()

    def event_sink(event: StepEvent) -> None:
        if event.step.workflow_id == "root" and event.step.outcome.kind == "waiting":
            joins.put_nowait(event.step.id)

    runner = WorkflowRunner(workspace, event_sink=event_sink)
    task = asyncio.create_task(runner.run("composed:gathered", workflow_id="root"))
    await asyncio.wait_for(execution_probe.started.get(), 2)
    await asyncio.wait_for(execution_probe.started.get(), 2)
    await asyncio.wait_for(joins.get(), 2)
    await asyncio.wait_for(joins.get(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 2)
    execution_id = str(runner.last_execution_id)
    record = await runner.execution_state(execution_id)
    database = SQLiteHistory(
        workspace / "data/executions" / execution_id / "history.sqlite3"
    )
    old_head = record.checkpoint.heads["root"]
    path = [step async for step in database.replay_path("root", old_head)]
    assert sum(step.outcome.kind == "waiting" for step in path) == 2
    execution_probe.gate.set()
    assert await WorkflowRunner(workspace).resume(execution_id) == [4, 2]
    resumed = await database.get_execution()
    assert resumed.completion_step_id is not None
    assert resumed.checkpoint.heads["root"] == resumed.completion_step_id
    assert (await database.get_step(old_head)).outcome.kind == "joined"
    assert await WorkflowRunner(workspace).resume(execution_id) == [4, 2]


def test_waiting_outcome_is_join_only() -> None:
    record = m.WorkflowStep(
        id="join",
        workflow_id="root",
        operation=m.WorkflowJoinOperation(spawn_step_ids=["spawn"]),
        outcome=m.StepWaiting(),
    )
    assert m.WorkflowStep.model_validate_json(record.model_dump_json()) == record
    with pytest.raises(pydantic.ValidationError):
        m.WorkflowStep(
            id="invalid",
            workflow_id="root",
            operation=m.StepOperation(kind="activity", name="work"),
            outcome=m.StepWaiting(),
        )


async def test_gather_failure_does_not_wait_for_unfinished_sibling(
    runner: WorkflowRunner, execution_probe: ExecutionProbe
) -> None:
    before = set(asyncio.all_tasks())
    with pytest.raises(ActivityError, match="negative child"):
        await asyncio.wait_for(runner.run("composed:fail_fast"), 2)
    assert execution_probe.active == 0
    assert not set(asyncio.all_tasks()) - before


async def test_many_child_calls_do_not_drop_requests(
    runner: WorkflowRunner, history: MemoryHistory, execution_probe: ExecutionProbe
) -> None:
    expected = [value * 2 for value in range(20)]
    assert await runner.run("composed:many_children", workflow_id="root") == expected
    before = list(history.appended)
    assert (
        await runner.run(
            "composed:many_children", continue_from_step_id=history.heads["root"]
        )
        == expected
    )
    assert history.appended == before
    assert sorted(execution_probe.calls) == list(range(20))
