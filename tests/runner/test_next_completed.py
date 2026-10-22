import asyncio
from collections.abc import Mapping
from pathlib import Path
from typing import Optional
from unittest.mock import patch

import pydantic
import pytest

from tests.fixtures import next_completed_workflows as flows
from tests.fixtures.history import MemoryHistory
from vectorops.runner import models
from vectorops.runner.runner import context, handlers, history_query
from vectorops.runner.runner.cached_history import CachedHistory
from vectorops.runner.runner.errors import ReplayMismatchError
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner.runner.session import ExecutionSession
from vectorops.runner.runner.workflow import WorkflowRunner


@pytest.fixture(autouse=True)
def reset() -> None:
    flows.gates = {value: asyncio.Event() for value in (-1, 1, 2, 3)}
    flows.started = asyncio.Queue()
    flows.observed = asyncio.Queue()
    flows.calls = []
    flows.waiter = asyncio.Event()


@pytest.mark.parametrize("persistent", [False, True])
@pytest.mark.parametrize("all_ready", [False, True])
async def test_committed_order_and_replay(
    workspace: Path, history: MemoryHistory, persistent: bool, all_ready: bool
) -> None:
    runner = WorkflowRunner(workspace, None if persistent else history)
    task = asyncio.create_task(
        runner.run(
            "python:tests.fixtures.next_completed_workflows:pool",
            workflow_id="root",
            input={"all_ready": all_ready},
        )
    )
    try:
        assert {await asyncio.wait_for(flows.started.get(), 5) for _ in range(2)} == {
            1,
            2,
        }
        flows.gates[2].set()
        if all_ready:
            async with asyncio.timeout(5):
                while True:
                    if persistent:
                        identifier = runner.last_execution_id
                        assert identifier is not None
                        page = await runner.query_history(
                            history_query.HistoryQuery(
                                execution_id=identifier, include_children=True
                            )
                        )
                        steps = [item.step for item in page.items]
                    else:
                        steps = list(history.steps.values())
                    if any(
                        step.operation.kind == "workflow.complete"
                        and step.outcome.kind == "completed"
                        and step.outcome.result == 2
                        for step in steps
                    ):
                        break
                    await asyncio.sleep(0.01)
        else:
            assert await asyncio.wait_for(flows.observed.get(), 5) == 2
            assert not task.done()
        flows.gates[1].set()
        assert await asyncio.wait_for(task, 5) == [2, 1]
        before = list(flows.calls)
        if persistent:
            assert await asyncio.wait_for(WorkflowRunner(workspace).resume(), 5) == [
                2,
                1,
            ]
        else:
            appended = list(history.appended)
            assert await runner.run(
                "python:tests.fixtures.next_completed_workflows:pool",
                continue_from_step_id=history.heads["root"],
            ) == [2, 1]
            assert history.appended == appended
        assert flows.calls == before
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await runner.aclose()


async def test_multiple_observers_do_not_consume_child(workspace: Path) -> None:
    flows.gates[1].set()
    runner = WorkflowRunner(workspace)
    assert (
        await runner.run(
            "python:tests.fixtures.next_completed_workflows:repeated_observers"
        )
        is True
    )
    assert await WorkflowRunner(workspace).resume() is True
    assert flows.calls == [1]


@pytest.mark.parametrize("foreign", [False, True])
async def test_handle_must_match_recorded_spawn(
    history: MemoryHistory, foreign: bool
) -> None:
    async def unused(
        configuration: models.Workflow,
        *,
        continue_from_step_id: Optional[str] = None,
        child_heads: Optional[Mapping[str, str]] = None,
    ) -> models.WorkflowStep:
        raise AssertionError("validation must precede execution")

    parent = models.Workflow(id="parent", definition_ref="test:parent")
    await history.save_workflow(parent)
    await history.save_workflow(
        models.Workflow(id="child", definition_ref="test:child")
    )
    await history.append_step(
        models.WorkflowStep(
            id="spawn",
            workflow_id="other" if foreign else "parent",
            operation=models.WorkflowSpawnOperation(child_workflow_id="child"),
            outcome=models.StepCompleted(),
        )
    )
    session = ExecutionSession(
        history, parent, unused, continue_from_step_id=None, child_heads={}
    )
    handle = context.ChildWorkflowHandle(
        "parent", "child" if foreign else "wrong", "spawn"
    )
    before = list(history.appended)
    with pytest.raises((ValueError, ReplayMismatchError)):
        await handlers.NextCompletedHandler(session).execute(
            context.NextCompletedRequest(
                models.WorkflowNextCompletedOperation(spawn_step_ids=["spawn"]),
                (handle,),
            )
        )
    assert history.appended == before


async def test_refill_does_not_wait_for_oldest(workspace: Path) -> None:
    runner = WorkflowRunner(workspace)
    task = asyncio.create_task(
        runner.run(
            "python:tests.fixtures.next_completed_workflows:pool",
            input={"refill": True},
        )
    )
    try:
        for _ in range(2):
            await asyncio.wait_for(flows.started.get(), 5)
        flows.gates[2].set()
        assert await asyncio.wait_for(flows.started.get(), 5) == 3
        assert await asyncio.wait_for(flows.observed.get(), 5) == 2
        flows.gates[3].set()
        assert await asyncio.wait_for(flows.observed.get(), 5) == 3
        flows.gates[1].set()
        assert await asyncio.wait_for(task, 5) == [2, 3, 1]
        assert await asyncio.wait_for(WorkflowRunner(workspace).resume(), 5) == [
            2,
            3,
            1,
        ]
        assert sorted(flows.calls) == [1, 2, 3]
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await runner.aclose()


@pytest.mark.parametrize(
    "name,expected",
    [("outcomes", ["failed", "cancelled"]), ("cancelled_waiter", "completed")],
)
async def test_outcomes_and_wait_only_cancellation(
    workspace: Path, name: str, expected: object
) -> None:
    flows.gates[-1].set()
    runner = WorkflowRunner(workspace)
    assert (
        await asyncio.wait_for(
            runner.run(f"python:tests.fixtures.next_completed_workflows:{name}"), 5
        )
        == expected
    )
    before = list(flows.calls)
    assert await asyncio.wait_for(WorkflowRunner(workspace).resume(), 5) == expected
    assert flows.calls == before


@pytest.mark.parametrize("persisted", [False, True])
async def test_interrupted_selection_recovers(workspace: Path, persisted: bool) -> None:
    reached = asyncio.Event()
    original = ExecutionSession.update

    async def interrupt(self: ExecutionSession, step: models.WorkflowStep) -> None:
        if step.operation.kind == "workflow.next_completed" and not reached.is_set():
            if persisted:
                await original(self, step)
            reached.set()
            await asyncio.Event().wait()
        await original(self, step)

    runner = WorkflowRunner(workspace)
    with patch.object(ExecutionSession, "update", interrupt):
        task = asyncio.create_task(
            runner.run("python:tests.fixtures.next_completed_workflows:pool")
        )
        for _ in range(2):
            await asyncio.wait_for(flows.started.get(), 5)
        flows.gates[2].set()
        await asyncio.wait_for(reached.wait(), 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)
    flows.gates[1].set()
    assert await asyncio.wait_for(WorkflowRunner(workspace).resume(), 5) == [2, 1]
    assert flows.calls.count(2) == 1


@pytest.mark.parametrize("selected", [[], ["a", "b"], ["outside"]])
def test_selection_requires_one_candidate(selected: list[str]) -> None:
    with pytest.raises(pydantic.ValidationError):
        models.WorkflowStep(
            id="next",
            workflow_id="parent",
            operation=models.WorkflowNextCompletedOperation(spawn_step_ids=["a", "b"]),
            outcome=models.StepJoined(
                child_completions=[
                    models.ChildWorkflowCompletion(
                        spawn_step_id=value, completion_step_id="done-" + value
                    )
                    for value in selected
                ]
            ),
        )


@pytest.mark.parametrize("sqlite", [False, True])
async def test_commit_order_precedes_callback_order_and_waits_for_cleanup(
    tmp_path: Path, sqlite: bool
) -> None:
    backend = SQLiteHistory(tmp_path / "history.sqlite3") if sqlite else MemoryHistory()
    async with CachedHistory(backend) as history:
        parent = models.Workflow(id="parent", definition_ref="test:parent")
        await history.save_workflow(parent)
        children = [
            models.Workflow(id=name, definition_ref="test:child") for name in ("a", "b")
        ]
        for child in children:
            await history.save_workflow(child)
            await history.append_step(
                models.WorkflowStep(
                    id="spawn-" + child.id,
                    workflow_id="parent",
                    operation=models.WorkflowSpawnOperation(child_workflow_id=child.id),
                    outcome=models.StepCompleted(),
                )
            )
        committed = asyncio.Event()
        cleaned = asyncio.Event()
        selected = asyncio.Event()
        original = history.first_completion

        async def observe(ids: list[str]) -> Optional[models.WorkflowStep]:
            result = await original(ids)
            if result is not None:
                selected.set()
            return result

        async def execute(
            configuration: models.Workflow,
            *,
            continue_from_step_id: Optional[str] = None,
            child_heads: Optional[Mapping[str, str]] = None,
        ) -> models.WorkflowStep:
            if configuration.id == "b":
                await committed.wait()
            step = models.WorkflowStep(
                id="done-" + configuration.id,
                workflow_id=configuration.id,
                operation=models.WorkflowCompleteOperation(),
                outcome=models.StepCompleted(result=configuration.id),
            )
            await history.append_step(step)
            if configuration.id == "a":
                committed.set()
                await cleaned.wait()
            return step

        session = ExecutionSession(
            history, parent, execute, continue_from_step_id=None, child_heads={}
        )
        handler = handlers.NextCompletedHandler(session)
        handles = tuple(
            context.ChildWorkflowHandle("parent", name, "spawn-" + name)
            for name in ("b", "a")
        )
        deferred = await handler.execute(
            context.NextCompletedRequest(
                models.WorkflowNextCompletedOperation(
                    spawn_step_ids=[h.spawn_step_id for h in handles]
                ),
                handles,
            )
        )
        with patch.object(history, "first_completion", observe):
            task = asyncio.ensure_future(deferred.invoke())
            try:
                await asyncio.wait_for(selected.wait(), 5)
                assert not task.done()
                cleaned.set()
                response = await asyncio.wait_for(task, 5)
                assert response.kind == "next_completed"
                assert response.completion.handle.workflow_id == "a"
            finally:
                cleaned.set()
                await asyncio.gather(
                    *[session.start_child(child) for child in children],
                    return_exceptions=True,
                )
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)


async def test_recovery_before_next_call_keeps_committed_fifo(workspace: Path) -> None:
    runner = WorkflowRunner(workspace)
    task = asyncio.create_task(
        runner.run("python:tests.fixtures.next_completed_workflows:recover_ready")
    )
    try:
        for _ in range(2):
            await asyncio.wait_for(flows.started.get(), 5)
        flows.gates[2].set()
        identifier = runner.last_execution_id
        assert identifier is not None
        async with asyncio.timeout(5):
            while True:
                page = await runner.query_history(
                    history_query.HistoryQuery(
                        execution_id=identifier, include_children=True
                    )
                )
                if any(
                    item.step.operation.kind == "workflow.complete"
                    for item in page.items
                ):
                    break
                await asyncio.sleep(0.01)
        flows.gates[1].set()
        async with asyncio.timeout(5):
            while True:
                page = await runner.query_history(
                    history_query.HistoryQuery(execution_id=identifier)
                )
                if any(
                    item.step.operation.kind == "test.next.pause" for item in page.items
                ):
                    break
                await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        flows.waiter.set()
        assert await WorkflowRunner(workspace).resume(identifier) == {
            "kind": "completed",
            "result": 2,
        }
        assert sorted(flows.calls) == [1, 2]
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await runner.aclose()
