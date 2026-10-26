import asyncio
from collections.abc import Callable
from pathlib import Path
import sqlite3

import pydantic
import pytest

from tests.runner.test_mailbox import store
from tests.runner.test_usage import generation, seed
from vectorops.runner import models
from vectorops.runner.runner import branching, events, mailbox
from vectorops.runner.runner import summary_history
from vectorops.runner.runner.sqlite_history import SQLiteHistory


async def test_changes_only_follow_committed_writes(tmp_path: Path) -> None:
    changes: list[tuple[str, ...]] = []
    database = SQLiteHistory(tmp_path / "history.sqlite3", change_sink=changes.append)
    await seed(database)
    changes.clear()
    await database.set_workflow_name("root", "Committed")
    assert changes == [("root",)]
    assert (await SQLiteHistory(database.path).get_workflow("root")).name == "Committed"
    changes.clear()
    await database.set_workflow_name("root", "Ignored")
    await database.save_workflow(await database.get_workflow("root"))
    assert not changes
    step = generation("pending", streaming=True)
    await database.append_step(step)
    changes.clear()
    with pytest.raises(KeyError):
        await database.update_steps((generation("pending"), generation("missing")))
    assert not changes
    assert (await database.get_step("pending")).outcome.kind == "streaming"


async def test_commit_notification_survives_caller_cancellation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    changes: list[tuple[str, ...]] = []
    database = SQLiteHistory(tmp_path / "history.sqlite3", change_sink=changes.append)
    await seed(database)
    changes.clear()
    entered = asyncio.Event()
    release = asyncio.Event()
    original = database._invoke

    async def hold[T](operation: Callable[[sqlite3.Connection], T]) -> T:
        result = await original(operation)
        entered.set()
        await release.wait()
        return result

    monkeypatch.setattr(database, "_invoke", hold)
    task = asyncio.create_task(database.set_workflow_name("root", "Committed"))
    await entered.wait()
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert changes == [("root",)]


@pytest.mark.parametrize("operation", ["selective", "bulk", "stop", "branch"])
async def test_cancellation_events_identify_only_changed_requests(
    tmp_path: Path, operation: str
) -> None:
    original = await store(tmp_path / "history.sqlite3")
    for identifier in ("second", "answered"):
        await original.append_step(
            models.WorkflowStep(
                id=identifier,
                workflow_id="workflow",
                operation=models.StepOperation(kind=mailbox.KIND, name="request_input"),
                outcome=models.StepPending(),
                deferred_delivery=True,
            )
        )
    for identifier in ("request", "second", "answered"):
        await original.register_input(identifier, mailbox.InputSpec(channel="approval"))
    await original.answer_input(
        mailbox.Answer(
            execution_id="execution",
            request_id="answered",
            response="yes",
            idempotency_key="answer",
        )
    )
    cancelled: list[mailbox.InputsCancelled] = []

    def receive(event: dict[str, pydantic.JsonValue]) -> None:
        snapshot = mailbox.InputsCancelled.model_validate(event)
        with sqlite3.connect(original.path) as connection:
            for identifier in snapshot.request_ids:
                payload = connection.execute(
                    "SELECT payload FROM input_requests WHERE request_id = ?",
                    (identifier,),
                ).fetchone()[0]
                assert (
                    mailbox.StoredInputRequest.model_validate_json(payload).status
                    == "cancelled"
                )
        cancelled.append(snapshot)

    database = SQLiteHistory(original.path, mailbox_sink=receive)
    if operation == "stop":
        await database.stop_execution()
    elif operation == "branch":
        await database.accept_branch(
            branching.Branch(
                execution_id="execution",
                step_id="request",
                idempotency_key="branch",
                action=branching.Restart(),
            )
        )
        await database.apply_branch()
    else:
        await database.cancel_inputs("request" if operation == "selective" else None)
    expected = ["request"] if operation == "selective" else ["request", "second"]
    assert cancelled == [
        mailbox.InputsCancelled(execution_id="execution", request_ids=expected)
    ]
    assert (await database.get_input("answered")).status == "answered"
    if operation == "selective":
        assert (await database.get_input("second")).status == "pending"
    await database.cancel_inputs("request")
    await database.cancel_inputs("missing")
    assert len(cancelled) == 1


async def test_failing_cancellation_sink_does_not_hide_branch_snapshot(
    tmp_path: Path,
) -> None:
    original = await store(tmp_path / "history.sqlite3")
    await original.register_input("request", mailbox.InputSpec(channel="approval"))
    snapshots: list[events.StepEvent] = []

    def broken_sink(event: dict[str, pydantic.JsonValue]) -> None:
        raise RuntimeError("sink unavailable")

    database = SQLiteHistory(
        original.path, event_sink=snapshots.append, mailbox_sink=broken_sink
    )
    await database.accept_branch(
        branching.Branch(
            execution_id="execution",
            step_id="request",
            idempotency_key="branch",
            action=branching.InsertInput(response="edited"),
        )
    )
    result = await database.apply_branch()
    assert result is not None and result.status == "applied"
    assert (await original.get_input("request")).status == "cancelled"
    assert len(snapshots) == 1
    assert snapshots[0].step.id == result.input_step_id


@pytest.mark.parametrize("writer", [False, True])
@pytest.mark.parametrize("operation", ["cancel", "stop", "branch"])
async def test_durable_events_survive_caller_cancellation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, writer: bool, operation: str
) -> None:
    original = await store(tmp_path / "history.sqlite3")
    await original.register_input("request", mailbox.InputSpec(channel="approval"))
    cancelled: list[mailbox.InputsCancelled] = []
    snapshots: list[events.StepEvent] = []
    database = SQLiteHistory(
        original.path,
        event_sink=snapshots.append,
        mailbox_sink=lambda event: cancelled.append(
            mailbox.InputsCancelled.model_validate(event)
        ),
    )
    branch = await database.accept_branch(
        branching.Branch(
            execution_id="execution",
            step_id="request",
            idempotency_key="branch",
            action=branching.InsertInput(response="edited"),
        )
    )
    entered = asyncio.Event()
    release = asyncio.Event()
    invoke = database._invoke

    async def hold[T](operation: Callable[[sqlite3.Connection], T]) -> T:
        result = await invoke(operation)
        entered.set()
        await release.wait()
        return result

    monkeypatch.setattr(database, "_invoke", hold)
    if writer:
        await database.__aenter__()
    try:
        if operation == "branch":
            task = asyncio.create_task(database.apply_branch())
        elif operation == "stop":
            task = asyncio.create_task(database.stop_execution())
        else:
            task = asyncio.create_task(database.cancel_inputs("request"))
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert cancelled == [
            mailbox.InputsCancelled(execution_id="execution", request_ids=["request"])
        ]
        if operation == "branch":
            assert len(snapshots) == 1
            snapshot = snapshots[0]
            assert snapshot.kind == "step.completed"
            assert snapshot.execution_id == "execution"
            assert snapshot.step.id == branch.input_step_id
            assert snapshot.step == await original.get_step(snapshot.step.id)
            assert snapshot.position == await original.get_step_position(
                snapshot.step.id
            )
        else:
            assert not snapshots
    finally:
        await database.aclose()


@pytest.mark.parametrize("failure", ["plan", "transaction"])
async def test_failed_branches_do_not_publish_durable_events(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    original = await store(tmp_path / "history.sqlite3")
    await original.register_input("request", mailbox.InputSpec(channel="approval"))
    snapshots: list[events.StepEvent] = []
    cancelled: list[mailbox.InputsCancelled] = []
    database = SQLiteHistory(
        original.path,
        event_sink=snapshots.append,
        mailbox_sink=lambda event: cancelled.append(
            mailbox.InputsCancelled.model_validate(event)
        ),
    )
    operation = await database.accept_branch(
        branching.Branch(
            execution_id="execution",
            step_id="request",
            idempotency_key="branch",
            action=branching.InsertInput(response="edited"),
        )
    )
    if failure == "plan":

        def invalid_plan(
            connection: sqlite3.Connection, operation: branching.Operation
        ) -> branching.Plan:
            raise ValueError("invalid branch")

        monkeypatch.setattr(database, "_branch_plan", invalid_plan)
        result = await database.apply_branch()
        assert result is not None and result.status == "failed"
    else:

        def fail_status(
            connection: sqlite3.Connection,
            status: str,
            changes: summary_history.Changes,
        ) -> None:
            raise RuntimeError("transaction failed")

        monkeypatch.setattr(database, "_set_status", fail_status)
        with pytest.raises(RuntimeError, match="transaction failed"):
            await database.apply_branch()
    assert not snapshots
    assert not cancelled
    assert (await original.get_input("request")).status == "pending"
    assert operation.input_step_id is not None
    with pytest.raises(KeyError):
        await original.get_step(operation.input_step_id)
