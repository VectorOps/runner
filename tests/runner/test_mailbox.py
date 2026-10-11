import asyncio
from pathlib import Path

import pydantic
import pytest

from vectorops.runner import models
from vectorops.runner.runner import execution_state, mailbox
from vectorops.runner.runner.sqlite_history import SQLiteHistory


async def store(path: Path) -> SQLiteHistory:
    database = SQLiteHistory(path)
    await database.save_workflow(
        models.Workflow(id="workflow", definition_ref="test:workflow")
    )
    await database.save_execution(
        execution_state.ExecutionRecord(id="execution", root_workflow_id="workflow")
    )
    await database.append_step(
        models.WorkflowStep(
            id="request",
            workflow_id="workflow",
            operation=models.StepOperation(kind=mailbox.KIND, name="request_input"),
            outcome=models.StepPending(),
            deferred_delivery=True,
        )
    )
    return database


def answer(value: pydantic.JsonValue = "hello", key: str = "answer") -> mailbox.Answer:
    return mailbox.Answer(
        execution_id="execution",
        request_id="request",
        response=value,
        idempotency_key=key,
    )


async def test_response_is_durable_atomic_and_idempotent(tmp_path: Path) -> None:
    database = await store(tmp_path / "history.sqlite3")
    with pytest.raises(KeyError):
        await database.answer_input(answer())
    request = await database.register_input(
        "request", mailbox.InputSpec(channel="user.message")
    )
    with pytest.raises(ValueError, match="text response"):
        await database.answer_input(answer(12))
    results = await asyncio.gather(
        database.answer_input(answer()),
        database.answer_input(answer("other", "other")),
        return_exceptions=True,
    )
    assert sum(isinstance(result, mailbox.InputRequest) for result in results) == 1
    assert sum(isinstance(result, mailbox.MailboxConflict) for result in results) == 1
    reopened = SQLiteHistory(database.path)
    accepted = await reopened.get_input(request.request_id)
    assert accepted.status == "answered"
    retry = answer(accepted.response, accepted.idempotency_key or "")
    assert await reopened.answer_input(retry) == accepted
    assert (
        await mailbox.Mailbox(reopened).wait(
            "request", mailbox.InputSpec(channel="user.message")
        )
        == accepted.response
    )
    assert not (
        await reopened.list_inputs(mailbox.RequestQuery(execution_id="execution"))
    ).requests


async def test_cancelled_requests_reject_answers(tmp_path: Path) -> None:
    database = await store(tmp_path / "history.sqlite3")
    await database.register_input("request", mailbox.InputSpec(channel="approval"))
    await database.cancel_inputs()
    with pytest.raises(mailbox.MailboxConflict, match="closed"):
        await database.answer_input(answer())
    with pytest.raises(mailbox.MailboxConflict, match="contract changed"):
        await database.register_input("request", mailbox.InputSpec(channel="another"))


async def test_json_null_is_an_answer(tmp_path: Path) -> None:
    database = await store(tmp_path / "history.sqlite3")
    spec = mailbox.InputSpec(channel="data", response_type="json")
    await database.register_input("request", spec)
    await database.answer_input(answer(None))
    assert await mailbox.Mailbox(database).wait("request", spec) is None


async def test_pause_answer_and_stale_checkpoint_are_serialized(tmp_path: Path) -> None:
    database = await store(tmp_path / "history.sqlite3")
    await database.register_input("request", mailbox.InputSpec(channel="user.message"))
    checkpoint = await database.get_execution()
    assert await database.set_paused({"request"})
    await database.save_execution(checkpoint, preserve_status=True)
    assert (await database.get_execution()).status == "paused"
    with pytest.raises(ValueError):
        await database.answer_input(answer(123))
    assert (await database.get_execution()).status == "paused"
    paused = await database.get_execution()
    await asyncio.gather(
        database.answer_input(answer()),
        database.set_paused({"request"}),
        database.save_execution(paused, preserve_status=True),
    )
    assert (await database.get_execution()).status == "running"
    assert not await database.set_paused({"request"})
    await database.save_execution(checkpoint.model_copy(update={"status": "completed"}))
    await database.answer_input(answer())
    assert (await database.get_execution()).status == "completed"


@pytest.mark.parametrize("channel", ["", "two words", "\n"])
def test_invalid_channel(channel: str) -> None:
    with pytest.raises(pydantic.ValidationError):
        mailbox.InputSpec(channel=channel)


async def test_notifications_cannot_break_durable_wait(tmp_path: Path) -> None:
    database = await store(tmp_path / "history.sqlite3")
    announced = asyncio.Event()

    def broken_sink(event: dict[str, pydantic.JsonValue]) -> None:
        announced.set()
        raise RuntimeError("host unavailable")

    owner = mailbox.Mailbox(database, broken_sink)
    task = asyncio.create_task(
        owner.wait("request", mailbox.InputSpec(channel="user.message"))
    )
    try:
        await asyncio.wait_for(announced.wait(), 5)
        await owner.answer(answer())
        assert await asyncio.wait_for(task, 5) == "hello"
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
