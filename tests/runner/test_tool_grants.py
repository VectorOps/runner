import asyncio
from pathlib import Path

import pydantic
import pytest

from tests.runner.test_mailbox import store
from vectorops.runner import models, tool_policy
from vectorops.runner.runner import mailbox
from vectorops.runner.runner.sqlite_history import SQLiteHistory


async def request_tool(
    database: SQLiteHistory, kind: str = "llm.tool"
) -> mailbox.InputRequest:
    await database.append_step(
        models.WorkflowStep(
            id="tool",
            workflow_id="workflow",
            operation=models.StepOperation(kind=kind, name="invoke_tool"),
            outcome=models.StepPending(),
            deferred_delivery=True,
        )
    )
    return await database.register_input(
        "tool",
        mailbox.InputSpec(
            channel="custom.approval",
            response_type="json",
            payload={"call": {"name": "test"}, "tool": {"name": "test"}},
        ),
    )


def answer(value: pydantic.JsonValue, key: str = "answer") -> mailbox.Answer:
    return mailbox.Answer(
        execution_id="execution",
        request_id="tool",
        response=value,
        idempotency_key=key,
    )


@pytest.mark.parametrize(
    "value,granted",
    [
        ({"approved": True, "scope": "workflow"}, True),
        ({"approved": True}, False),
        ({"approved": True, "scope": "call"}, False),
        ({"approved": False}, False),
        ({"approved": False, "scope": "workflow"}, False),
        ({"approved": "true", "scope": "workflow"}, False),
        ({"approved": True, "scope": "all"}, False),
        ({"approved": True, "scope": "workflow", "workflow_id": "other"}, False),
    ],
)
async def test_grant_is_atomic_durable_and_workflow_scoped(
    tmp_path: Path, value: pydantic.JsonValue, granted: bool
) -> None:
    database = await store(tmp_path / "history.sqlite3")
    request = await request_tool(database)
    assert not await database.tool_approved(request.workflow_id, "test")
    receipt = await database.answer_input(answer(value))
    reopened = SQLiteHistory(database.path)
    assert await reopened.tool_approved(request.workflow_id, "test") is granted
    assert not await reopened.tool_approved("child", "test")
    assert not await reopened.tool_approved(request.workflow_id, "other")
    assert await reopened.answer_input(answer(value)) == receipt
    other_execution = await store(tmp_path / "other.sqlite3")
    assert not await other_execution.tool_approved(request.workflow_id, "test")


async def test_competing_answers_only_winner_can_grant(tmp_path: Path) -> None:
    database = await store(tmp_path / "history.sqlite3")
    await request_tool(database)
    results = await asyncio.gather(
        database.answer_input(answer({"approved": False}, "reject")),
        database.answer_input(answer({"approved": True, "scope": "workflow"}, "grant")),
        return_exceptions=True,
    )
    assert sum(isinstance(result, mailbox.MailboxConflict) for result in results) == 1
    winner = await database.get_input("tool")
    assert await database.tool_approved("workflow", "test") is (
        tool_policy.workflow_grant(winner.payload, winner.response) is not None
    )


async def test_ordinary_mailbox_cannot_create_tool_grant(tmp_path: Path) -> None:
    database = await store(tmp_path / "history.sqlite3")
    await request_tool(database, kind=mailbox.KIND)
    await database.answer_input(answer({"approved": True, "scope": "workflow"}))
    assert not await database.tool_approved("workflow", "test")


async def test_cancelled_or_wrong_execution_answer_cannot_grant(tmp_path: Path) -> None:
    database = await store(tmp_path / "history.sqlite3")
    await request_tool(database)
    grant = answer({"approved": True, "scope": "workflow"})
    with pytest.raises(mailbox.MailboxConflict, match="identity"):
        await database.answer_input(grant.model_copy(update={"execution_id": "other"}))
    await database.cancel_inputs("tool")
    with pytest.raises(mailbox.MailboxConflict, match="closed"):
        await database.answer_input(grant)
    assert not await database.tool_approved("workflow", "test")
