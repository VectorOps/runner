import asyncio
from pathlib import Path

import pydantic
import pytest
import connect

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
        tool_policy.ToolApprovalRequest(
            call=connect.ToolCallBlock(id="call", name="test", arguments={}),
            tool=connect.ToolSpec.external(
                name="test", description="Test", input_schema={"type": "object"}
            ),
            tool_version=1,
        ).input_spec("custom.approval"),
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
        ({"decision": "approve", "scope": "workflow"}, True),
        ({"decision": "approve"}, False),
        ({"decision": "approve", "scope": "call"}, False),
        ({"decision": "deny"}, False),
        ({"decision": "deny", "scope": "workflow"}, True),
    ],
)
async def test_grant_is_atomic_durable_and_workflow_scoped(
    tmp_path: Path, value: pydantic.JsonValue, granted: bool
) -> None:
    database = await store(tmp_path / "history.sqlite3")
    request = await request_tool(database)
    assert not await database.authorization_rules(request.workflow_id, "test")
    receipt = await database.answer_input(answer(value))
    reopened = SQLiteHistory(database.path)
    assert (
        bool(await reopened.authorization_rules(request.workflow_id, "test")) is granted
    )
    assert not await reopened.authorization_rules("child", "test")
    assert not await reopened.authorization_rules(request.workflow_id, "other")
    assert await reopened.answer_input(answer(value)) == receipt
    other_execution = await store(tmp_path / "other.sqlite3")
    assert not await other_execution.authorization_rules(request.workflow_id, "test")


async def test_competing_answers_only_winner_can_grant(tmp_path: Path) -> None:
    database = await store(tmp_path / "history.sqlite3")
    await request_tool(database)
    results = await asyncio.gather(
        database.answer_input(answer({"decision": "deny"}, "reject")),
        database.answer_input(
            answer({"decision": "approve", "scope": "workflow"}, "grant")
        ),
        return_exceptions=True,
    )
    assert sum(isinstance(result, mailbox.MailboxConflict) for result in results) == 1
    winner = await database.get_input("tool")
    assert bool(await database.authorization_rules("workflow", "test")) is (
        tool_policy.response_rule(winner.payload, winner.response) is not None
    )


async def test_ordinary_mailbox_cannot_create_tool_grant(tmp_path: Path) -> None:
    database = await store(tmp_path / "history.sqlite3")
    await request_tool(database, kind=mailbox.KIND)
    await database.answer_input(answer({"decision": "approve", "scope": "workflow"}))
    assert not await database.authorization_rules("workflow", "test")


async def test_cancelled_or_wrong_execution_answer_cannot_grant(tmp_path: Path) -> None:
    database = await store(tmp_path / "history.sqlite3")
    await request_tool(database)
    grant = answer({"decision": "approve", "scope": "workflow"})
    with pytest.raises(mailbox.MailboxConflict, match="identity"):
        await database.answer_input(grant.model_copy(update={"execution_id": "other"}))
    await database.cancel_inputs("tool")
    with pytest.raises(mailbox.MailboxConflict, match="closed"):
        await database.answer_input(grant)
    assert not await database.authorization_rules("workflow", "test")
