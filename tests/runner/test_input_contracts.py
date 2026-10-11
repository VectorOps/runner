import asyncio
from pathlib import Path

import connect
import pydantic
import pytest

from tests.runner.test_mailbox import answer, store
from vectorops.runner import input_schema, models, tool_policy
from vectorops.runner.api import llm
from vectorops.runner.runner import mailbox
from vectorops.runner.runner.sqlite_history import SQLiteHistory


def approval() -> mailbox.InputSpec:
    return tool_policy.ToolApprovalRequest(
        call=connect.ToolCallBlock(id="call", name="test", arguments={"value": 1}),
        tool=connect.ToolSpec.external(
            name="test", description="Test", input_schema={"type": "object"}
        ),
        tool_version=1,
    ).input_spec("shared")


@pytest.mark.parametrize(
    "schema",
    [
        {"type": "invalid"},
        {"$schema": "http://json-schema.org/draft-07/schema#"},
        {"$ref": "https://example.com/schema"},
        {"$ref": "#/$defs/missing"},
        {"$defs": {"node": {"$ref": "#/$defs/node"}}, "$ref": "#/$defs/node"},
        {"properties": {"nested": {"$ref": "file:///tmp/schema"}}},
        {"$defs": {"unused": {"$ref": "https://example.com/schema"}}},
        {"type": "string", "format": "unknown"},
        {"unknownKeyword": True},
        {"$dynamicRef": "#node"},
    ],
)
@pytest.mark.parametrize("field", ["request_schema", "response_schema"])
def test_bad_schema_rejected_at_registration(
    schema: dict[str, pydantic.JsonValue], field: str
) -> None:
    with pytest.raises(pydantic.ValidationError):
        mailbox.InputSpec.model_validate({"channel": "shared", field: schema})


@pytest.mark.parametrize("case", ["missing", "extra", "wrong_type", "identity"])
async def test_invalid_workflow_approval_request_never_registered(
    tmp_path: Path, case: str
) -> None:
    database = await store(tmp_path / "history.sqlite3")
    spec = approval()
    assert isinstance(spec.payload, dict)
    if case == "missing":
        spec.payload.pop("call")
    elif case == "extra":
        spec.payload["unexpected"] = True
    elif case == "wrong_type":
        spec.payload["tool_version"] = "1"
    else:
        call = spec.payload["call"]
        assert isinstance(call, dict)
        call["name"] = "another"
    with pytest.raises(pydantic.ValidationError):
        await database.register_input("request", spec)
    with pytest.raises(KeyError):
        await database.get_input("request")


async def test_durable_schema_rejection_retry_and_contract_identity(
    tmp_path: Path,
) -> None:
    database = await store(tmp_path / "history.sqlite3")
    spec = approval()
    original = await database.register_input("request", spec)
    reopened = SQLiteHistory(database.path)
    assert await reopened.get_input_spec("request") == spec
    with pytest.raises(input_schema.InputValidationError) as error:
        await reopened.answer_input(answer({"approved": "yes"}))
    assert error.value.issues[0].path == ["response", "approved"]
    assert await reopened.get_input("request") == original
    receipt = await reopened.answer_input(answer({"approved": True}))
    assert await reopened.answer_input(answer({"approved": True})) == receipt
    assert await mailbox.Mailbox(reopened).wait("request", spec) == {"approved": True}
    with pytest.raises(mailbox.MailboxConflict, match="contract changed"):
        await reopened.register_input(
            "request", spec.model_copy(update={"response_schema": {"type": "string"}})
        )


@pytest.mark.parametrize(
    "value",
    [
        {"approved": "true"},
        {"approved": False, "scope": "workflow"},
        {"approved": True, "scope": "all"},
        {"approved": True, "scope": "workflow", "workflow_id": "another"},
    ],
)
async def test_invalid_approval_cannot_create_grant(
    tmp_path: Path, value: pydantic.JsonValue
) -> None:
    database = await store(tmp_path / "history.sqlite3")
    await database.append_step(
        models.WorkflowStep(
            id="tool",
            workflow_id="workflow",
            operation=models.StepOperation(kind="llm.tool", name="invoke_tool"),
            outcome=models.StepPending(),
            deferred_delivery=True,
        )
    )
    await database.register_input("tool", approval())
    invalid = answer(value).model_copy(update={"request_id": "tool"})
    with pytest.raises(input_schema.InputValidationError):
        await database.answer_input(invalid)
    assert (await database.get_input("tool")).status == "pending"
    assert not await database.tool_approved("workflow", "test")
    valid = invalid.model_copy(
        update={"response": {"approved": True, "scope": "workflow"}}
    )
    await database.answer_input(valid)
    assert await database.tool_approved("workflow", "test")


async def test_two_contracts_on_one_channel(tmp_path: Path) -> None:
    database = await store(tmp_path / "history.sqlite3")
    await database.append_step(
        models.WorkflowStep(
            id="chat",
            workflow_id="workflow",
            operation=models.StepOperation(kind=mailbox.KIND, name="request_input"),
            outcome=models.StepPending(),
            deferred_delivery=True,
        )
    )
    await database.register_input("request", approval())
    chat = llm.UserMessageRequest(prompt="Describe the image").input_spec("shared")
    await database.register_input("chat", chat)
    page = await database.list_inputs(
        mailbox.RequestQuery(execution_id="execution", channel="shared")
    )
    assert {request.request_type for request in page.requests} == {
        "chat.message.v1",
        "tool.approval.v1",
    }
    with pytest.raises(input_schema.InputValidationError):
        await database.answer_input(
            answer({"approved": True}).model_copy(update={"request_id": "chat"})
        )
    content: pydantic.JsonValue = {
        "content": [
            {"type": "text", "text": "What is this?"},
            {"type": "image", "mime_type": "image/png", "data": "YQ=="},
        ]
    }
    await database.answer_input(
        answer(content).model_copy(update={"request_id": "chat"})
    )
    assert (await database.get_input("request")).status == "pending"


async def test_invalid_answer_does_not_complete_waiter(tmp_path: Path) -> None:
    database = await store(tmp_path / "history.sqlite3")
    ready = asyncio.Event()
    owner = mailbox.Mailbox(database, lambda event: ready.set())
    task = asyncio.create_task(owner.wait("request", approval()))
    try:
        await asyncio.wait_for(ready.wait(), 5)
        with pytest.raises(input_schema.InputValidationError):
            await owner.answer(answer({"approved": "yes"}))
        assert not task.done()
        await owner.answer(answer({"approved": False}))
        assert await asyncio.wait_for(task, 5) == {"approved": False}
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
