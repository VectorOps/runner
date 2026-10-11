import asyncio

import connect
import pydantic
import pytest

from tests.api.test_tools import ApprovalStore, approve
from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from vectorops.runner import input_schema
from vectorops.runner.api import composition, llm, mailbox as api_mailbox
from vectorops.runner.runner import mailbox
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.errors import ReplayMismatchError


@pytest.mark.parametrize(
    "response",
    [
        {"role": "system", "content": [{"type": "text", "text": "hello"}]},
        {"content": []},
        {
            "content": [
                {"type": "tool_call", "id": "1", "name": "tool", "arguments": {}}
            ]
        },
        {"content": [{"text": "missing discriminator"}]},
        {"content": [{"type": "text", "text": 42}]},
        {"content": [{"type": "image", "mime_type": "text/html", "data": "YQ=="}]},
        {
            "content": [
                {
                    "type": "image",
                    "mime_type": "image/png",
                    "data": "https://example.com",
                }
            ]
        },
        {"content": [{"type": "text", "text": "x" * 65537}]},
        {
            "content": [
                {
                    "type": "image",
                    "mime_type": "image/png",
                    "data": "YQ==",
                    "provider_meta": {},
                }
            ]
        },
    ],
)
def test_chat_contract_rejects_invalid_content(response: pydantic.JsonValue) -> None:
    spec = llm.UserMessageRequest(prompt="Reply").input_spec()
    with pytest.raises(input_schema.InputValidationError):
        spec.validate_response(response)
    with pytest.raises(pydantic.ValidationError):
        llm.UserMessageInput.model_validate(response)


def test_chat_request_payload_is_validated() -> None:
    spec = llm.UserMessageRequest(prompt="Reply").input_spec()
    with pytest.raises(pydantic.ValidationError):
        mailbox.InputSpec.model_validate(
            {**spec.model_dump(), "payload": {"prompt": 42}}
        )


async def test_chat_helper_mixed_content_and_replay(history: MemoryHistory) -> None:
    store = ApprovalStore()
    box = mailbox.Mailbox(store)

    async def workflow() -> pydantic.JsonValue:
        message = await llm.request_user_message(
            "Describe this", channel="conversation"
        )
        assert isinstance(message, connect.UserMessage)
        assert message.role == "user"
        assert isinstance(message.content, list)
        assert message.content[0].type == "text"
        assert message.content[1].type == "image"
        return message.model_dump(mode="json")

    task = asyncio.create_task(replay.run(history, workflow, mailbox=box))
    await asyncio.wait_for(store.registered.wait(), 2)
    request = next(iter(store.requests.values()))
    assert request.request_type == "chat.message.v1"
    assert request.payload == {"prompt": "Describe this"}
    await approve(
        box,
        request,
        {
            "content": [
                {"type": "text", "text": "What is this?"},
                {"type": "image", "mime_type": "image/png", "data": "YQ=="},
            ]
        },
    )
    first = await asyncio.wait_for(task, 2)
    assert first.outcome.kind == "completed", first.outcome
    assert (await replay.run(history, workflow, first.id)).outcome == first.outcome


async def test_changed_runtime_contract_rejects_replay(history: MemoryHistory) -> None:
    store = ApprovalStore()
    box = mailbox.Mailbox(store)
    schema: input_schema.Schema = {"type": "string"}

    async def workflow() -> pydantic.JsonValue:
        return await api_mailbox.request_input("test", response_schema=schema)

    task = asyncio.create_task(replay.run(history, workflow, mailbox=box))
    await asyncio.wait_for(store.registered.wait(), 2)
    await approve(box, next(iter(store.requests.values())), "hello")
    first = await asyncio.wait_for(task, 2)
    assert first.outcome.kind == "completed"
    assert (await replay.run(history, workflow, first.id)).outcome == first.outcome
    schema = {"type": "string", "minLength": 10}
    with pytest.raises(ReplayMismatchError):
        await replay.run(history, workflow, first.id)


async def test_invalid_workflow_payload_never_reaches_mailbox(
    history: MemoryHistory,
) -> None:
    store = ApprovalStore()

    async def workflow() -> pydantic.JsonValue:
        return await api_mailbox.request_input(
            "shared",
            {"prompt": 42},
            response_type="json",
            request_type="custom.v1",
            request_schema=llm.UserMessageRequest.model_json_schema(),
            response_schema=llm.UserMessageInput.model_json_schema(),
        )

    result = await replay.run(history, workflow, mailbox=mailbox.Mailbox(store))
    assert result.outcome.kind == "failed"
    assert store.requests == {}


async def test_old_untyped_mailbox_history_still_replays(
    history: MemoryHistory,
) -> None:
    @composition.activity(name="request_input", kind=mailbox.KIND)
    async def old_request(
        ctx: ActivityExecutionContext,
        channel: str,
        payload: pydantic.JsonValue = None,
        *,
        response_type: str = "text",
    ) -> pydantic.JsonValue:
        return "hello"

    async def old_workflow() -> pydantic.JsonValue:
        return await old_request("shared", {"prompt": "Reply"})

    async def workflow() -> pydantic.JsonValue:
        return await api_mailbox.request_input("shared", {"prompt": "Reply"})

    first = await replay.run(history, old_workflow)
    assert first.outcome.kind == "completed"
    assert (await replay.run(history, workflow, first.id)).outcome == first.outcome
