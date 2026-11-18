import asyncio
import base64

import connect
import pydantic
import pytest
import vectorops.runner.api.chat_input as chat_input
import vectorops.runner.api.user_input as user_input

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


def test_default_chat_contract_digest_is_unchanged() -> None:
    spec = user_input.input_spec(chat_input.UserMessageRequest(prompt="Your message?"))
    assert mailbox.InputSpec.contract_digest(
        spec.request_schema, spec.response_schema
    ) == ("551f7a32be94530740d2420aa65e449a4f7f6d720781d32ff6af445c0d72f36c")
    assert spec.response_schema == chat_input.UserMessageInput.model_json_schema()


def test_large_image_contract_is_opt_in_and_bounded() -> None:
    data = base64.b64encode(bytes(chat_input.MAX_IMAGE_BYTES)).decode("ascii")
    payload = {"content": [{"type": "image", "mime_type": "image/png", "data": data}]}
    spec = user_input.input_spec(
        chat_input.UserMessageRequest(prompt="Image?"),
        max_image_bytes=chat_input.MAX_IMAGE_BYTES,
    )
    spec.validate_response(payload)
    message = chat_input.LargeUserMessageInput.model_validate(payload).to_message()
    assert message.role == "user"
    assert isinstance(message.content, list)
    assert message.content[0].type == "image"
    assert message.content[0].data == data
    with pytest.raises(pydantic.ValidationError):
        chat_input.UserMessageInput.model_validate(payload)
    payload["content"][0]["data"] += "AAAA"
    with pytest.raises(input_schema.InputValidationError):
        spec.validate_response(payload)
    with pytest.raises(pydantic.ValidationError):
        chat_input.LargeUserMessageInput.model_validate(payload)


@pytest.mark.parametrize("size", [0, -1, True, 3145729])
def test_invalid_image_budgets(size: int) -> None:
    with pytest.raises(ValueError):
        chat_input.response_schema(size)


async def test_large_image_mailbox_and_replay(history: MemoryHistory) -> None:
    store = ApprovalStore()
    box = mailbox.Mailbox(store)
    data = base64.b64encode(bytes(1024 * 1024)).decode("ascii")

    async def workflow() -> pydantic.JsonValue:
        message = await llm.request_user_message(
            max_image_bytes=chat_input.MAX_IMAGE_BYTES
        )
        assert isinstance(message.content, list)
        assert message.content[0].type == "image"
        return len(message.content[0].data)

    task = asyncio.create_task(replay.run(history, workflow, mailbox=box))
    await asyncio.wait_for(store.registered.wait(), 2)
    await approve(
        box,
        next(iter(store.requests.values())),
        {"content": [{"type": "image", "mime_type": "image/png", "data": data}]},
    )
    first = await asyncio.wait_for(task, 5)
    assert first.outcome.kind == "completed", first.outcome
    assert (await replay.run(history, workflow, first.id)).outcome == first.outcome
