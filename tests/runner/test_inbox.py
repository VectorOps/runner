import asyncio

import pydantic
import pytest

from vectorops.runner.api import chat_input
from vectorops.runner.runner import inbox


def payload(text: str) -> chat_input.UserMessageInput:
    return chat_input.UserMessageInput(
        content=[chat_input.TextInput(type="text", text=text)]
    )


def test_queue_management_and_snapshots() -> None:
    events: list[dict[str, pydantic.JsonValue]] = []
    owner = inbox.Registry("execution", "root", events.append)
    queue = owner.register("chat")
    first = queue.send(payload("one"))
    second = queue.send(payload("two"))
    third = queue.send(payload("three"))
    assert owner.listing().mailboxes[0].queued_messages == 3
    query = inbox.MessageQuery(**queue.address.model_dump(), limit=1)
    page = queue.messages(query)
    assert page.messages == (first,)
    assert page.next_after == first.id
    page.messages[0].payload.content.clear()
    assert queue.messages(query).messages[0].payload == payload("one")
    assert queue.remove(second.id) == second
    assert queue.remove(second.id) is None
    assert queue.pop() == first
    assert queue.drain() == (third,)
    assert queue.pop() is None
    assert queue.drain() == ()
    assert all("payload" not in event for event in events)
    assert events[-1]["reason"] == "llm_drain"
    assert events[-1]["queued_messages"] == 0


async def test_blocking_receive_and_close() -> None:
    queue = inbox.Registry("execution", "root").register("chat")
    waiting = asyncio.create_task(queue.receive())
    await asyncio.sleep(0)
    assert not waiting.done()
    sent = queue.send(payload("hello"))
    assert await asyncio.wait_for(waiting, 1) == sent
    waiting = asyncio.create_task(queue.receive())
    await asyncio.sleep(0)
    queue.close()
    with pytest.raises(inbox.InboxConflict, match="closed"):
        await waiting
    assert queue.inspect().status == "closed"
    with pytest.raises(inbox.InboxConflict, match="closed"):
        queue.send(payload("late"))


def test_targeting_and_registration() -> None:
    owner = inbox.Registry("execution", "root")
    queue = owner.register("chat")
    assert owner.get(queue.address) is queue
    assert owner.register("chat") is queue
    for address in (
        queue.address.model_copy(update={"workflow_id": "child"}),
        queue.address.model_copy(update={"execution_id": "other"}),
        queue.address.model_copy(update={"conversation_id": "unknown"}),
    ):
        with pytest.raises(inbox.InboxConflict):
            owner.get(address)
    owner.close()
    with pytest.raises(inbox.InboxConflict, match="closed"):
        owner.register("new")


def test_missing_target_drain_is_empty() -> None:
    owner = inbox.Registry("execution", "root")
    assert (
        owner.drain(inbox.Target(workflow_id="root", conversation_id="missing")) == ()
    )
    queue = owner.register("chat")
    message = queue.send(payload("root only"))
    assert owner.drain(inbox.Target(workflow_id="child", conversation_id="chat")) == ()
    assert owner.drain(inbox.Target(workflow_id="root", conversation_id="chat")) == (
        message,
    )
    owner.close()
    assert owner.drain(inbox.Target(workflow_id="root", conversation_id="chat")) == ()


def test_notification_failure_does_not_break_delivery() -> None:
    def fail(event: dict[str, pydantic.JsonValue]) -> None:
        raise RuntimeError("failed listener")

    queue = inbox.Registry("execution", "root", fail).register("chat")
    message = queue.send(payload("hello"))
    assert queue.pop() == message


async def test_competing_removal_has_one_winner() -> None:
    queue = inbox.Registry("execution", "root").register("chat")
    message = queue.send(payload("hello"))

    async def remove() -> tuple[inbox.Message, ...]:
        await asyncio.sleep(0)
        result = queue.remove(message.id)
        return () if result is None else (result,)

    async def drain() -> tuple[inbox.Message, ...]:
        await asyncio.sleep(0)
        return queue.drain()

    results = await asyncio.gather(remove(), drain())
    assert [item for batch in results for item in batch] == [message]
    assert queue.inspect().queued_messages == 0
