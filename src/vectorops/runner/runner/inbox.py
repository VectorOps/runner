"""Execution-local conversation queues and atomic host management."""

import asyncio
from collections.abc import Callable
import logging
import typing
from uuid import uuid4

import pydantic

from vectorops.runner.api import chat_input
from vectorops.runner.runner.execution_state import timestamp


class Model(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)


class Execution(Model):
    execution_id: str = pydantic.Field(pattern=r"^[A-Za-z0-9_-]+$", max_length=128)


class Target(Model):
    workflow_id: str = pydantic.Field(min_length=1, max_length=128)
    conversation_id: str = pydantic.Field(
        min_length=1, max_length=256, pattern=r"^\S+$"
    )


class Address(Execution, Target):
    pass


class Send(Address):
    payload: chat_input.UserMessageInput


class MessageRef(Address):
    message_id: str = pydantic.Field(min_length=1, max_length=128)


class MessageQuery(Address):
    after: typing.Optional[str] = None
    limit: int = pydantic.Field(default=100, ge=1, le=500, strict=True)


class Message(Model):
    id: str = pydantic.Field(default_factory=lambda: uuid4().hex)
    created_at: str = pydantic.Field(default_factory=timestamp)
    payload: chat_input.UserMessageInput


class MessagePage(Model):
    messages: tuple[Message, ...] = ()
    next_after: typing.Optional[str] = None


class Status(Model):
    address: Address
    status: typing.Literal["open", "closed"]
    queued_messages: int


class Listing(Model):
    mailboxes: tuple[Status, ...] = ()


class InboxConflict(ValueError):
    pass


RemovalReason: typing.TypeAlias = typing.Literal[
    "llm_drain", "workflow_receive", "host_remove"
]
EventSink: typing.TypeAlias = Callable[[dict[str, pydantic.JsonValue]], None]


class Inbox:
    """Serialize queue mutations without yielding; wakeups never consume messages."""

    def __init__(self, address: Address, sink: typing.Optional[EventSink]) -> None:
        self.address = address
        self._sink = sink
        self._messages: dict[str, Message] = {}
        self._closed = False
        self._available = asyncio.Event()

    def inspect(self) -> Status:
        return Status(
            address=self.address,
            status="closed" if self._closed else "open",
            queued_messages=len(self._messages),
        )

    def _require_open(self) -> None:
        if self._closed:
            raise InboxConflict("conversation inbox is closed")

    def _emit(
        self,
        kind: str,
        message: typing.Optional[Message] = None,
        reason: typing.Optional[RemovalReason] = None,
    ) -> None:
        if self._sink is not None:
            try:
                event: dict[str, pydantic.JsonValue] = {
                    "kind": kind,
                    "execution_id": self.address.execution_id,
                    **self.inspect().model_dump(mode="json"),
                }
                if message is not None:
                    event["message_id"] = message.id
                    event["reason"] = reason
                    if kind == "mailbox.enqueued":
                        event["message"] = message.model_dump(mode="json")
                self._sink(event)
            except Exception:
                logging.getLogger(__name__).exception("Inbox event sink failed")

    def publish_state(self) -> None:
        self._emit("mailbox.state_updated")

    def send(self, payload: chat_input.UserMessageInput) -> Message:
        self._require_open()
        message = Message(payload=payload.model_copy(deep=True))
        self._messages[message.id] = message
        self._available.set()
        self._emit("mailbox.enqueued", message)
        return message.model_copy(deep=True)

    def messages(self, query: MessageQuery) -> MessagePage:
        selected = list(self._messages.values())
        if query.after is not None:
            ids = [message.id for message in selected]
            if query.after not in ids:
                raise InboxConflict("message cursor is not queued")
            selected = selected[ids.index(query.after) + 1 :]
        return MessagePage(
            messages=tuple(
                message.model_copy(deep=True) for message in selected[: query.limit]
            ),
            next_after=(
                selected[query.limit - 1].id if len(selected) > query.limit else None
            ),
        )

    def remove(
        self, message_id: str, reason: RemovalReason = "host_remove"
    ) -> typing.Optional[Message]:
        self._require_open()
        message = self._messages.pop(message_id, None)
        if not self._messages:
            self._available.clear()
        if message is not None:
            self._emit("mailbox.removed", message, reason)
            return message.model_copy(deep=True)
        return None

    def pop(self, reason: RemovalReason = "host_remove") -> typing.Optional[Message]:
        self._require_open()
        identifier = next(iter(self._messages), None)
        return None if identifier is None else self.remove(identifier, reason)

    def drain(self) -> tuple[Message, ...]:
        self._require_open()
        selected = tuple(self._messages)
        messages = [self.remove(identifier, "llm_drain") for identifier in selected]
        return tuple(message for message in messages if message is not None)

    async def wait_available(self) -> None:
        self._require_open()
        await self._available.wait()
        self._require_open()

    async def receive(self) -> Message:
        while True:
            await self.wait_available()
            message = self.pop("workflow_receive")
            if message is not None:
                return message

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._available.set()
        self.publish_state()


class Registry:
    """Register explicit root conversations, never arbitrary sender destinations."""

    def __init__(
        self,
        execution_id: str,
        root_workflow_id: str,
        sink: typing.Optional[EventSink] = None,
    ) -> None:
        self.execution_id = execution_id
        self.root_workflow_id = root_workflow_id
        self._sink = sink
        self._inboxes: dict[str, Inbox] = {}
        self._closed = False

    def register(self, conversation_id: str) -> Inbox:
        if self._closed:
            raise InboxConflict("execution inboxes are closed")
        address = Address(
            execution_id=self.execution_id,
            workflow_id=self.root_workflow_id,
            conversation_id=conversation_id,
        )
        if conversation_id in self._inboxes:
            return self._inboxes[conversation_id]
        inbox = Inbox(address, self._sink)
        self._inboxes[conversation_id] = inbox
        inbox.publish_state()
        return inbox

    def get(self, address: Address) -> Inbox:
        if (address.execution_id, address.workflow_id) != (
            self.execution_id,
            self.root_workflow_id,
        ):
            raise InboxConflict(
                "inbox address does not belong to the root conversation"
            )
        inbox = self._inboxes.get(address.conversation_id)
        if inbox is None:
            raise InboxConflict("conversation inbox is not registered")
        return inbox

    def listing(self) -> Listing:
        return Listing(
            mailboxes=tuple(inbox.inspect() for inbox in self._inboxes.values())
        )

    def drain(self, target: Target) -> tuple[Message, ...]:
        if self._closed or target.workflow_id != self.root_workflow_id:
            return ()
        inbox = self._inboxes.get(target.conversation_id)
        return () if inbox is None else inbox.drain()

    def close(self) -> None:
        self._closed = True
        for inbox in self._inboxes.values():
            inbox.close()
