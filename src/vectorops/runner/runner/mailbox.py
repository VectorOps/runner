"""Durable request/response contracts and execution-local mailbox wakeups."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
import logging
import typing

import pydantic

from vectorops.runner.runner.execution_state import timestamp

KIND: typing.Final = "mailbox.input"


class Model(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class InputSpec(Model):
    channel: str = pydantic.Field(min_length=1, max_length=256, pattern=r"^\S+$")
    payload: pydantic.JsonValue = None
    response_type: typing.Literal["text", "json"] = "text"

    def validate_response(self, value: pydantic.JsonValue) -> None:
        if self.response_type == "text" and not isinstance(value, str):
            raise ValueError("input request requires a text response")


class InputRequest(InputSpec):
    request_id: str
    execution_id: str
    workflow_id: str
    status: typing.Literal["pending", "answered", "cancelled"] = "pending"
    created_at: str = pydantic.Field(default_factory=timestamp)
    answered_at: typing.Optional[str] = None
    response: pydantic.JsonValue = None
    idempotency_key: typing.Optional[str] = None


class RequestRef(Model):
    execution_id: str = pydantic.Field(pattern=r"^[A-Za-z0-9_-]+$", max_length=128)
    request_id: str = pydantic.Field(min_length=1, max_length=128)


class Answer(RequestRef):
    response: pydantic.JsonValue
    idempotency_key: str = pydantic.Field(min_length=1, max_length=256)


class RequestQuery(Model):
    execution_id: str = pydantic.Field(pattern=r"^[A-Za-z0-9_-]+$", max_length=128)
    workflow_id: typing.Optional[str] = None
    channel: typing.Optional[str] = None
    after: typing.Optional[str] = None
    limit: int = pydantic.Field(default=100, ge=1, le=500, strict=True)


class RequestPage(Model):
    requests: list[InputRequest]
    next_after: typing.Optional[str] = None


class MailboxConflict(ValueError):
    pass


class MailboxStore(typing.Protocol):
    async def register_input(
        self, request_id: str, spec: InputSpec
    ) -> InputRequest: ...

    async def get_input(self, request_id: str) -> InputRequest: ...

    async def answer_input(self, answer: Answer) -> InputRequest: ...

    async def cancel_inputs(self, request_id: typing.Optional[str] = None) -> None: ...


EventSink: typing.TypeAlias = Callable[[dict[str, pydantic.JsonValue]], None]


class Mailbox:
    """Use durable rows as truth and events only to wake local activity waiters."""

    def __init__(
        self, store: MailboxStore, sink: typing.Optional[EventSink] = None
    ) -> None:
        self.store = store
        self.sink = sink
        self.suspending = False
        self._waiters: dict[str, asyncio.Event] = {}

    @property
    def waiting(self) -> bool:
        return bool(self._waiters)

    def emit(self, kind: str, request: InputRequest) -> None:
        if self.sink is not None:
            try:
                self.sink(
                    {
                        "kind": kind,
                        "execution_id": request.execution_id,
                        "request": request.model_dump(mode="json"),
                    }
                )
            except Exception:
                logging.getLogger(__name__).exception("Mailbox event sink failed")

    async def wait(self, request_id: str, spec: InputSpec) -> pydantic.JsonValue:
        """Reattach to the same durable request, including an undelivered answer."""
        wakeup = asyncio.Event()
        if request_id in self._waiters:
            raise MailboxConflict("input request already has a local waiter")
        self._waiters[request_id] = wakeup
        try:
            request = await self.store.register_input(request_id, spec)
            if request.status == "pending":
                self.emit("mailbox.requested", request)
            while True:
                wakeup.clear()
                request = await self.store.get_input(request_id)
                if request.status == "answered":
                    return request.response
                if request.status != "pending":
                    raise MailboxConflict("input request is closed")
                await wakeup.wait()
        except asyncio.CancelledError:
            if not self.suspending:
                await self.store.cancel_inputs(request_id)
            raise
        finally:
            self._waiters.pop(request_id, None)

    async def answer(self, answer: Answer) -> InputRequest:
        try:
            request = await self.store.answer_input(answer)
        finally:
            if (wakeup := self._waiters.get(answer.request_id)) is not None:
                wakeup.set()
        self.emit("mailbox.answered", request)
        return request
