"""Durable request/response contracts and execution-local mailbox wakeups."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
import hashlib
import json
import logging
import typing

import pydantic

from vectorops.runner import input_schema
from vectorops.runner.runner import inbox as inbox_api
from vectorops.runner.runner.execution_state import timestamp

if typing.TYPE_CHECKING:
    from vectorops.runner.runner.pause import ExecutionPause

KIND: typing.Final = "mailbox.input"


class Model(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class InputFields(Model):
    channel: str = pydantic.Field(min_length=1, max_length=256, pattern=r"^\S+$")
    payload: pydantic.JsonValue = None
    response_type: typing.Literal["text", "json"] = "text"
    request_type: typing.Optional[str] = pydantic.Field(
        default=None, min_length=1, max_length=256, pattern=r"^\S+$"
    )

    def validate_response(self, value: pydantic.JsonValue) -> None:
        if self.response_type == "text" and not isinstance(value, str):
            raise ValueError("input request requires a text response")


class InputSpec(InputFields):
    request_schema: typing.Optional[input_schema.Schema] = None
    response_schema: typing.Optional[input_schema.Schema] = None

    @pydantic.field_validator("request_schema", "response_schema", mode="after")
    @classmethod
    def validate_schema(
        cls, value: typing.Optional[input_schema.Schema]
    ) -> typing.Optional[input_schema.Schema]:
        if value is not None:
            input_schema.validator(value)
        return value

    @pydantic.model_validator(mode="after")
    def validate_payload(self) -> typing.Self:
        if self.request_schema is not None:
            input_schema.validate(self.request_schema, self.payload, field="payload")
        return self

    def specification(self) -> InputSpec:
        return InputSpec.model_validate(
            self.model_dump(include=set(InputSpec.model_fields))
        )

    def fields(self) -> InputFields:
        return InputFields(
            channel=self.channel,
            payload=self.payload,
            response_type=self.response_type,
            request_type=self.request_type,
        )

    @staticmethod
    def contract_digest(
        request_schema: typing.Optional[input_schema.Schema],
        response_schema: typing.Optional[input_schema.Schema],
    ) -> str:
        value = json.dumps(
            [request_schema, response_schema],
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        return hashlib.sha256(value.encode()).hexdigest()

    def validate_response(self, value: pydantic.JsonValue) -> None:
        super().validate_response(value)
        if self.response_schema is not None:
            input_schema.validate(self.response_schema, value, field="response")


class InputRequest(InputFields):
    request_id: str
    execution_id: str
    workflow_id: str
    status: typing.Literal["pending", "answered", "cancelled"] = "pending"
    created_at: str = pydantic.Field(default_factory=timestamp)
    answered_at: typing.Optional[str] = None
    response: pydantic.JsonValue = None
    idempotency_key: typing.Optional[str] = None


class StoredInputRequest(InputRequest, InputSpec):
    """Internal durable validation contract, never returned as a public request."""

    def public(self) -> InputRequest:
        return InputRequest(
            **self.fields().model_dump(),
            request_id=self.request_id,
            execution_id=self.execution_id,
            workflow_id=self.workflow_id,
            status=self.status,
            created_at=self.created_at,
            answered_at=self.answered_at,
            response=self.response,
            idempotency_key=self.idempotency_key,
        )


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

    async def get_input_spec(self, request_id: str) -> InputSpec: ...

    async def answer_input(self, answer: Answer) -> InputRequest: ...

    async def tool_approved(self, workflow_id: str, tool_name: str) -> bool: ...

    async def cancel_inputs(self, request_id: typing.Optional[str] = None) -> None: ...


EventSink: typing.TypeAlias = Callable[[dict[str, pydantic.JsonValue]], None]


class Mailbox:
    """Use durable rows as truth and events only to wake local activity waiters."""

    def __init__(
        self,
        store: MailboxStore,
        sink: typing.Optional[EventSink] = None,
        pause: typing.Optional[ExecutionPause] = None,
        *,
        inboxes: typing.Optional[inbox_api.Registry] = None,
    ) -> None:
        self.store = store
        self.inboxes = inboxes
        self.sink = sink
        self.pause = pause
        self.suspending = False
        self._waiters: dict[str, asyncio.Event] = {}

    @property
    def waiting(self) -> bool:
        return bool(self._waiters)

    async def existing(self, request_id: str) -> typing.Optional[InputSpec]:
        try:
            return await self.store.get_input_spec(request_id)
        except KeyError:
            return None

    async def tool_approved(self, workflow_id: str, tool_name: str) -> bool:
        return await self.store.tool_approved(workflow_id, tool_name)

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
                if self.pause is not None:
                    await self.pause.wait(request_id)
                await wakeup.wait()
        except asyncio.CancelledError:
            if not self.suspending:
                await self.store.cancel_inputs(request_id)
            raise
        finally:
            self._waiters.pop(request_id, None)
            if self.pause is not None:
                await self.pause.release(request_id)

    async def answer(self, answer: Answer) -> InputRequest:
        try:
            request = await self.store.answer_input(answer)
        finally:
            if (wakeup := self._waiters.get(answer.request_id)) is not None:
                wakeup.set()
        self.emit("mailbox.answered", request)
        return request
