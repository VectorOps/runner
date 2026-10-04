"""Version-two packets and request parameters for bearer-authenticated control."""

import typing

import pydantic

VERSION: typing.Final = 2
MAX_FRAME: typing.Final = 8 * 1024 * 1024
HANDSHAKE_TIMEOUT: typing.Final = 10.0


class Model(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class Authenticate(Model):
    model_config = pydantic.ConfigDict(hide_input_in_errors=True)

    kind: typing.Literal["authenticate"] = "authenticate"
    version: typing.Literal[2] = VERSION
    token: str = pydantic.Field(
        min_length=43, max_length=4096, pattern=r"^[A-Za-z0-9_-]+$", repr=False
    )


class Authenticated(Model):
    kind: typing.Literal["authenticated"] = "authenticated"
    version: typing.Literal[2] = VERSION
    instance_id: str
    session_id: str


class Request(Model):
    kind: typing.Literal["request"] = "request"
    id: str = pydantic.Field(min_length=1, max_length=128)
    method: str = pydantic.Field(min_length=1, max_length=128)
    params: dict[str, pydantic.JsonValue] = pydantic.Field(default_factory=dict)


class Response(Model):
    kind: typing.Literal["response"] = "response"
    id: str
    result: pydantic.JsonValue = None


class Error(Model):
    kind: typing.Literal["error"] = "error"
    id: typing.Optional[str] = None
    code: str
    message: str


class Event(Model):
    kind: typing.Literal["event"] = "event"
    subscription_id: str
    sequence: int
    event: dict[str, pydantic.JsonValue]


class Ping(Model):
    kind: typing.Literal["ping"] = "ping"
    id: str


class Pong(Model):
    kind: typing.Literal["pong"] = "pong"
    id: str


Packet: typing.TypeAlias = typing.Annotated[
    typing.Union[
        Authenticate,
        Authenticated,
        Request,
        Response,
        Error,
        Event,
        Ping,
        Pong,
    ],
    pydantic.Field(discriminator="kind"),
]
PACKET: typing.Final[pydantic.TypeAdapter[Packet]] = pydantic.TypeAdapter(Packet)
JSON: typing.Final[pydantic.TypeAdapter[pydantic.JsonValue]] = pydantic.TypeAdapter(
    pydantic.JsonValue
)


class Empty(Model):
    pass


class Start(Model):
    file: str = pydantic.Field(min_length=1, max_length=4096)
    callable: str = pydantic.Field(min_length=1, max_length=256)
    input: dict[str, pydantic.JsonValue] = pydantic.Field(default_factory=dict)
    idempotency_key: str = pydantic.Field(min_length=1, max_length=256)

    @pydantic.field_validator("callable")
    @classmethod
    def identifier(cls, value: str) -> str:
        if not value.isidentifier():
            raise ValueError("callable must be an identifier")
        return value


class Execution(Model):
    execution_id: str = pydantic.Field(pattern=r"^[A-Za-z0-9_-]+$", max_length=128)


class Resume(Execution):
    idempotency_key: str = pydantic.Field(min_length=1, max_length=256)


class Current(Model):
    execution_id: typing.Optional[str] = pydantic.Field(
        default=None, pattern=r"^[A-Za-z0-9_-]+$", max_length=128
    )


class ExecutionList(Model):
    after: typing.Optional[str] = None
    limit: int = pydantic.Field(default=100, ge=1, le=500, strict=True)


class Subscribe(Model):
    execution_id: typing.Optional[str] = None
    kinds: list[str] = pydantic.Field(default_factory=list, max_length=32)


class Unsubscribe(Model):
    subscription_id: str


class Shutdown(Model):
    mode: typing.Literal["cancel", "drain"] = "cancel"


class Accepted(Model):
    execution_id: str
    status: typing.Literal["starting"] = "starting"


class Active(Model):
    execution_id: str
    status: typing.Literal["starting", "running", "stopping"] = "starting"


class ActivePage(Model):
    executions: list[Active]
    next_after: typing.Optional[str] = None


class ExecutionPage(Model):
    execution_ids: list[str]
    next_after: typing.Optional[str] = None


class RemoteError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def json_value(model: pydantic.BaseModel) -> pydantic.JsonValue:
    return JSON.validate_json(model.model_dump_json())
