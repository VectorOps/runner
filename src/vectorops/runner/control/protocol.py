"""Version-two packets and request parameters for bearer-authenticated control."""

import typing
from uuid import UUID

import pydantic

from vectorops.runner.references import WorkflowReference
from vectorops.runner.environment.target import ProjectPath
from vectorops.runner.api.definitions import WorkflowDescriptor
from vectorops.runner.runner import catalog, inspection
from vectorops.runner.runner.branching import (
    Branch as Branch,
    Accepted as BranchAccepted,
)

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
    details: pydantic.JsonValue = None


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
    reference: str = pydantic.Field(min_length=1, max_length=4096)
    input: dict[str, pydantic.JsonValue] = pydantic.Field(default_factory=dict)
    idempotency_key: str = pydantic.Field(min_length=1, max_length=256)
    base_project_path: typing.Optional[ProjectPath] = None
    worktree_id: typing.Optional[UUID] = None

    @pydantic.model_validator(mode="after")
    def target_selection(self) -> typing.Self:
        if self.base_project_path is not None and self.worktree_id is not None:
            raise ValueError("base_project_path and worktree_id are mutually exclusive")
        return self

    @pydantic.field_validator("reference", mode="after")
    @classmethod
    def identifier(cls, value: str) -> str:
        return str(WorkflowReference.parse(value))


class Execution(Model):
    execution_id: str = pydantic.Field(pattern=r"^[A-Za-z0-9_-]+$", max_length=128)


class WorkflowState(Execution):
    workflow_id: str = pydantic.Field(min_length=1, max_length=128)


class Resume(Execution):
    idempotency_key: str = pydantic.Field(min_length=1, max_length=256)


class Current(Model):
    execution_id: typing.Optional[str] = pydantic.Field(
        default=None, pattern=r"^[A-Za-z0-9_-]+$", max_length=128
    )


class ExecutionList(Model):
    after: typing.Optional[str] = None
    limit: int = pydantic.Field(default=100, ge=1, le=500, strict=True)


class WorkflowSearch(ExecutionList):
    query: str


class DescribeWorkflow(Model):
    reference: str = pydantic.Field(min_length=1, max_length=4096)


class WorkflowListing(Model):
    descriptor: WorkflowDescriptor


class WorkflowPage(Model):
    workflows: list[WorkflowListing]
    errors: list[catalog.DiscoveryError] = pydantic.Field(default_factory=list)
    next_after: typing.Optional[str] = None


class SummaryQuery(ExecutionList):
    active_only: bool = False


class SummaryPage(Model):
    executions: list[inspection.ExecutionSummary]
    next_after: typing.Optional[str] = None


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
    status: typing.Literal["starting", "running", "paused", "stopping"] = "starting"


class ActivePage(Model):
    executions: list[Active]
    next_after: typing.Optional[str] = None


class ExecutionPage(Model):
    execution_ids: list[str]
    next_after: typing.Optional[str] = None


class RemoteError(Exception):
    def __init__(
        self, code: str, message: str, details: pydantic.JsonValue = None
    ) -> None:
        super().__init__(message)
        self.code = code
        self.details = details


def json_value(model: pydantic.BaseModel) -> pydantic.JsonValue:
    return JSON.validate_json(model.model_dump_json())
