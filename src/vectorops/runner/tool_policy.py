"""Runtime-only tool authorization, independent of bindings and saga history."""

from collections.abc import Awaitable, Callable
import typing

import connect
import pydantic

from vectorops.runner.runner import mailbox


class AuthContext(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    properties: dict[str, pydantic.JsonValue] = pydantic.Field(default_factory=dict)


class ToolApprovalRequest(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    instance: typing.Optional[str] = None
    call: connect.ToolCallBlock
    tool: connect.ToolSpec
    tool_version: int = pydantic.Field(ge=1, strict=True)
    auth_context: AuthContext = pydantic.Field(default_factory=AuthContext)

    @pydantic.model_validator(mode="after")
    def validate_identity(self) -> typing.Self:
        if not self.call.name or self.call.name != self.tool.name:
            raise ValueError("approval call and tool names must match")
        return self

    def input_spec(self, channel: str = "tool.approval") -> mailbox.InputSpec:
        schema = self.model_json_schema()
        schema["allOf"] = [
            {
                "properties": {
                    key: {"properties": {"name": {"const": self.tool.name}}}
                    for key in ("call", "tool")
                }
            }
        ]
        return mailbox.InputSpec(
            channel=channel,
            payload=self.model_dump(mode="json"),
            response_type="json",
            request_type="tool.approval.v1",
            request_schema=schema,
            response_schema=ToolApprovalResponse.model_json_schema(),
        )


class ToolApprovalResponse(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(
        extra="forbid",
        frozen=True,
        json_schema_extra={
            "if": {
                "properties": {"scope": {"const": "workflow"}},
                "required": ["scope"],
            },
            "then": {"properties": {"approved": {"const": True}}},
        },
    )

    approved: bool = pydantic.Field(strict=True)
    reason: typing.Optional[str] = None
    scope: typing.Literal["call", "workflow"] = "call"

    @pydantic.model_validator(mode="after")
    def validate_scope(self) -> typing.Self:
        if self.scope == "workflow" and not self.approved:
            raise ValueError("workflow scope requires approval")
        return self


def workflow_grant(
    payload: pydantic.JsonValue, response: pydantic.JsonValue
) -> typing.Optional[str]:
    """Interpret a tool approval answer without trusting host-supplied identities."""
    try:
        decision = ToolApprovalResponse.model_validate(response)
    except pydantic.ValidationError:
        return None
    if not decision.approved or decision.scope != "workflow":
        return None
    if not isinstance(payload, dict):
        return None
    call = payload.get("call")
    tool = payload.get("tool")
    if not isinstance(call, dict) or not isinstance(tool, dict):
        return None
    name = call.get("name")
    if not isinstance(name, str) or not name or tool.get("name") != name:
        return None
    return name


class ApprovalSettings(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    approve_all: bool = pydantic.Field(default=False, strict=True)
    whitelist: list[str] = pydantic.Field(default_factory=list)

    @pydantic.field_validator("whitelist", mode="after")
    @classmethod
    def validate_names(cls, names: list[str]) -> list[str]:
        if any(not name or name.strip() != name for name in names):
            raise ValueError("whitelist names must be nonempty and unpadded")
        if len(names) != len(set(names)):
            raise ValueError("whitelist names must be unique")
        return names


class ToolCall(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    name: str
    version: int
    arguments: dict[str, pydantic.JsonValue]
    definition_digest: str
    execution_id: typing.Optional[str] = None
    workflow_id: str
    step_id: str
    auth_context: AuthContext = pydantic.Field(default_factory=AuthContext)


class Decision(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    action: typing.Literal["allow", "require_confirmation", "deny"]
    reason: typing.Optional[str] = None


Check: typing.TypeAlias = Callable[[ToolCall], Awaitable[typing.Optional[str]]]
Matcher: typing.TypeAlias = Callable[[ToolCall], Awaitable[bool]]


class ToolPolicy:
    """Own exact-name grants and extensible checks; denials precede all grants."""

    def __init__(self) -> None:
        self._whitelist: set[str] = set()
        self._checks: dict[str, Check] = {}
        self._matchers: dict[str, Matcher] = {}

    def allow(self, tool_name: str) -> None:
        ApprovalSettings(whitelist=[tool_name])
        self._whitelist.add(tool_name)

    def remove(self, tool_name: str) -> None:
        self._whitelist.discard(tool_name)

    def register_check(self, name: str, check: Check) -> None:
        if not name or name in self._checks:
            raise ValueError("check name must be nonempty and unique")
        self._checks[name] = check

    def register_matcher(self, name: str, matcher: Matcher) -> None:
        if not name or name in self._matchers:
            raise ValueError("matcher name must be nonempty and unique")
        self._matchers[name] = matcher

    async def evaluate(self, settings: ApprovalSettings, call: ToolCall) -> Decision:
        """Apply current settings without recording a policy activity or decision."""
        for check in self._checks.values():
            reason = await check(call.model_copy(deep=True))
            if reason is not None:
                return Decision(action="deny", reason=reason)
        if (
            settings.approve_all
            or call.name in settings.whitelist
            or call.name in self._whitelist
        ):
            return Decision(action="allow")
        for matcher in self._matchers.values():
            if await matcher(call.model_copy(deep=True)):
                return Decision(action="allow")
        return Decision(action="require_confirmation")
