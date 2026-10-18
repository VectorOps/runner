"""Runtime-only tool authorization, independent of bindings and saga history."""

from collections.abc import Awaitable, Callable, Sequence
import json
import typing
from uuid import UUID

import connect
import pydantic

from vectorops.runner.runner import mailbox


class AuthContext(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    properties: dict[str, pydantic.JsonValue] = pydantic.Field(default_factory=dict)


class RuleInput(AuthContext):
    tool_name: str = pydantic.Field(min_length=1)
    decision: typing.Literal["approve", "deny"]

    @pydantic.field_validator("tool_name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        if value.strip() != value:
            raise ValueError("tool name must be unpadded")
        return value


class AuthorizationRule(RuleInput):
    id: UUID


class ScopedRule(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)
    scope: typing.Literal["workspace", "workflow"]
    rule: AuthorizationRule


def matches(
    mask: dict[str, pydantic.JsonValue], properties: dict[str, pydantic.JsonValue]
) -> bool:
    """Match complete top-level values without conflating booleans and numbers."""
    return all(
        key in properties
        and json.dumps(value, sort_keys=True, allow_nan=False)
        == json.dumps(properties[key], sort_keys=True, allow_nan=False)
        for key, value in mask.items()
    )


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
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    decision: typing.Literal["approve", "deny"]
    reason: typing.Optional[str] = None
    scope: typing.Literal["call", "workflow", "workspace"] = "call"
    properties: dict[str, pydantic.JsonValue] = pydantic.Field(default_factory=dict)


def response_rule(
    payload: pydantic.JsonValue, response: pydantic.JsonValue
) -> typing.Optional[RuleInput]:
    """Derive identity from the stored request and validate the selected mask."""
    answer = ToolApprovalResponse.model_validate(response)
    request = ToolApprovalRequest.model_validate(payload)
    if not matches(answer.properties, request.auth_context.properties):
        raise ValueError("approval properties must match the request auth context")
    if answer.scope == "call":
        return None
    return RuleInput(
        tool_name=request.tool.name,
        properties=answer.properties,
        decision=answer.decision,
    )


class ApprovalSettings(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    approve_all: bool = pydantic.Field(default=False, strict=True)


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

    action: typing.Literal["approve", "ask", "deny"]
    reason: typing.Optional[str] = None
    rule_id: typing.Optional[UUID] = None


Check: typing.TypeAlias = Callable[[ToolCall], Awaitable[typing.Optional[str]]]


class ToolPolicy:
    """Mandatory checks veto rules; specificity precedes scope and decision ties."""

    def __init__(self) -> None:
        self._checks: dict[str, Check] = {}

    def register_check(self, name: str, check: Check) -> None:
        if not name or name in self._checks:
            raise ValueError("check name must be nonempty and unique")
        self._checks[name] = check

    async def check(self, call: ToolCall) -> typing.Optional[Decision]:
        for check in self._checks.values():
            reason = await check(call.model_copy(deep=True))
            if reason is not None:
                return Decision(action="deny", reason=reason)
        return None

    @staticmethod
    def resolve(
        settings: ApprovalSettings, call: ToolCall, rules: Sequence[ScopedRule]
    ) -> Decision:
        candidates = [
            item
            for item in rules
            if item.rule.tool_name == call.name
            and matches(item.rule.properties, call.auth_context.properties)
        ]
        if not candidates:
            return Decision(action="approve" if settings.approve_all else "ask")
        winner = min(
            candidates,
            key=lambda item: (
                -len(item.rule.properties),
                -int(item.scope == "workflow"),
                -int(item.rule.decision == "deny"),
                str(item.rule.id),
            ),
        )
        return Decision(action=winner.rule.decision, rule_id=winner.rule.id)

    async def evaluate(
        self,
        settings: ApprovalSettings,
        call: ToolCall,
        rules: Sequence[ScopedRule] = (),
    ) -> Decision:
        return await self.check(call) or self.resolve(settings, call, rules)
