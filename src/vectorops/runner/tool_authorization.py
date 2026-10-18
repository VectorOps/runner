"""Tool validation and authorization, independent of workflow recording and execution."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
import hashlib
import json
import typing
from uuid import UUID

import connect
import pydantic

from vectorops.runner import tool_policy
from vectorops.runner.runner import mailbox

if typing.TYPE_CHECKING:
    from vectorops.runner.runner.activity_context import ActivityExecutionContext


class ToolApproval(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)
    channel: str = pydantic.Field(
        default="tool.approval", min_length=1, max_length=256, pattern=r"^\S+$"
    )


class ToolExecutionError(Exception):
    """An explicit tool error, model-visible unless strict handling is enabled."""


AuthContext = tool_policy.AuthContext
AuthContextCallback: typing.TypeAlias = Callable[
    [dict[str, pydantic.JsonValue]], Awaitable[AuthContext]
]


@typing.runtime_checkable
class AuthContextProvider(typing.Protocol):
    async def auth_context(
        self, arguments: dict[str, pydantic.JsonValue]
    ) -> AuthContext: ...


class ToolMetadata(typing.Protocol):
    @property
    def definition(self) -> connect.ToolSpec: ...

    @property
    def version(self) -> int: ...

    @property
    def approval(self) -> typing.Optional[ToolApproval]: ...

    def validate(self, arguments: dict[str, pydantic.JsonValue]) -> None:
        """Validate deterministically before approval, without performing I/O."""
        ...


def digest(value: pydantic.JsonValue) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(encoded.encode()).hexdigest()


class AuthorizationData(typing.Protocol):
    def settings(self) -> tool_policy.ApprovalSettings: ...

    async def rules(
        self, workflow_id: str, tool_name: str
    ) -> list[tool_policy.ScopedRule]: ...

    async def approval_rule_id(self, request_id: str) -> typing.Optional[UUID]: ...


@dataclass(frozen=True)
class AuthorizationResult:
    decision: tool_policy.Decision
    error: typing.Optional[Exception] = None

    @property
    def rule_id(self) -> typing.Optional[UUID]:
        return self.decision.rule_id


_CURRENT: ContextVar["ToolAuthorizationService"] = ContextVar("tool_authorization")


def current() -> ToolAuthorizationService:
    try:
        return _CURRENT.get()
    except LookupError:
        raise RuntimeError("tool authorization requires an execution scope") from None


class ToolAuthorizationService:
    """Own the live gate, delegating policy rules and durable state to their owners."""

    def __init__(
        self,
        data: AuthorizationData,
        policy: typing.Optional[tool_policy.ToolPolicy] = None,
    ) -> None:
        self.policy = policy if policy is not None else tool_policy.ToolPolicy()
        self._data = data

    @contextmanager
    def activate(self) -> Iterator[None]:
        token = _CURRENT.set(self)
        try:
            yield
        finally:
            _CURRENT.reset(token)

    @staticmethod
    def validate(
        binding: typing.Optional[ToolMetadata],
        request: connect.ToolCallBlock,
        definition: typing.Optional[connect.ToolSpec],
    ) -> None:
        if binding is None:
            raise ToolExecutionError(f"unknown tool: {request.name}")
        if definition is None or definition.name != request.name:
            raise ToolExecutionError(f"tool binding does not match: {request.name}")
        binding.validate(request.arguments)

    @staticmethod
    def validate_response(answer: pydantic.JsonValue) -> None:
        response = tool_policy.ToolApprovalResponse.model_validate(answer)
        if response.decision == "deny":
            raise ToolExecutionError(response.reason or "Tool call rejected by host")

    @staticmethod
    async def resolve_auth_context(
        binding: typing.Optional[ToolMetadata], request: connect.ToolCallBlock
    ) -> AuthContext:
        if not isinstance(binding, AuthContextProvider):
            return AuthContext()
        result = await binding.auth_context(request.model_copy(deep=True).arguments)
        if not isinstance(result, AuthContext):
            raise TypeError("auth context callback must return AuthContext")
        return AuthContext.model_validate(result.model_dump(mode="json"))

    @staticmethod
    async def record_authorization(context: ActivityExecutionContext) -> None:
        if not context.authorized:
            await context.record_authorization()

    @staticmethod
    async def _wait(
        context: ActivityExecutionContext,
        channel: str,
        payload: pydantic.JsonValue,
        existing: typing.Optional[mailbox.InputSpec],
    ) -> pydantic.JsonValue:
        if context.mailbox is None:
            raise RuntimeError("tool approval requires a persistent workflow runner")
        spec = (
            existing.specification()
            if existing is not None
            else tool_policy.ToolApprovalRequest.model_validate(payload).input_spec(
                channel
            )
        )
        return await context.mailbox.wait(context.step_id, spec)

    @classmethod
    async def request_approval(
        cls,
        context: ActivityExecutionContext,
        channel: str,
        payload: pydantic.JsonValue,
        binding: typing.Optional[ToolMetadata] = None,
    ) -> pydantic.JsonValue:
        """Serve legacy approval activities without owning their recording boundary."""
        if context.mailbox is None:
            raise RuntimeError("tool approval requires a persistent workflow runner")
        existing = await context.mailbox.existing(context.step_id)
        if existing is None and binding is not None:
            approval = tool_policy.ToolApprovalRequest.model_validate(payload)
            auth_context = await cls.resolve_auth_context(binding, approval.call)
            payload = approval.model_copy(
                update={"auth_context": auth_context}
            ).model_dump(mode="json")
        return await cls._wait(context, channel, payload, existing)

    async def authorize(
        self,
        context: ActivityExecutionContext,
        binding: typing.Optional[ToolMetadata],
        request: connect.ToolCallBlock,
        instance: typing.Optional[str] = None,
    ) -> AuthorizationResult:
        """Persist a successful gate before execution; return only tool-visible errors.

        Authorized steps and existing confirmations retain their prior decisions.
        Mailbox and authorization persistence failures propagate to the runner.
        """
        if context.authorized:
            return AuthorizationResult(
                tool_policy.Decision(
                    action="approve", rule_id=context.authorization_rule_id
                )
            )
        definition = binding.definition if binding is not None else None
        definition_digest = (
            digest(definition.model_dump(mode="json")) if definition else ""
        )
        decision = tool_policy.Decision(action="ask")
        auth_context = AuthContext()
        existing = (
            await context.mailbox.existing(context.step_id)
            if context.mailbox is not None
            else None
        )
        try:
            self.validate(binding, request, definition)
            if existing is None:
                assert binding is not None
                auth_context = await self.resolve_auth_context(binding, request)
                call = tool_policy.ToolCall(
                    name=request.name,
                    version=binding.version,
                    arguments=request.arguments,
                    definition_digest=definition_digest,
                    execution_id=context.execution_id,
                    workflow_id=context.workflow_id,
                    step_id=context.step_id,
                    auth_context=auth_context,
                )
                decision = await self.policy.check(call) or decision
                settings = self._data.settings()
        except Exception as exc:
            return AuthorizationResult(
                tool_policy.Decision(action="deny", reason=str(exc)), exc
            )

        if existing is None and decision.action != "deny":
            rules = await self._data.rules(context.workflow_id, request.name)
            decision = self.policy.resolve(settings, call, rules)

        if decision.action == "ask":
            if context.mailbox is None:
                raise RuntimeError(
                    "tool approval requires a persistent workflow runner"
                )
            assert binding is not None and definition is not None
            approval = binding.approval
            payload = (
                existing.payload
                if existing is not None
                else tool_policy.ToolApprovalRequest(
                    instance=instance,
                    call=request,
                    tool=definition,
                    tool_version=binding.version,
                    auth_context=auth_context,
                ).model_dump(mode="json")
            )
            answer = await self._wait(
                context,
                approval.channel if approval else "tool.approval",
                payload,
                existing,
            )
            try:
                response = tool_policy.ToolApprovalResponse.model_validate(answer)
            except Exception as exc:
                return AuthorizationResult(
                    tool_policy.Decision(action="deny", reason=str(exc)), exc
                )
            decision = tool_policy.Decision(
                action=response.decision,
                reason=response.reason,
                rule_id=await self._data.approval_rule_id(context.step_id),
            )
        if decision.rule_id is not None:
            await context.record_authorization_decision(
                decision.action == "approve", decision.rule_id
            )
        elif decision.action == "approve":
            await self.record_authorization(context)
        error = (
            ToolExecutionError(decision.reason or "Tool call denied by policy")
            if decision.action == "deny"
            else None
        )
        return AuthorizationResult(decision, error)
