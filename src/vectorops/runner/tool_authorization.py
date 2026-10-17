"""Tool validation and authorization, independent of workflow recording and execution."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
import hashlib
import json
import typing

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

    async def tool_approved(self, workflow_id: str, tool_name: str) -> bool: ...


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
        if not response.approved:
            raise ToolExecutionError(response.reason or "Tool call rejected by host")

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
    ) -> pydantic.JsonValue:
        """Serve legacy approval activities without owning their recording boundary."""
        if context.mailbox is None:
            raise RuntimeError("tool approval requires a persistent workflow runner")
        existing = await context.mailbox.existing(context.step_id)
        return await cls._wait(context, channel, payload, existing)

    async def authorize(
        self,
        context: ActivityExecutionContext,
        binding: typing.Optional[ToolMetadata],
        request: connect.ToolCallBlock,
        instance: typing.Optional[str] = None,
    ) -> typing.Optional[Exception]:
        """Persist a successful gate before execution; return only tool-visible errors.

        Authorized steps and existing confirmations retain their prior decisions.
        Mailbox and authorization persistence failures propagate to the runner.
        """
        if context.authorized:
            return None
        definition = binding.definition if binding is not None else None
        definition_digest = (
            digest(definition.model_dump(mode="json")) if definition else ""
        )
        decision = tool_policy.Decision(action="require_confirmation")
        existing = (
            await context.mailbox.existing(context.step_id)
            if context.mailbox is not None
            else None
        )
        try:
            self.validate(binding, request, definition)
            if existing is None:
                assert binding is not None
                decision = await self.policy.evaluate(
                    self._data.settings(),
                    tool_policy.ToolCall(
                        name=request.name,
                        version=binding.version,
                        arguments=request.arguments,
                        definition_digest=definition_digest,
                        execution_id=context.execution_id,
                        workflow_id=context.workflow_id,
                        step_id=context.step_id,
                    ),
                )
            if decision.action == "deny":
                raise ToolExecutionError(
                    decision.reason or "Tool call denied by policy"
                )
        except Exception as exc:
            return exc

        if (
            existing is None
            and decision.action == "require_confirmation"
            and context.mailbox is not None
            and await self._data.tool_approved(context.workflow_id, request.name)
        ):
            decision = tool_policy.Decision(action="allow")

        if decision.action == "require_confirmation":
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
                ).model_dump(mode="json")
            )
            answer = await self._wait(
                context,
                approval.channel if approval else "tool.approval",
                payload,
                existing,
            )
            try:
                self.validate_response(answer)
            except Exception as exc:
                return exc
        await self.record_authorization(context)
        return None
