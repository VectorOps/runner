from collections.abc import Awaitable, Callable
from typing import Optional
from unittest.mock import AsyncMock, Mock

import connect
import pydantic
import pytest

from vectorops.runner import tool_authorization as authorization, tool_policy
from vectorops.runner.api import tool_contracts
from vectorops.runner.runner import mailbox
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.errors import AuthorizationPersistenceError


class Binding:
    def __init__(self, name: str = "test", *, invalid: bool = False) -> None:
        self.definition = connect.ToolSpec.external(
            name=name, description="Test", input_schema={"type": "object"}
        )
        self.version = 1
        self.approval = authorization.ToolApproval(channel="custom.approval")
        self.invalid = invalid
        self.validations = 0

    def validate(self, arguments: dict[str, pydantic.JsonValue]) -> None:
        self.validations += 1
        if self.invalid:
            raise ValueError("invalid arguments")


def request() -> connect.ToolCallBlock:
    return connect.ToolCallBlock(id="call", name="test", arguments={})


def context(
    persist: Callable[[], Awaitable[None]],
    *,
    box: Optional[mailbox.Mailbox] = None,
    authorized: bool = False,
) -> ActivityExecutionContext:
    return ActivityExecutionContext(
        "execution",
        "workflow",
        "step",
        mailbox=box,
        authorized=authorized,
        persist_authorization=persist,
    )


async def test_service_records_authorization_without_execution() -> None:
    persist = AsyncMock()
    settings = Mock(return_value=tool_policy.ApprovalSettings(approve_all=True))
    service = authorization.ToolAuthorizationService(tool_policy.ToolPolicy(), settings)
    binding = Binding()
    assert await service.authorize(context(persist), binding, request()) is None
    assert binding.validations == 1
    settings.assert_called_once_with()
    persist.assert_awaited_once_with()


@pytest.mark.parametrize(
    "binding,message",
    [
        (None, "unknown tool"),
        (Binding("other"), "tool binding does not match"),
        (Binding(invalid=True), "invalid arguments"),
    ],
)
async def test_validation_precedes_policy_and_persistence(
    binding: Optional[Binding], message: str
) -> None:
    persist = AsyncMock()
    settings = Mock(side_effect=AssertionError("settings before validation"))
    service = authorization.ToolAuthorizationService(tool_policy.ToolPolicy(), settings)
    error = await service.authorize(context(persist), binding, request())
    assert error is not None and message in str(error)
    settings.assert_not_called()
    persist.assert_not_awaited()


async def test_authorized_step_skips_entire_boundary() -> None:
    persist = AsyncMock()
    settings = Mock(side_effect=AssertionError("settings during recovery"))
    service = authorization.ToolAuthorizationService(tool_policy.ToolPolicy(), settings)
    binding = Binding(invalid=True)
    assert (
        await service.authorize(context(persist, authorized=True), binding, request())
        is None
    )
    assert binding.validations == 0
    settings.assert_not_called()
    persist.assert_not_awaited()


async def test_authorization_persistence_failure_propagates() -> None:
    persist = AsyncMock(side_effect=OSError("write failed"))
    service = authorization.ToolAuthorizationService(
        tool_policy.ToolPolicy(), lambda: tool_policy.ApprovalSettings(approve_all=True)
    )
    with pytest.raises(AuthorizationPersistenceError):
        await service.authorize(context(persist), Binding(), request())


async def test_mandatory_denial_precedes_workflow_grant() -> None:
    policy = tool_policy.ToolPolicy()

    async def deny(call: tool_policy.ToolCall) -> Optional[str]:
        return "denied"

    policy.register_check("deny", deny)
    service = authorization.ToolAuthorizationService(
        policy, tool_policy.ApprovalSettings
    )
    box = Mock(spec=mailbox.Mailbox)
    box.existing = AsyncMock(return_value=None)
    box.tool_approved = AsyncMock(return_value=True)
    persist = AsyncMock()
    error = await service.authorize(context(persist, box=box), Binding(), request())
    assert isinstance(error, authorization.ToolExecutionError)
    assert str(error) == "denied"
    box.tool_approved.assert_not_awaited()
    box.wait.assert_not_called()
    persist.assert_not_awaited()


@pytest.mark.parametrize("approved", [True, False])
async def test_existing_confirmation_keeps_contract_and_decision(
    approved: bool,
) -> None:
    settings = Mock(side_effect=AssertionError("policy repeated"))
    service = authorization.ToolAuthorizationService(tool_policy.ToolPolicy(), settings)
    binding = Binding()
    spec = tool_policy.ToolApprovalRequest(
        call=request(), tool=binding.definition, tool_version=1
    ).input_spec("original.channel")
    box = Mock(spec=mailbox.Mailbox)
    box.existing = AsyncMock(return_value=spec)
    box.wait = AsyncMock(return_value={"approved": approved})
    persist = AsyncMock()
    error = await service.authorize(context(persist, box=box), binding, request())
    assert (error is None) is approved
    box.wait.assert_awaited_once_with("step", spec)
    box.tool_approved.assert_not_called()
    settings.assert_not_called()
    assert persist.await_count == int(approved)


async def test_mailbox_failure_propagates_without_authorizing() -> None:
    service = authorization.ToolAuthorizationService(
        tool_policy.ToolPolicy(), tool_policy.ApprovalSettings
    )
    box = Mock(spec=mailbox.Mailbox)
    box.existing = AsyncMock(return_value=None)
    box.tool_approved = AsyncMock(return_value=False)
    box.wait = AsyncMock(side_effect=OSError("mailbox failed"))
    persist = AsyncMock()
    with pytest.raises(OSError, match="mailbox failed"):
        await service.authorize(context(persist, box=box), Binding(), request())
    persist.assert_not_awaited()


def test_public_contracts_reexport_runtime_types() -> None:
    assert tool_contracts.ToolApproval is authorization.ToolApproval
    assert tool_contracts.ToolMetadata is authorization.ToolMetadata
    assert tool_contracts.ToolExecutionError is authorization.ToolExecutionError
    assert tool_contracts.ToolApprovalRequest is tool_policy.ToolApprovalRequest
    assert tool_contracts.ToolApprovalResponse is tool_policy.ToolApprovalResponse
