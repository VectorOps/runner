import asyncio
from collections.abc import Awaitable, Callable
from typing import Optional
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

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
        persist_authorization_decision=AsyncMock(),
    )


def data(settings: Callable[[], tool_policy.ApprovalSettings]) -> Mock:
    store = Mock(spec=authorization.AuthorizationData)
    store.settings = settings
    store.rules = AsyncMock(return_value=[])
    store.approval_rule_id = AsyncMock(return_value=None)
    return store


async def test_service_records_authorization_without_execution() -> None:
    persist = AsyncMock()
    settings = Mock(return_value=tool_policy.ApprovalSettings(approve_all=True))
    service = authorization.ToolAuthorizationService(data(settings))
    binding = Binding()
    assert (await service.authorize(context(persist), binding, request())).error is None
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
    service = authorization.ToolAuthorizationService(data(settings))
    error = (await service.authorize(context(persist), binding, request())).error
    assert error is not None and message in str(error)
    settings.assert_not_called()
    persist.assert_not_awaited()


async def test_authorized_step_skips_entire_boundary() -> None:
    persist = AsyncMock()
    settings = Mock(side_effect=AssertionError("settings during recovery"))
    service = authorization.ToolAuthorizationService(data(settings))
    binding = Binding(invalid=True)
    assert (
        await service.authorize(context(persist, authorized=True), binding, request())
    ).error is None
    assert binding.validations == 0
    settings.assert_not_called()
    persist.assert_not_awaited()


async def test_authorization_persistence_failure_propagates() -> None:
    persist = AsyncMock(side_effect=OSError("write failed"))
    service = authorization.ToolAuthorizationService(
        data(lambda: tool_policy.ApprovalSettings(approve_all=True))
    )
    with pytest.raises(AuthorizationPersistenceError):
        await service.authorize(context(persist), Binding(), request())


async def test_mandatory_denial_precedes_workflow_grant() -> None:
    policy = tool_policy.ToolPolicy()

    async def deny(call: tool_policy.ToolCall) -> Optional[str]:
        return "denied"

    policy.register_check("deny", deny)
    store = data(tool_policy.ApprovalSettings)
    store.rules.return_value = []
    service = authorization.ToolAuthorizationService(store, policy)
    box = Mock(spec=mailbox.Mailbox)
    box.existing = AsyncMock(return_value=None)
    persist = AsyncMock()
    error = (
        await service.authorize(context(persist, box=box), Binding(), request())
    ).error
    assert isinstance(error, authorization.ToolExecutionError)
    assert str(error) == "denied"
    store.rules.assert_not_awaited()
    box.wait.assert_not_called()
    persist.assert_not_awaited()


@pytest.mark.parametrize("approved", [True, False])
async def test_existing_confirmation_keeps_contract_and_decision(
    approved: bool,
) -> None:
    settings = Mock(side_effect=AssertionError("policy repeated"))
    store = data(settings)
    service = authorization.ToolAuthorizationService(store)
    binding = Binding()
    spec = tool_policy.ToolApprovalRequest(
        call=request(), tool=binding.definition, tool_version=1
    ).input_spec("original.channel")
    box = Mock(spec=mailbox.Mailbox)
    box.existing = AsyncMock(return_value=spec)
    box.wait = AsyncMock(return_value={"decision": "approve" if approved else "deny"})
    persist = AsyncMock()
    error = (
        await service.authorize(context(persist, box=box), binding, request())
    ).error
    assert (error is None) is approved
    box.wait.assert_awaited_once_with("step", spec)
    store.rules.assert_not_called()
    settings.assert_not_called()
    assert persist.await_count == int(approved)


async def test_mailbox_failure_propagates_without_authorizing() -> None:
    service = authorization.ToolAuthorizationService(data(tool_policy.ApprovalSettings))
    box = Mock(spec=mailbox.Mailbox)
    box.existing = AsyncMock(return_value=None)
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


async def test_execution_scopes_are_isolated_and_restored() -> None:
    first = authorization.ToolAuthorizationService(data(tool_policy.ApprovalSettings))
    second = authorization.ToolAuthorizationService(data(tool_policy.ApprovalSettings))
    ready = asyncio.Event()

    async def run(service: authorization.ToolAuthorizationService) -> None:
        with service.activate():
            ready.set()
            await asyncio.sleep(0)
            assert authorization.current() is service
        with pytest.raises(RuntimeError, match="execution scope"):
            authorization.current()

    await asyncio.gather(run(first), run(second))
    assert ready.is_set()
    with first.activate():
        with pytest.raises(ValueError):
            with second.activate():
                assert authorization.current() is second
                raise ValueError("interrupted")
        assert authorization.current() is first
    with pytest.raises(RuntimeError, match="execution scope"):
        authorization.current()


async def test_workflow_grant_lookup_is_independent_of_mailbox() -> None:
    store = data(tool_policy.ApprovalSettings)
    rule = tool_policy.AuthorizationRule(
        id=uuid4(), tool_name="test", decision="approve"
    )
    store.rules.return_value = [tool_policy.ScopedRule(scope="workflow", rule=rule)]
    service = authorization.ToolAuthorizationService(store)
    box = Mock(spec=mailbox.Mailbox)
    box.existing = AsyncMock(return_value=None)
    persist = AsyncMock()
    result = await service.authorize(context(persist, box=box), Binding(), request())
    assert result.error is None and result.rule_id == rule.id
    store.rules.assert_awaited_once_with("workflow", "test")
    box.wait.assert_not_called()
    persist.assert_not_awaited()


async def test_workflow_grant_storage_failure_propagates() -> None:
    store = data(tool_policy.ApprovalSettings)
    store.rules.side_effect = OSError("grant storage failed")
    service = authorization.ToolAuthorizationService(store)
    box = Mock(spec=mailbox.Mailbox)
    box.existing = AsyncMock(return_value=None)
    persist = AsyncMock()
    with pytest.raises(OSError, match="grant storage failed"):
        await service.authorize(context(persist, box=box), Binding(), request())
    box.wait.assert_not_called()
    persist.assert_not_awaited()
