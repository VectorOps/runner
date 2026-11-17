from dataclasses import replace
import hashlib
import json
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from tests.runner import test_tool_authorization as gate
from tests.runner.test_tool_safety import assessment
from vectorops.runner import tool_authorization, tool_policy, tool_safety
from vectorops.runner.runner import mailbox
from vectorops.runner.runner.errors import AuthorizationPersistenceError


def recovered_assessment(approved: bool) -> tool_safety.Assessment:
    binding = gate.Binding()
    payload = {
        "tool": "test",
        "version": 1,
        "definition_digest": tool_authorization.digest(
            binding.definition.model_dump(mode="json")
        ),
        "description": binding.definition.description,
        "arguments": {},
    }
    return assessment(approved).model_copy(
        update={
            "input_digest": hashlib.sha256(
                json.dumps(payload, sort_keys=True, allow_nan=False).encode()
            ).hexdigest()
        }
    )


@pytest.mark.parametrize("approved", [True, False])
async def test_assessment_is_persisted_before_authorization_or_human_wait(
    approved: bool,
) -> None:
    result = assessment(approved)
    assessor = Mock(spec=tool_safety.Assessor)
    assessor.assess = AsyncMock(return_value=result)
    trace: list[str] = []

    async def record(value: tool_safety.Assessment) -> None:
        assert value == result
        trace.append("assessment")

    async def authorize() -> None:
        trace.append("authorization")

    async def wait(*args: object) -> dict[str, str]:
        trace.append("human")
        return {"decision": "approve"}

    box = Mock(spec=mailbox.Mailbox)
    box.existing = AsyncMock(return_value=None)
    box.wait = AsyncMock(side_effect=wait)
    context = replace(
        gate.context(authorize, box=box), persist_safety_assessment=record
    )
    service = tool_authorization.ToolAuthorizationService(
        gate.data(tool_policy.ApprovalSettings), safety=assessor
    )
    decision = await service.authorize(context, gate.Binding(), gate.request())
    assert decision.error is None and decision.rule_id is None
    assert trace == (
        ["assessment", "authorization"]
        if approved
        else ["assessment", "human", "authorization"]
    )
    assessor.assess.assert_awaited_once()


@pytest.mark.parametrize("approved", [True, False])
async def test_recovered_assessment_is_not_repeated(approved: bool) -> None:
    assessor = Mock(spec=tool_safety.Assessor)
    assessor.assess = AsyncMock(side_effect=AssertionError("assessment repeated"))
    box = Mock(spec=mailbox.Mailbox)
    box.existing = AsyncMock(return_value=None)
    box.wait = AsyncMock(return_value={"decision": "deny"})
    persist = AsyncMock()
    context = replace(
        gate.context(persist, box=box), safety_assessment=recovered_assessment(approved)
    )
    service = tool_authorization.ToolAuthorizationService(
        gate.data(tool_policy.ApprovalSettings), safety=assessor
    )
    result = await service.authorize(context, gate.Binding(), gate.request())
    assert (result.error is None) is approved
    assert box.wait.await_count == int(not approved)
    assert persist.await_count == int(approved)
    assessor.assess.assert_not_awaited()


@pytest.mark.parametrize("change", ["arguments", "definition", "version"])
async def test_recovered_approval_is_bound_to_original_call(change: str) -> None:
    assessor = Mock(spec=tool_safety.Assessor)
    assessor.assess = AsyncMock(side_effect=AssertionError("reassessment"))
    binding = gate.Binding()
    request = gate.request()
    if change == "arguments":
        request = request.model_copy(update={"arguments": {"value": "changed"}})
    elif change == "definition":
        binding.definition = binding.definition.model_copy(
            update={"description": "changed"}
        )
    else:
        binding.version = 2
    persist = AsyncMock()
    context = replace(
        gate.context(persist), safety_assessment=recovered_assessment(True)
    )
    service = tool_authorization.ToolAuthorizationService(
        gate.data(tool_policy.ApprovalSettings), safety=assessor
    )
    result = await service.authorize(context, binding, request)
    assert result.error is not None and "changed" in str(result.error)
    persist.assert_not_awaited()
    assessor.assess.assert_not_awaited()


@pytest.mark.parametrize(
    "path",
    [
        "authorized",
        "pending",
        "approve_all",
        "allow_rule",
        "deny_rule",
        "mandatory",
        "invalid",
    ],
)
async def test_assessor_cannot_override_existing_authority(path: str) -> None:
    assessor = Mock(spec=tool_safety.Assessor)
    assessor.assess = AsyncMock(side_effect=AssertionError("unexpected assessment"))
    store = gate.data(
        lambda: tool_policy.ApprovalSettings(approve_all=path == "approve_all")
    )
    policy = tool_policy.ToolPolicy()
    if path == "mandatory":
        policy.register_check("mandatory", AsyncMock(return_value="blocked"))
    if path.endswith("_rule"):
        store.rules.return_value = [
            tool_policy.ScopedRule(
                scope="workspace",
                rule=tool_policy.AuthorizationRule(
                    id=uuid4(),
                    tool_name="test",
                    decision="approve" if path == "allow_rule" else "deny",
                ),
            )
        ]
    box = Mock(spec=mailbox.Mailbox)
    existing = tool_policy.ToolApprovalRequest(
        call=gate.request(), tool=gate.Binding().definition, tool_version=1
    ).input_spec()
    box.existing = AsyncMock(return_value=existing if path == "pending" else None)
    box.wait = AsyncMock(return_value={"decision": "deny"})
    context = gate.context(AsyncMock(), box=box, authorized=path == "authorized")
    service = tool_authorization.ToolAuthorizationService(
        store, policy, safety=assessor
    )
    result = await service.authorize(
        context, gate.Binding(invalid=path == "invalid"), gate.request()
    )
    assert (result.error is None) is (
        path in {"authorized", "approve_all", "allow_rule"}
    )
    assessor.assess.assert_not_awaited()


@pytest.mark.parametrize("missing", [False, True])
async def test_assessment_storage_failure_prevents_execution_and_human_request(
    missing: bool,
) -> None:
    assessor = Mock(spec=tool_safety.Assessor)
    assessor.assess = AsyncMock(return_value=assessment())
    box = Mock(spec=mailbox.Mailbox)
    box.existing = AsyncMock(return_value=None)
    persist = AsyncMock()
    context = replace(
        gate.context(persist, box=box),
        persist_safety_assessment=(
            None if missing else AsyncMock(side_effect=OSError("write failed"))
        ),
    )
    service = tool_authorization.ToolAuthorizationService(
        gate.data(tool_policy.ApprovalSettings), safety=assessor
    )
    with pytest.raises(AuthorizationPersistenceError):
        await service.authorize(context, gate.Binding(), gate.request())
    persist.assert_not_awaited()
    box.wait.assert_not_called()


async def test_disabled_assessor_preserves_human_confirmation() -> None:
    assessor = Mock(spec=tool_safety.Assessor)
    assessor.assess = AsyncMock(return_value=None)
    box = Mock(spec=mailbox.Mailbox)
    box.existing = AsyncMock(return_value=None)
    box.wait = AsyncMock(return_value={"decision": "deny"})
    record = AsyncMock()
    context = replace(
        gate.context(AsyncMock(), box=box), persist_safety_assessment=record
    )
    service = tool_authorization.ToolAuthorizationService(
        gate.data(tool_policy.ApprovalSettings), safety=assessor
    )
    result = await service.authorize(context, gate.Binding(), gate.request())
    assert result.error is not None
    box.wait.assert_awaited_once()
    record.assert_not_awaited()
