import itertools
import sqlite3
from pathlib import Path
from typing import Literal, Optional
from unittest.mock import AsyncMock, Mock, patch
from uuid import UUID, uuid4

import connect
import pydantic
import pytest
from vectorops.runner import tool_authorization

from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from tests.runner.test_mailbox import store
from vectorops.runner import (
    models,
    tool_policy as p,
    tool_authorization_store as stores,
)
from vectorops.runner.api import tool_contracts as tools
from vectorops.runner.api.tool_execution import invoke_tool
from vectorops.runner.runner import mailbox
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner.runner.workspace_database import WorkspaceDatabase


def rule(
    properties: dict[str, pydantic.JsonValue],
    decision: Literal["approve", "deny"] = "approve",
    scope: Literal["workspace", "workflow"] = "workspace",
    *,
    name: str = "read",
    identifier: Optional[UUID] = None,
) -> p.ScopedRule:
    return p.ScopedRule(
        scope=scope,
        rule=p.AuthorizationRule(
            id=identifier or uuid4(),
            tool_name=name,
            properties=properties,
            decision=decision,
        ),
    )


def call(properties: dict[str, pydantic.JsonValue]) -> p.ToolCall:
    return p.ToolCall(
        name="read",
        version=1,
        arguments={},
        definition_digest="digest",
        workflow_id="workflow",
        step_id="step",
        auth_context=p.AuthContext(properties=properties),
    )


@pytest.mark.parametrize(
    "mask,properties,expected",
    [
        ({}, {}, True),
        ({}, {"scope": "internal"}, True),
        ({"scope": "internal"}, {"scope": "internal", "mode": "read"}, True),
        ({"scope": "external"}, {"scope": "internal"}, False),
        ({"scope": None}, {}, False),
        ({"scope": None}, {"scope": None}, True),
        ({"flag": True}, {"flag": 1}, False),
        ({"nested": {"a": 1}}, {"nested": {"a": 1, "b": 2}}, False),
    ],
)
def test_property_matching(
    mask: dict[str, pydantic.JsonValue],
    properties: dict[str, pydantic.JsonValue],
    expected: bool,
) -> None:
    assert p.matches(mask, properties) is expected


def test_specificity_scope_deny_and_stable_id_order() -> None:
    properties: dict[str, pydantic.JsonValue] = {"scope": "internal", "mode": "read"}
    generic = rule({}, "deny", "workflow")
    specific = rule(properties, "approve", "workspace")
    workflow = rule(properties, "approve", "workflow")
    deny = rule(properties, "deny", "workflow")
    tied = rule(properties, "deny", "workflow", identifier=UUID(int=1))
    for rules, winner in [
        ([generic, specific], specific),
        ([specific, workflow], workflow),
        ([workflow, deny], deny),
        ([deny, tied], tied),
    ]:
        for ordered in itertools.permutations(rules):
            result = p.ToolPolicy.resolve(
                p.ApprovalSettings(), call(properties), ordered
            )
            assert result.rule_id == winner.rule.id
            assert result.action == winner.rule.decision


def test_conflicting_disjoint_masks_and_unmatched_default() -> None:
    approved = rule({"scope": "internal"})
    denied = rule({"mode": "read"}, "deny")
    result = p.ToolPolicy.resolve(
        p.ApprovalSettings(approve_all=True),
        call({"scope": "internal", "mode": "read"}),
        [approved, denied],
    )
    assert result.action == "deny" and result.rule_id == denied.rule.id
    unmatched = [rule({"scope": "external"}), rule({}, name="other")]
    assert p.ToolPolicy.resolve(
        p.ApprovalSettings(), call({"scope": "internal"}), unmatched
    ) == p.Decision(action="ask")
    assert (
        p.ToolPolicy.resolve(
            p.ApprovalSettings(approve_all=True), call({}), unmatched
        ).action
        == "approve"
    )


async def test_mandatory_checks_override_specific_approval() -> None:
    policy = p.ToolPolicy()

    async def deny(request: p.ToolCall) -> str:
        return "mandatory"

    policy.register_check("deny", deny)
    assert await policy.evaluate(
        p.ApprovalSettings(approve_all=True), call({}), [rule({})]
    ) == p.Decision(action="deny", reason="mandatory")


@pytest.mark.parametrize("decision", ["approve", "deny"])
async def test_step_records_winning_rule_and_replay_preserves_it(
    history: MemoryHistory, decision: Literal["approve", "deny"]
) -> None:
    class Arguments(pydantic.BaseModel):
        pass

    selected = rule({}, decision)
    executions = 0

    async def execute(
        ctx: ActivityExecutionContext, args: Arguments
    ) -> tools.ToolResponse:
        nonlocal executions
        executions += 1
        assert (
            await history.get_step(ctx.step_id)
        ).authorization_rule_id == selected.rule.id
        return tools.ToolResponse(content=[])

    async def workflow() -> pydantic.JsonValue:
        result = await invoke_tool(
            tools.Tool("read", "Read", Arguments, execute),
            connect.ToolCallBlock(id="call", name="read", arguments={}),
        )
        assert result.is_error is (decision == "deny")
        return None

    result = await replay.run(
        history, workflow, tool_policy_version=1, rules=[selected]
    )
    assert result.outcome.kind == "completed"
    step = next(
        step for step in history.steps.values() if step.operation.kind == "llm.tool"
    )
    assert step.authorization_rule_id == selected.rule.id
    assert step.authorized is (decision == "approve")
    assert models.WorkflowStep.model_validate_json(step.model_dump_json()) == step
    with patch.object(
        p.ToolPolicy, "check", side_effect=AssertionError("re-evaluation")
    ):
        assert (
            await replay.run(history, workflow, result.id, tool_policy_version=1)
            == result
        )
    assert executions == int(decision == "approve")


async def register(database: SQLiteHistory) -> mailbox.Answer:
    await database.append_step(
        models.WorkflowStep(
            id="tool",
            workflow_id="workflow",
            operation=models.StepOperation(kind="llm.tool", name="invoke_tool"),
            outcome=models.StepPending(),
        )
    )
    spec = p.ToolApprovalRequest(
        call=connect.ToolCallBlock(id="call", name="read", arguments={}),
        tool=connect.ToolSpec.external(
            name="read", description="Read", input_schema={"type": "object"}
        ),
        tool_version=1,
        auth_context=p.AuthContext(properties={"scope": "internal", "mode": "read"}),
    ).input_spec()
    await database.register_input("tool", spec)
    return mailbox.Answer(
        execution_id="execution",
        request_id="tool",
        idempotency_key="answer",
        response={
            "decision": "approve",
            "scope": "workflow",
            "properties": {"scope": "internal"},
        },
    )


async def test_workflow_rule_crud_mask_and_atomic_rejection(tmp_path: Path) -> None:
    database = await store(tmp_path / "history.sqlite3")
    answer = await register(database)
    with pytest.raises(ValueError, match="must match"):
        await database.answer_input(
            answer.model_copy(
                update={
                    "response": {
                        "decision": "approve",
                        "scope": "workflow",
                        "properties": {"scope": "external"},
                    }
                }
            )
        )
    assert (await database.get_input("tool")).status == "pending"
    assert await database.authorization_rules("workflow") == []
    await database.answer_input(answer)
    (saved,) = await database.authorization_rules("workflow")
    assert saved.properties == {"scope": "internal"}
    assert await database.approval_rule_id("tool") == saved.id
    await database.answer_input(answer)
    assert await database.authorization_rules("workflow") == [saved]
    updated = await database.save_authorization_rule(
        "workflow", p.RuleInput(tool_name="read", decision="deny"), saved.id
    )
    assert updated.id == saved.id and updated.decision == "deny"
    await database.delete_authorization_rule("workflow", saved.id)
    assert await database.authorization_rules("workflow") == []
    assert await database.approval_rule_id("tool") == saved.id


async def test_workspace_outbox_recovery_and_deleted_rule_not_resurrected(
    tmp_path: Path,
) -> None:
    seeded = await store(tmp_path / "history.sqlite3")
    workspace = stores.WorkspaceRules(WorkspaceDatabase(tmp_path))
    database = SQLiteHistory(seeded.path, workspace_rules=workspace)
    answer = (await register(database)).model_copy(
        update={
            "response": {
                "decision": "deny",
                "scope": "workspace",
                "properties": {"scope": "internal"},
            }
        }
    )
    with patch.object(
        workspace, "import_rule", new=AsyncMock(side_effect=OSError("interrupted"))
    ):
        with pytest.raises(OSError, match="interrupted"):
            await database.answer_input(answer)
    assert (await database.get_input("tool")).status == "answered"
    identifier = await database.approval_rule_id("tool")
    assert identifier is not None
    reopened = SQLiteHistory(seeded.path, workspace_rules=workspace)
    await reopened.publish_authorization_rules()
    (saved,) = await workspace.list("read")
    assert saved.id == identifier and saved.decision == "deny"
    assert saved.properties == {"scope": "internal"}
    await workspace.delete(saved.id)
    await workspace.import_rule(saved)
    await reopened.answer_input(answer)
    assert await workspace.list() == []


def test_ask_is_not_storable_or_a_host_decision() -> None:
    with pytest.raises(pydantic.ValidationError):
        p.RuleInput.model_validate({"tool_name": "read", "decision": "ask"})
    with pytest.raises(pydantic.ValidationError):
        p.ToolApprovalResponse.model_validate({"decision": "ask"})


async def test_rule_id_returned_on_denial() -> None:
    selected = rule({}, "deny")
    data = Mock(spec=tool_authorization.AuthorizationData)
    data.settings.return_value = p.ApprovalSettings()
    data.rules = AsyncMock(return_value=[selected])
    persist = AsyncMock()
    context = ActivityExecutionContext(
        None, "workflow", "step", persist_authorization_decision=persist
    )

    class Arguments(pydantic.BaseModel):
        pass

    async def execute(
        ctx: ActivityExecutionContext, args: Arguments
    ) -> tools.ToolResponse:
        raise AssertionError("must not execute")

    result = await tool_authorization.ToolAuthorizationService(data).authorize(
        context,
        tools.Tool("read", "Read", Arguments, execute),
        connect.ToolCallBlock(id="call", name="read", arguments={}),
    )
    assert result.rule_id == selected.rule.id
    assert result.decision.action == "deny"
    assert isinstance(result.error, tool_authorization.ToolExecutionError)
    persist.assert_awaited_once_with(False, selected.rule.id)


async def test_branch_retains_manual_rules_and_removes_answer_rules(
    tmp_path: Path,
) -> None:
    database = await store(tmp_path / "history.sqlite3")
    await database.answer_input(await register(database))
    manual = await database.save_authorization_rule(
        "workflow", p.RuleInput(tool_name="other", decision="deny")
    )
    with sqlite3.connect(database.path) as connection:
        stores.SQLiteWorkflowGrants.retain(connection, set())
    assert await database.authorization_rules("workflow") == [manual]
    assert await database.approval_rule_id("tool") is not None


async def test_workspace_crud_does_not_lose_concurrent_changes(tmp_path: Path) -> None:
    import asyncio

    database = WorkspaceDatabase(tmp_path)
    first = stores.WorkspaceRules(database)
    second = stores.WorkspaceRules(database)
    created = await asyncio.gather(
        *(
            owner.save(p.RuleInput(tool_name=f"tool_{index}", decision="approve"))
            for index, owner in enumerate([first, second] * 5)
        )
    )
    assert {item.id for item in await first.list()} == {item.id for item in created}
    with pytest.raises(KeyError):
        await first.save(p.RuleInput(tool_name="missing", decision="deny"), uuid4())
    assert len(await second.list()) == 10
