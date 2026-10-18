import asyncio
import sqlite3
from pathlib import Path
from typing import Literal
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import connect
import pytest

from vectorops.runner import (
    models,
    tool_authorization_store as stores,
    tool_policy as policy,
)
from vectorops.runner.runner import mailbox
from vectorops.runner.runner.execution_state import ExecutionRecord
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner.runner.workspace_database import WorkspaceDatabase


def value(decision: Literal["approve", "deny"] = "approve") -> policy.RuleInput:
    return policy.RuleInput(
        tool_name="test",
        decision=decision,
        properties={"scope": "external", "nested": {"a": 1, "b": True}},
    )


async def database(path: Path, workspace: stores.WorkspaceRules) -> SQLiteHistory:
    history = SQLiteHistory(path, workspace_rules=workspace)
    await history.save_workflow(
        models.Workflow(id="workflow", definition_ref="test:main")
    )
    await history.save_execution(
        ExecutionRecord(id="execution", root_workflow_id="workflow")
    )
    return history


async def request(
    history: SQLiteHistory,
    identifier: str,
    scope: Literal["workflow", "workspace"],
    decision: Literal["approve", "deny"] = "approve",
) -> mailbox.Answer:
    await history.append_step(
        models.WorkflowStep(
            id=identifier,
            workflow_id="workflow",
            operation=models.StepOperation(kind="llm.tool", name="invoke_tool"),
            outcome=models.StepPending(),
            deferred_delivery=True,
        )
    )
    spec = policy.ToolApprovalRequest(
        call=connect.ToolCallBlock(id=identifier, name="test", arguments={}),
        tool=connect.ToolSpec.external(
            name="test", description="Test", input_schema={"type": "object"}
        ),
        tool_version=1,
        auth_context=policy.AuthContext(properties=value().properties),
    ).input_spec()
    await history.register_input(identifier, spec)
    return mailbox.Answer(
        execution_id="execution",
        request_id=identifier,
        idempotency_key=identifier,
        response={
            "decision": decision,
            "scope": scope,
            "properties": value().properties,
        },
    )


@pytest.mark.parametrize("scope", ["workflow", "workspace"])
@pytest.mark.parametrize("decision", ["approve", "deny"])
async def test_five_parallel_answers_share_one_rule(
    tmp_path: Path,
    scope: Literal["workflow", "workspace"],
    decision: Literal["approve", "deny"],
) -> None:
    workspace = stores.WorkspaceRules(WorkspaceDatabase(tmp_path))
    history = await database(tmp_path / "history.sqlite3", workspace)
    answers = [
        await request(history, str(index), scope, decision) for index in range(5)
    ]
    providers = [
        SQLiteHistory(
            history.path,
            workspace_rules=stores.WorkspaceRules(WorkspaceDatabase(tmp_path)),
        )
        for _ in answers
    ]
    await asyncio.gather(
        *(owner.answer_input(answer) for owner, answer in zip(providers, answers))
    )
    rules = (
        await workspace.list("test")
        if scope == "workspace"
        else await history.authorization_rules("workflow", "test")
    )
    assert len(rules) == 1 and rules[0].decision == decision
    ids = await asyncio.gather(
        *(history.approval_rule_id(answer.request_id) for answer in answers)
    )
    assert ids == [rules[0].id] * 5
    await asyncio.gather(
        *(owner.answer_input(answer) for owner, answer in zip(providers, answers))
    )
    assert (
        await workspace.list("test")
        if scope == "workspace"
        else await history.authorization_rules("workflow", "test")
    ) == rules
    for answer in answers:
        assert (await history.get_input(answer.request_id)).status == "answered"


async def test_workspace_answers_across_executions_share_rule(tmp_path: Path) -> None:
    workspace = stores.WorkspaceRules(WorkspaceDatabase(tmp_path))
    histories = [
        await database(
            tmp_path / str(index) / "history.sqlite3",
            stores.WorkspaceRules(WorkspaceDatabase(tmp_path)),
        )
        for index in range(5)
    ]
    answers = [await request(history, "call", "workspace") for history in histories]
    await asyncio.gather(
        *(history.answer_input(answer) for history, answer in zip(histories, answers))
    )
    (saved,) = await workspace.list("test")
    assert (
        await asyncio.gather(
            *(history.approval_rule_id("call") for history in histories)
        )
        == [saved.id] * 5
    )


@pytest.mark.parametrize("scope", ["workflow", "workspace"])
async def test_concurrent_crud_deduplicates_and_update_conflict_is_atomic(
    tmp_path: Path, scope: str
) -> None:
    workspace = stores.WorkspaceRules(WorkspaceDatabase(tmp_path))
    history = await database(tmp_path / "history.sqlite3", workspace)

    async def save(item: policy.RuleInput) -> policy.AuthorizationRule:
        return (
            await workspace.save(item)
            if scope == "workspace"
            else await history.save_authorization_rule("workflow", item)
        )

    saved = await asyncio.gather(*(save(value()) for _ in range(5)))
    assert len({rule.id for rule in saved}) == 1
    same = value().model_copy(
        update={"properties": {"nested": {"b": True, "a": 1}, "scope": "external"}}
    )
    assert (await save(same)).id == saved[0].id
    denied = await save(value("deny"))
    with pytest.raises(ValueError, match="equivalent"):
        if scope == "workspace":
            await workspace.save(value(), denied.id)
        else:
            await history.save_authorization_rule("workflow", value(), denied.id)
    different = value().model_copy(
        update={"properties": {"scope": "external", "nested": {"a": 1, "b": 1}}}
    )
    await save(different)
    rules = (
        await workspace.list("test")
        if scope == "workspace"
        else await history.authorization_rules("workflow", "test")
    )
    assert len(rules) == 3
    assert next(rule for rule in rules if rule.id == denied.id).decision == "deny"
    with pytest.raises(KeyError):
        if scope == "workspace":
            await workspace.save(value(), uuid4())
        else:
            await history.save_authorization_rule("workflow", value(), uuid4())


async def test_import_retries_keep_canonical_id_after_deletion(tmp_path: Path) -> None:
    workspace = stores.WorkspaceRules(WorkspaceDatabase(tmp_path))
    original = await workspace.save(value())
    incoming = policy.AuthorizationRule(**value().model_dump(), id=uuid4())
    assert await workspace.import_rule(incoming) == original.id
    assert await workspace.list("test") == [original]
    await workspace.delete(original.id)
    reopened = stores.WorkspaceRules(WorkspaceDatabase(tmp_path))
    assert await reopened.import_rule(incoming) == original.id
    assert await reopened.list("test") == []
    fresh = policy.AuthorizationRule(**value().model_dump(), id=uuid4())
    assert await reopened.import_rule(fresh) == fresh.id


async def test_outbox_retry_preserves_canonical_receipt(tmp_path: Path) -> None:
    workspace = stores.WorkspaceRules(WorkspaceDatabase(tmp_path))
    original = await workspace.save(value())
    history = await database(tmp_path / "history.sqlite3", workspace)
    answer = await request(history, "call", "workspace")
    imported = workspace.import_rule

    async def interrupt(rule: policy.AuthorizationRule) -> None:
        await imported(rule)
        raise OSError("interrupted after import")

    with patch.object(workspace, "import_rule", new=AsyncMock(side_effect=interrupt)):
        with pytest.raises(OSError, match="interrupted"):
            await history.answer_input(answer)
    await workspace.delete(original.id)
    reopened = SQLiteHistory(
        history.path, workspace_rules=stores.WorkspaceRules(WorkspaceDatabase(tmp_path))
    )
    await reopened.publish_authorization_rules()
    assert await reopened.approval_rule_id("call") == original.id
    assert await workspace.list("test") == []


async def test_branch_keeps_shared_rule_until_all_supporting_requests_removed(
    tmp_path: Path,
) -> None:
    workspace = stores.WorkspaceRules(WorkspaceDatabase(tmp_path))
    history = await database(tmp_path / "history.sqlite3", workspace)
    answers = [await request(history, str(index), "workflow") for index in range(5)]
    for answer in answers:
        await history.answer_input(answer)
    (saved,) = await history.authorization_rules("workflow", "test")
    with sqlite3.connect(history.path) as connection:
        stores.SQLiteWorkflowGrants.retain(connection, {"4"})
    assert await history.authorization_rules("workflow", "test") == [saved]
    with sqlite3.connect(history.path) as connection:
        stores.SQLiteWorkflowGrants.retain(connection, set())
    assert await history.authorization_rules("workflow", "test") == []
    assert await history.approval_rule_id("4") == saved.id


@pytest.mark.parametrize("manual_first", [True, False])
async def test_manual_workflow_rule_survives_branch_cleanup(
    tmp_path: Path, manual_first: bool
) -> None:
    workspace = stores.WorkspaceRules(WorkspaceDatabase(tmp_path))
    history = await database(tmp_path / "history.sqlite3", workspace)
    if manual_first:
        await history.save_authorization_rule("workflow", value())
    await history.answer_input(await request(history, "call", "workflow"))
    saved = await history.save_authorization_rule("workflow", value())
    assert await history.approval_rule_id("call") == saved.id
    with sqlite3.connect(history.path) as connection:
        stores.SQLiteWorkflowGrants.retain(connection, set())
    assert await history.authorization_rules("workflow", "test") == [saved]


async def test_workflow_deduplication_does_not_cross_scope(tmp_path: Path) -> None:
    workspace = stores.WorkspaceRules(WorkspaceDatabase(tmp_path))
    history = await database(tmp_path / "history.sqlite3", workspace)
    await history.save_workflow(models.Workflow(id="other", definition_ref="test:main"))
    first = await history.save_authorization_rule("workflow", value())
    second = await history.save_authorization_rule("other", value())
    global_rule = await workspace.save(value())
    assert len({first.id, second.id, global_rule.id}) == 3
