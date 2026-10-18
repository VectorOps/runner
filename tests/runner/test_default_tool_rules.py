import asyncio
from pathlib import Path
from typing import Literal, Optional
from uuid import uuid4

import pytest

from vectorops.runner import tool_policy as policy, tool_authorization_store as stores
from vectorops.runner.runner.workspace_database import WorkspaceDatabase


async def test_defaults_are_atomic_stable_and_workspace_local(tmp_path: Path) -> None:
    owners = [stores.WorkspaceRules(WorkspaceDatabase(tmp_path)) for _ in range(6)]
    results = await asyncio.gather(*(owner.list() for owner in owners))
    assert all(result == results[0] for result in results)
    defaults = results[0]
    assert {rule.tool_name for rule in defaults} == {"read_file", "list_files"}
    assert all(
        rule.properties == {"scope": "internal"} and rule.decision == "approve"
        for rule in defaults
    )
    assert await stores.WorkspaceRules(WorkspaceDatabase(tmp_path)).list() == defaults
    other = await stores.WorkspaceRules(WorkspaceDatabase(tmp_path / "other")).list()
    assert {rule.id for rule in other}.isdisjoint(rule.id for rule in defaults)


@pytest.mark.parametrize("first_operation", ["save", "import"])
async def test_first_write_includes_defaults(
    tmp_path: Path, first_operation: str
) -> None:
    owner = stores.WorkspaceRules(WorkspaceDatabase(tmp_path))
    value = policy.RuleInput(tool_name="custom", decision="deny")
    if first_operation == "save":
        await owner.save(value)
    else:
        await owner.import_rule(
            policy.AuthorizationRule(**value.model_dump(), id=uuid4())
        )
    assert {rule.tool_name for rule in await owner.list()} == {
        "read_file",
        "list_files",
        "custom",
    }


async def test_deleted_and_edited_defaults_are_not_restored(tmp_path: Path) -> None:
    owner = stores.WorkspaceRules(WorkspaceDatabase(tmp_path))
    defaults = {rule.tool_name: rule for rule in await owner.list()}
    await owner.delete(defaults["read_file"].id)
    denied = await owner.save(
        policy.RuleInput(
            tool_name="list_files", decision="deny", properties={"scope": "internal"}
        ),
        defaults["list_files"].id,
    )
    reopened = stores.WorkspaceRules(WorkspaceDatabase(tmp_path))
    assert await reopened.list() == [denied]
    await reopened.delete(denied.id)
    assert await stores.WorkspaceRules(WorkspaceDatabase(tmp_path)).list() == []


@pytest.mark.parametrize("empty", [False, True])
async def test_existing_documents_are_not_changed(tmp_path: Path, empty: bool) -> None:
    database = WorkspaceDatabase(tmp_path)
    document = stores.RuleDocument()
    if not empty:
        rule = policy.AuthorizationRule(
            id=uuid4(), tool_name="read_file", decision="deny"
        )
        document.rules[str(rule.id)] = rule
    original = document.model_dump_json()
    await database.save_document("tool.authorization.rules", original)
    assert await stores.WorkspaceRules(database).list() == list(document.rules.values())
    assert (
        await database.load_document("tool.authorization.rules", lambda: "invalid")
        == original
    )


@pytest.mark.parametrize(
    "name,scope,expected",
    [
        ("read_file", "internal", "approve"),
        ("list_files", "internal", "approve"),
        ("read_file", "external", "ask"),
        ("list_files", "external", "ask"),
        ("apply_patch", "internal", "ask"),
        ("exec", "internal", "ask"),
        ("read_file", None, "ask"),
    ],
)
async def test_defaults_only_allow_internal_reads(
    tmp_path: Path, name: str, scope: Optional[str], expected: Literal["approve", "ask"]
) -> None:
    rules = await stores.WorkspaceRules(WorkspaceDatabase(tmp_path)).list()
    call = policy.ToolCall(
        name=name,
        version=1,
        arguments={},
        definition_digest="digest",
        workflow_id="child",
        step_id="step",
        auth_context=policy.AuthContext(
            properties={} if scope is None else {"scope": scope}
        ),
    )
    result = await policy.ToolPolicy().evaluate(
        policy.ApprovalSettings(),
        call,
        [policy.ScopedRule(scope="workspace", rule=rule) for rule in rules],
    )
    assert result.action == expected
    assert (result.rule_id is not None) is (expected == "approve")
