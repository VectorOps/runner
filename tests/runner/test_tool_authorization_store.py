from pathlib import Path
from unittest.mock import AsyncMock, Mock

from vectorops.runner import tool_authorization_store, tool_policy
from vectorops.runner.runner.project import Configuration, RunnerProject


async def test_workspace_settings_are_live_durable_and_isolated(tmp_path: Path) -> None:
    first_path = tmp_path / "first"
    second_path = tmp_path / "second"
    first_path.mkdir()
    second_path.mkdir()
    first = await RunnerProject.open(first_path)
    second = await RunnerProject.open(second_path)
    store = tool_authorization_store.AuthorizationStore(first.configuration)
    other = tool_authorization_store.AuthorizationStore(second.configuration)
    settings = tool_policy.ApprovalSettings(approve_all=True)
    await store.save_settings(settings, permanent=True)
    assert store.settings() == settings
    assert other.settings() == tool_policy.ApprovalSettings()
    reopened = await RunnerProject.open(first_path)
    assert (
        tool_authorization_store.AuthorizationStore(reopened.configuration).settings()
        == settings
    )
    await first.configuration.set("tools.approval.approve_all", True)
    assert store.settings().approve_all


async def test_workflow_store_delegates_without_caching() -> None:
    grants = AsyncMock(spec=tool_authorization_store.WorkflowGrants)
    grants.authorization_rules.side_effect = [[], []]
    store = tool_authorization_store.AuthorizationStore(
        Mock(spec=Configuration), grants
    )
    assert await store.rules("workflow", "test") == []
    assert await store.rules("workflow", "test") == []
    assert grants.authorization_rules.await_count == 2
    grants.authorization_rules.assert_awaited_with("workflow", "test")
