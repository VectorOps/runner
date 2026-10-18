from pathlib import Path

import pytest
import pydantic

from tests.control.test_mailbox import pending, completed
from tests.control.test_tool_grants import WORKFLOW, answer
from vectorops.runner.control import client, protocol
from vectorops.runner.runner import history_query


async def test_default_read_rules_are_visible_and_deletable(tmp_path: Path) -> None:
    async with client.embedded(tmp_path) as host:
        listing = await host.request("tools.rules.list", {})
        assert isinstance(listing, dict) and isinstance(listing["rules"], list)
        names = set()
        for rule in listing["rules"]:
            assert isinstance(rule, dict)
            names.add(str(rule["tool_name"]))
            assert rule["properties"] == {"scope": "internal"}
            assert rule["decision"] == "approve"
            await host.request("tools.rules.delete", {"rule_id": rule["id"]})
        assert names == {"read_file", "list_files"}
    async with client.embedded(tmp_path) as host:
        assert await host.request("tools.rules.list", {}) == {"rules": []}


async def test_workspace_rule_from_response_survives_restart_and_has_crud(
    tmp_path: Path,
) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/flow.py").write_text(WORKFLOW)
    async with client.embedded(tmp_path) as host:
        run = await host.start(
            protocol.Start(reference="flow:child", idempotency_key="start")
        )
        (request,) = await pending(host, run.execution_id)
        await host.answer_input(answer(request, scope="workspace"))
        await completed(host, run.execution_id)
        listing = await host.request("tools.rules.list", {"tool_name": "test"})
        assert isinstance(listing, dict) and isinstance(listing["rules"], list)
        (saved,) = listing["rules"]
        assert isinstance(saved, dict) and isinstance(saved["id"], str)
        identifier = saved["id"]
        page = await host.history(
            history_query.HistoryQuery(execution_id=run.execution_id)
        )
        step = next(
            item.step for item in page.items if item.step.operation.kind == "llm.tool"
        )
        assert str(step.authorization_rule_id) == identifier
    async with client.embedded(tmp_path) as host:
        assert await host.request("tools.rules.get", {"rule_id": identifier}) == saved
        run = await host.start(
            protocol.Start(reference="flow:child", idempotency_key="fresh")
        )
        await completed(host, run.execution_id)
        updated = await host.request(
            "tools.rules.update",
            {
                "rule_id": identifier,
                "rule": {"tool_name": saved["tool_name"], "decision": "deny"},
            },
        )
        assert isinstance(updated, dict) and updated["decision"] == "deny"
        await host.request("tools.rules.delete", {"rule_id": identifier})
        assert await host.request("tools.rules.list", {"tool_name": "test"}) == {
            "rules": []
        }
        with pytest.raises(protocol.RemoteError):
            await host.request("tools.rules.get", {"rule_id": identifier})


async def test_rule_crud_validates_scope_and_decisions(tmp_path: Path) -> None:
    async with client.embedded(tmp_path) as host:
        cases: list[dict[str, pydantic.JsonValue]] = [
            {"scope": "workflow", "rule": {"tool_name": "read", "decision": "approve"}},
            {"rule": {"tool_name": "read", "decision": "ask"}},
            {
                "workflow_id": "other",
                "rule": {"tool_name": "read", "decision": "approve"},
            },
        ]
        for params in cases:
            with pytest.raises(protocol.RemoteError):
                await host.request("tools.rules.create", params)


async def test_workflow_crud_is_scoped_to_execution_and_workflow(
    tmp_path: Path,
) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/flow.py").write_text(WORKFLOW)
    async with client.embedded(tmp_path) as host:
        run = await host.start(
            protocol.Start(reference="flow:child", idempotency_key="start")
        )
        (request,) = await pending(host, run.execution_id)
        scope: dict[str, pydantic.JsonValue] = {
            "scope": "workflow",
            "execution_id": run.execution_id,
            "workflow_id": request.workflow_id,
        }
        saved = await host.request(
            "tools.rules.create",
            {
                **scope,
                "rule": {
                    "tool_name": "test",
                    "decision": "deny",
                    "properties": {"scope": "internal"},
                },
            },
        )
        assert isinstance(saved, dict)
        assert await host.request("tools.rules.list", scope) == {"rules": [saved]}
        assert await host.request("tools.rules.list", {"tool_name": "test"}) == {
            "rules": []
        }
        reference = {**scope, "rule_id": saved["id"]}
        with pytest.raises(protocol.RemoteError):
            await host.request("tools.rules.get", {**reference, "workflow_id": "other"})
        updated = await host.request(
            "tools.rules.update",
            {**reference, "rule": {"tool_name": "test", "decision": "approve"}},
        )
        assert isinstance(updated, dict) and updated["id"] == saved["id"]
        await host.request("tools.rules.delete", reference)
        assert await host.request("tools.rules.list", scope) == {"rules": []}
        await host.answer_input(answer(request))
        await completed(host, run.execution_id)
