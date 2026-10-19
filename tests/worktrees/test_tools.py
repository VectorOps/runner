import asyncio
from pathlib import Path
import sys
from unittest.mock import AsyncMock, patch

import connect
import pydantic
import pytest

from tests.fixtures import worktree_cases as cases
from vectorops.runner import tool_policy
from vectorops.runner.api import tools
from vectorops.runner.environment.configuration import LocalEnvironmentConfig
from vectorops.runner.environment.target import LocalExecutionTarget
from vectorops.runner.environment.processes import models as processes
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.project import RunnerProject
from vectorops.runner.runner.workflow import WorkflowRunner
from vectorops.runner.workflows.tools import default_collections
from vectorops.runner.worktrees import models as m
from vectorops.runner.worktrees.manager import Manager
from vectorops.runner.worktrees.runtime import ExecutionTargets


def payload(response: tools.ToolResponse) -> dict[str, pydantic.JsonValue]:
    block = response.content[0]
    assert isinstance(block, connect.TextBlock)
    return pydantic.TypeAdapter(dict[str, pydantic.JsonValue]).validate_json(block.text)


async def test_opt_in_tools_provenance_and_reuse(
    manager: Manager, repository: Path
) -> None:
    names = {"worktree_create", "worktree_list", "worktree_delete"}
    for collection in default_collections([]).values():
        assert not names.intersection(collection.tools)
    targets = ExecutionTargets(
        manager, LocalEnvironmentConfig(base_project_path=str(repository)), "execution"
    )
    context = ActivityExecutionContext("execution", "parent", "create", targets=targets)
    created = m.Record.model_validate(
        payload(
            await tools.worktree_create_tool().execute(
                context, {"name": "Investigate parser"}
            )
        )
    )
    assert created.origin == m.Origin(
        kind="workflow",
        execution_id="execution",
        workflow_id="parent",
        step_id="create",
    )
    repeated = m.Record.model_validate(
        payload(
            await tools.worktree_create_tool().execute(
                context, {"name": "Investigate parser"}
            )
        )
    )
    assert repeated.id == created.id
    listing = m.Page.model_validate(
        payload(await tools.worktree_list_tool().execute(context, {}))
    )
    assert [item.id for item in listing.worktrees] == [created.id]
    async with manager.use(created.id, "unrelated-execution"):
        pass
    deleted = m.Record.model_validate(
        payload(
            await tools.worktree_delete_tool().execute(
                context, {"worktree_id": str(created.id)}
            )
        )
    )
    assert deleted.status == "deleted"


async def test_exec_override_routes_output_and_protects_process(
    manager: Manager, repository: Path
) -> None:
    record = await manager.create(m.Create(name="Command"), key="command")
    targets = ExecutionTargets(
        manager, LocalEnvironmentConfig(base_project_path=str(repository)), "execution"
    )
    environment = await targets.open(
        await LocalExecutionTarget.resolve(str(repository))
    )
    context = ActivityExecutionContext(
        "execution", "parent", "exec", targets=targets, environment=environment
    )
    try:
        result = payload(
            await tools.exec_tool().execute(
                context,
                {
                    "worktree_id": str(record.id),
                    "command": {
                        "argv": [
                            sys.executable,
                            "-u",
                            "-c",
                            "import os, time; print(os.getcwd()); time.sleep(1)",
                        ]
                    },
                    "yield_timeout_ms": 0,
                },
            )
        )
        process_id = result["process_id"]
        assert isinstance(process_id, str)
        with pytest.raises(m.WorktreeError, match="in use"):
            await manager.delete(m.Delete(worktree_id=record.id, force=True))
        async with asyncio.timeout(10):
            while await manager.store.uses(record.id):
                await asyncio.sleep(0.05)
        assert (
            await manager.delete(m.Delete(worktree_id=record.id))
        ).status == "deleted"
        output = payload(
            await tools.poll_process_tool().execute(context, {"process_id": process_id})
        )
        assert record.path in str(output["chunks"])
        assert output["returncode"] == 0
        assert (
            context.environment is not None and context.environment.root == repository
        )
    finally:
        await targets.close()


async def test_unconfirmed_process_cleanup_retains_abandoned_lease(
    manager: Manager, repository: Path
) -> None:
    record = await manager.create(m.Create(name="Cleanup"), key="cleanup")
    targets = ExecutionTargets(
        manager, LocalEnvironmentConfig(base_project_path=str(repository)), "execution"
    )
    environment = await targets.open(await manager.resolve(record.id))
    with patch.object(
        environment.processes,
        "close",
        AsyncMock(
            return_value=[
                processes.KillResult(
                    process_id="unknown", termination_confirmed=False, scope="group"
                )
            ]
        ),
    ):
        with pytest.raises(ExceptionGroup):
            await targets.close()
    with pytest.raises(m.WorktreeError, match="abandoned"):
        await manager.delete(m.Delete(worktree_id=record.id))
    assert (
        await manager.delete(m.Delete(worktree_id=record.id, force=True))
    ).status == "deleted"


async def test_finished_exec_releases_lease_before_return(
    manager: Manager, repository: Path
) -> None:
    record = await manager.create(m.Create(name="Short command"), key="short")
    targets = ExecutionTargets(
        manager, LocalEnvironmentConfig(base_project_path=str(repository)), "execution"
    )
    try:
        result = await targets.exec(
            processes.ExecRequest(
                command=processes.ArgvCommand(argv=[sys.executable, "-c", "pass"]),
                yield_timeout_ms=1000,
            ),
            record.id,
        )
        assert result.state == "exited"
        assert await manager.store.uses(record.id) == []
        assert (
            await manager.delete(m.Delete(worktree_id=record.id))
        ).status == "deleted"
    finally:
        await targets.close()


@pytest.mark.parametrize("factory", ["run_agent", "subworkflow"])
@pytest.mark.parametrize("approved", [False, True])
async def test_subworkflow_tool_targeting_approval_and_replay(
    worktree_project: RunnerProject,
    worktree_probe: cases.Probe,
    factory: str,
    approved: bool,
) -> None:
    manager = worktree_project.worktrees
    record = await manager.create(m.Create(name="Tool child"), key="tool-child")
    runner = WorkflowRunner(worktree_project.root, project=worktree_project)
    with (
        patch.object(
            tool_policy.ToolPolicy,
            "check",
            return_value=tool_policy.Decision(
                action="approve" if approved else "deny", reason="test decision"
            ),
        ),
        patch.object(manager, "resolve", wraps=manager.resolve) as resolved,
    ):
        result = connect.ToolResultMessage.model_validate(
            await runner.run(
                cases.REFERENCE + "tool_parent",
                input={"worktree_id": str(record.id), "factory": factory},
            )
        )
    identifier = str(runner.last_execution_id)
    assert result.is_error is not approved
    assert (await runner.inspect_execution(identifier)).stats.workflows == (
        2 if approved else 1
    )
    if approved:
        context = worktree_probe.contexts["tool-child"]
        assert (
            context.environment is not None
            and str(context.environment.root) == record.path
        )
        block = result.content[0]
        assert isinstance(block, connect.TextBlock)
        assert pydantic.TypeAdapter(str).validate_json(block.text) == record.path
    else:
        assert worktree_probe.contexts == {}
        assert resolved.await_count == 0
    await manager.delete(m.Delete(worktree_id=record.id))
    worktree_probe.events.clear()
    with (
        patch.object(
            tool_policy.ToolPolicy,
            "check",
            side_effect=AssertionError("completed tool must not reauthorize"),
        ),
        patch.object(
            manager,
            "resolve",
            side_effect=AssertionError("completed tool must not reattach"),
        ),
    ):
        assert await runner.resume(identifier) == result.model_dump(mode="json")
    assert worktree_probe.events == []


async def test_stdin_and_kill_route_across_default_and_worktree_processes(
    manager: Manager, repository: Path
) -> None:
    records = [
        await manager.create(m.Create(name=name), key=name)
        for name in ("first", "second")
    ]
    targets = ExecutionTargets(
        manager, LocalEnvironmentConfig(base_project_path=str(repository)), "execution"
    )
    environment = await targets.open(
        await LocalExecutionTarget.resolve(str(repository))
    )
    context = ActivityExecutionContext(
        "execution", "workflow", "tools", targets=targets, environment=environment
    )
    command: dict[str, pydantic.JsonValue] = {
        "argv": [
            sys.executable,
            "-u",
            "-c",
            "import os, sys, time; print(os.getcwd()); print('ready'); print('echo:' + sys.stdin.readline().strip()); time.sleep(60)",
        ]
    }
    cursors: dict[str, int] = {}
    output: dict[str, str] = {}

    def collect(result: dict[str, pydantic.JsonValue]) -> str:
        identifier, cursor, chunks = (
            result["process_id"],
            result["cursor"],
            result["chunks"],
        )
        assert (
            isinstance(identifier, str)
            and isinstance(cursor, int)
            and isinstance(chunks, list)
        )
        cursors[identifier] = cursor
        output.setdefault(identifier, "")
        for chunk in chunks:
            assert (
                isinstance(chunk, dict)
                and isinstance(chunk["data"], str)
                and chunk["encoding"] == "utf-8"
            )
            output[identifier] += chunk["data"]
        return identifier

    async def until(identifier: str, text: str) -> None:
        async with asyncio.timeout(5):
            while text not in output[identifier]:
                collect(
                    payload(
                        await tools.poll_process_tool().execute(
                            context,
                            {
                                "process_id": identifier,
                                "cursor": cursors[identifier],
                                "wait_timeout_ms": 100,
                            },
                        )
                    )
                )

    try:
        identifiers = []
        for worktree_id in (None, records[0].id, records[1].id):
            arguments: dict[str, pydantic.JsonValue] = {
                "command": command,
                "keep_stdin_open": True,
                "yield_timeout_ms": 0,
            }
            if worktree_id is not None:
                arguments["worktree_id"] = str(worktree_id)
            identifier = collect(
                payload(await tools.exec_tool().execute(context, arguments))
            )
            identifiers.append(identifier)
            await until(identifier, "ready")
        assert str(repository) in output[identifiers[0]]
        assert records[0].path in output[identifiers[1]]
        assert records[1].path in output[identifiers[2]]
        await tools.write_stdin_tool().execute(
            context, {"process_id": identifiers[1], "data": "first\n"}
        )
        await until(identifiers[1], "echo:first")
        killed = payload(
            await tools.kill_process_tool().execute(
                context, {"process_id": identifiers[1]}
            )
        )
        assert killed["termination_confirmed"] is True
        for identifier, text in ((identifiers[0], "root"), (identifiers[2], "second")):
            await tools.write_stdin_tool().execute(
                context, {"process_id": identifier, "data": text + "\n"}
            )
            await until(identifier, "echo:" + text)
            killed = payload(
                await tools.kill_process_tool().execute(
                    context, {"process_id": identifier}
                )
            )
            assert killed["termination_confirmed"] is True
        async with asyncio.timeout(5):
            while any([await manager.store.uses(record.id) for record in records]):
                await asyncio.sleep(0.01)
    finally:
        await targets.close()
