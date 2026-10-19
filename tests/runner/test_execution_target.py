import asyncio
import json
from pathlib import Path

import pydantic
import pytest

from tests.fixtures.history import MemoryHistory
from tests.fixtures.probe import ExecutionProbe
from vectorops.runner import models
from vectorops.runner.environment.errors import EnvironmentError
from vectorops.runner.environment.target import LocalExecutionTarget
from vectorops.runner.runner.execution_state import ExecutionRecord
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner.runner.state_store import RunnerStateStore
from vectorops.runner.runner.workflow import WorkflowRunner


def configure(workspace: Path, target: Path) -> None:
    (workspace / "config.toml").write_text(
        f'[environment]\nkind = "local"\nbase_project_path = {json.dumps(str(target))}\n'
    )


@pytest.mark.parametrize("override", [False, True])
@pytest.mark.parametrize("persistent", [False, True])
async def test_target_is_inherited_without_changing_config(
    workspace: Path,
    execution_probe: ExecutionProbe,
    override: bool,
    persistent: bool,
) -> None:
    default = workspace / "default"
    default.mkdir()
    selected = workspace / "selected"
    selected.mkdir()
    link = workspace / "link"
    link.symlink_to(selected, target_is_directory=True)
    configure(workspace, default)
    runner = WorkflowRunner(workspace, None if persistent else MemoryHistory())
    assert await runner.run(
        "children:nested",
        base_project_path=str(link) if override else None,
    ) == [2, 4]
    expected = selected if override else default
    contexts = execution_probe.contexts
    assert len({ctx.workflow_id for ctx in contexts}) == 2
    for ctx in contexts:
        assert ctx.environment is not None
        assert ctx.environment.root == expected
        assert ctx.environment is contexts[0].environment
    project = await runner.project()
    assert project.config.environment is not None
    assert project.config.environment.base_project_path == str(default)
    if persistent:
        record = await runner.execution_state(str(runner.last_execution_id))
        assert record.target == LocalExecutionTarget(root=str(expected))


@pytest.mark.parametrize("override", [False, True])
async def test_resume_keeps_target_after_config_changes(
    workspace: Path, execution_probe: ExecutionProbe, override: bool
) -> None:
    original = workspace / "original"
    original.mkdir()
    changed = workspace / "changed"
    changed.mkdir()
    configure(workspace, changed if override else original)
    runner = WorkflowRunner(workspace)
    task = asyncio.create_task(
        runner.run(
            "activities:wait",
            base_project_path=str(original) if override else None,
        )
    )
    await asyncio.wait_for(execution_probe.started.get(), 5)
    execution_id = str(runner.last_execution_id)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    configure(workspace, changed)
    reopened = WorkflowRunner(workspace)
    resumed = asyncio.create_task(reopened.resume(execution_id))
    try:
        await asyncio.wait_for(execution_probe.started.get(), 5)
        ctx = execution_probe.contexts[-1]
        assert ctx.environment is not None and ctx.environment.root == original
    finally:
        execution_probe.gate.set()
        await asyncio.wait_for(resumed, 5)
    assert await reopened.resume(execution_id) == 0
    assert len(execution_probe.contexts) == 2
    with pytest.raises(ValueError, match="only be set for a new execution"):
        await reopened.run(
            "activities:wait", execution_id=execution_id, base_project_path=str(changed)
        )


async def test_concurrent_executions_have_independent_targets(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    roots = {name: workspace / name for name in ("first", "second")}
    for root in roots.values():
        root.mkdir()
    async with RunnerStateStore(workspace / "data") as store:
        runner = WorkflowRunner(workspace, state_store=store)
        tasks = [
            asyncio.create_task(
                runner.run(
                    "activities:wait", workflow_id=name, base_project_path=str(root)
                )
            )
            for name, root in roots.items()
        ]
        try:
            for _ in tasks:
                await asyncio.wait_for(execution_probe.started.get(), 5)
            for ctx in execution_probe.contexts:
                assert ctx.environment is not None
                assert ctx.environment.root == roots[ctx.workflow_id]
            assert (
                execution_probe.contexts[0].environment
                is not execution_probe.contexts[1].environment
            )
        finally:
            execution_probe.gate.set()
            await asyncio.gather(*tasks, return_exceptions=True)
        assert [task.result() for task in tasks] == [0, 0]
        assert await runner.run("activities:workflow") == 2
        ctx = execution_probe.contexts[-1]
        assert ctx.environment is not None and ctx.environment.root == workspace


async def test_legacy_execution_binds_default_on_resume(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    record = ExecutionRecord.model_validate(
        {"id": "legacy", "root_workflow_id": "root"}
    )
    assert record.target is None
    database = SQLiteHistory(workspace / "data/executions/legacy/history.sqlite3")
    await database.save_workflow(
        models.Workflow(id="root", definition_ref="activities:workflow")
    )
    await database.save_execution(record)
    runner = WorkflowRunner(workspace)
    assert await runner.resume("legacy") == 2
    assert (await database.get_execution()).target == LocalExecutionTarget(
        root=str(workspace)
    )


async def test_bound_target_cannot_be_replaced_or_cleared(tmp_path: Path) -> None:
    database = SQLiteHistory(tmp_path / "history.sqlite3")
    await database.save_workflow(
        models.Workflow(id="root", definition_ref="activities:workflow")
    )
    target = LocalExecutionTarget(root=str(tmp_path))
    record = ExecutionRecord(id="execution", root_workflow_id="root", target=target)
    await database.save_execution(record)
    for replacement in (None, LocalExecutionTarget(root=str(tmp_path / "other"))):
        with pytest.raises(ValueError, match="execution target cannot change"):
            await database.save_execution(
                record.model_copy(update={"target": replacement})
            )
        assert (await database.get_execution()).target == target


async def test_missing_persisted_target_does_not_fall_back(workspace: Path) -> None:
    target = workspace / "target"
    target.mkdir()
    runner = WorkflowRunner(workspace)
    await runner.run("activities:empty", base_project_path=str(target))
    execution_id = str(runner.last_execution_id)
    target.rmdir()
    with pytest.raises(EnvironmentError):
        await WorkflowRunner(workspace).resume(execution_id)
    record = await runner.execution_state(execution_id)
    assert record.target == LocalExecutionTarget(root=str(target))


@pytest.mark.parametrize("path", ["", "relative", "/bad\0path"])
async def test_invalid_override_is_rejected(workspace: Path, path: str) -> None:
    with pytest.raises(pydantic.ValidationError):
        await WorkflowRunner(workspace).run("activities:empty", base_project_path=path)


@pytest.mark.parametrize("missing", [False, True])
async def test_override_requires_existing_directory(
    workspace: Path, missing: bool
) -> None:
    target = workspace / "target"
    if not missing:
        target.touch()
    with pytest.raises(EnvironmentError):
        await WorkflowRunner(workspace).run(
            "activities:empty", base_project_path=str(target)
        )
