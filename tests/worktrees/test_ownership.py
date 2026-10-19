from contextlib import closing
from pathlib import Path
import sqlite3
from unittest.mock import patch
from uuid import uuid4

import pydantic
import pytest

from tests.runner.test_usage import seed
from vectorops.runner import models
from vectorops.runner.environment.target import LocalExecutionTarget
from vectorops.runner.runner import execution_state
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner.runner.workflow import WorkflowRunner


def test_reference_field_is_backward_compatible_and_validated() -> None:
    legacy = {"id": "execution", "root_workflow_id": "root"}
    assert (
        execution_state.ExecutionRecord.model_validate(legacy).referenced_targets == []
    )
    with pytest.raises(pydantic.ValidationError):
        execution_state.ExecutionRecord.model_validate(
            {**legacy, "referenced_targets": [{"root": "relative"}]}
        )


async def test_references_survive_checkpoint_writes_and_head_replacement(
    tmp_path: Path,
) -> None:
    history = SQLiteHistory(tmp_path / "history.sqlite3")
    await seed(history)
    previous = await history.get_execution()
    runtime = LocalExecutionTarget(root=str(tmp_path / "runtime"), worktree_id=uuid4())
    child = LocalExecutionTarget(root=str(tmp_path / "child"), worktree_id=uuid4())
    await history.reference_target(runtime)
    await history.reference_target(runtime)
    await history.save_workflow(
        models.Workflow(id="child", definition_ref="test:child", target=child)
    )
    await history.append_step(
        models.WorkflowStep(
            id="completion",
            workflow_id="child",
            operation=models.WorkflowCompleteOperation(),
            outcome=models.StepCompleted(),
        )
    )
    await history.save_execution(previous)
    assert (await history.get_execution()).referenced_targets == [runtime]
    assert set(await history.referenced_targets()) == {child, runtime}
    await history.save_execution(previous, replace_heads=True)
    reopened = SQLiteHistory(history.path)
    assert set(await reopened.referenced_targets()) == {child, runtime}
    assert (await reopened.get_step("completion")).outcome.kind == "completed"
    assert (await reopened.get_execution()).checkpoint.heads == {}


async def test_index_preserves_references_across_stale_publications_and_rebuilds(
    tmp_path: Path,
) -> None:
    runner = WorkflowRunner(tmp_path)
    project = await runner.project()
    history = SQLiteHistory(
        project.data_path / "executions/execution/history.sqlite3",
        summary_store=project.summaries,
    )
    await seed(history)
    await runner.initialize_usage()
    index = await runner.execution_index()
    old = await history.summary_snapshot()
    identifier = uuid4()
    target = LocalExecutionTarget(
        root=str(tmp_path / "worktree"), worktree_id=identifier
    )
    await history.reference_target(target)
    latest = await history.summary_snapshot()
    await project.summaries.record(old)
    assert await index.worktree_executions(identifier, target.root) == {"execution"}

    def clear(connection: sqlite3.Connection) -> None:
        with connection:
            connection.execute("DELETE FROM execution_targets")

    await project.database.run(clear)
    assert await index.worktree_executions(identifier, target.root) == set()
    await index.reconcile()
    assert await index.worktree_executions(identifier, target.root) == {"execution"}
    await project.summaries.remove(["execution"])
    await project.summaries.record(latest)
    assert await index.worktree_executions(identifier, target.root) == set()


async def test_projection_failure_is_repaired_before_ownership_lookup(
    tmp_path: Path,
) -> None:
    runner = WorkflowRunner(tmp_path)
    project = await runner.project()
    history = SQLiteHistory(
        project.data_path / "executions/execution/history.sqlite3",
        summary_store=project.summaries,
    )
    await seed(history)
    await runner.initialize_usage()
    index = await runner.execution_index()
    identifier = uuid4()
    target = LocalExecutionTarget(
        root=str(tmp_path / "worktree"), worktree_id=identifier
    )
    with patch.object(
        project.summaries, "record", side_effect=RuntimeError("unavailable")
    ):
        await history.reference_target(target)
    assert (await project.database.index_status())[1] is None
    assert await index.worktree_executions(identifier, target.root) == {"execution"}
    assert (await project.database.index_status())[1] is not None


async def test_legacy_workspace_rebuilds_index_from_all_workflow_targets(
    tmp_path: Path,
) -> None:
    runner = WorkflowRunner(tmp_path)
    project = await runner.project()
    history = SQLiteHistory(project.data_path / "executions/execution/history.sqlite3")
    await seed(history)
    identifier = uuid4()
    target = LocalExecutionTarget(
        root=str(tmp_path / "worktree"), worktree_id=identifier
    )
    await history.save_workflow(
        models.Workflow(id="historical", definition_ref="test:child", target=target)
    )
    await runner.initialize_usage()
    with closing(sqlite3.connect(project.database.path)) as connection, connection:
        connection.execute("DROP TABLE execution_targets")
        connection.execute("PRAGMA user_version = 3")
    restarted = WorkflowRunner(tmp_path)
    await restarted.initialize_usage()
    assert await (await restarted.execution_index()).worktree_executions(
        identifier, target.root
    ) == {"execution"}


async def test_scoped_index_seeks_uuid_and_raw_paths_without_scanning_archive(
    tmp_path: Path,
) -> None:
    runner = WorkflowRunner(tmp_path)
    project = await runner.project()
    await runner.initialize_usage()
    identifier = uuid4()
    path = str(tmp_path / "worktree")

    def populate(connection: sqlite3.Connection) -> None:
        with connection:
            connection.executemany(
                "INSERT INTO execution_targets VALUES (?, ?, ?)",
                [
                    (f"unrelated-{index}", str(tmp_path / f"other-{index}"), "")
                    for index in range(10000)
                ]
                + [
                    ("by-id", "/elsewhere", str(identifier)),
                    ("raw", path, ""),
                    ("nested", path + "/nested", ""),
                    ("prefix-neighbor", path + "-other", ""),
                ],
            )

    operations = 0

    def progress() -> int:
        nonlocal operations
        operations += 1
        return 0

    async with project.database:
        await project.database.run(populate)
        await project.database.run(
            lambda connection: connection.set_progress_handler(progress, 1)
        )
        try:
            assert await project.summaries.worktree_executions(identifier, path) == {
                "by-id",
                "raw",
                "nested",
            }
        finally:
            await project.database.run(
                lambda connection: connection.set_progress_handler(None, 0)
            )
    assert operations < 1000
