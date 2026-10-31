import asyncio
from pathlib import Path
import sqlite3
from unittest.mock import AsyncMock, patch

import pytest

from tests.runner.test_usage import generation, seed, spawn
from vectorops.runner import models
from vectorops.runner.control.updates import SnapshotPublisher
from vectorops.runner.environment.target import LocalExecutionTarget
from vectorops.runner.runner import events, mailbox, summary_history
from vectorops.runner.runner import inspection
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner.runner.summary_store import SummaryStore
from vectorops.runner.runner.usage_history import UsageHistory
from vectorops.runner.runner.workspace_database import WorkspaceDatabase


async def test_usage_only_publication_skips_execution_and_unchanged_ancestors(
    tmp_path: Path,
) -> None:
    store = SummaryStore(WorkspaceDatabase(tmp_path / "workspace"))
    published: list[events.Publication] = []
    database = SQLiteHistory(
        tmp_path / "history.sqlite3",
        summary_store=store,
        publication_sink=published.append,
    )
    await seed(database)
    await spawn(database, "root", "child")
    await database.append_step(generation("call", "child", streaming=True))
    published.clear()
    with patch.object(
        UsageHistory, "save_usage", wraps=UsageHistory.save_usage
    ) as save:
        await database.update_step(
            generation("call", "child", streaming=True, outgoing=50)
        )
        assert [call.args[1] for call in save.call_args_list] == ["child"]
    assert published == [events.Publication(("child",), summary=True)]
    execution = AsyncMock(side_effect=AssertionError("usage-only execution read"))
    summary = AsyncMock(return_value=await store.get("execution"))
    workflow = AsyncMock(return_value=await database.get_workflow("child"))
    publisher = SnapshotPublisher(
        summary, workflow, lambda event: None, execution=execution
    )
    publisher.publish("execution", published[0])
    await publisher.close()
    execution.assert_not_awaited()
    summary.assert_awaited_once()
    workflow.assert_awaited_once_with("execution", "child")
    published.clear()
    with patch.object(
        UsageHistory, "save_usage", wraps=UsageHistory.save_usage
    ) as save:
        await database.update_step(
            generation("call", "child", streaming=True, outgoing=60, cost="0.02")
        )
        assert {call.args[1] for call in save.call_args_list} == {"child", "root"}
    assert published[0].workflows == ("child", "root")
    assert not published[0].execution
    await database.checkpoint_execution("execution")
    assert published[-1] == events.Publication(execution=True)


async def test_publication_merges_flags_arriving_during_reads() -> None:
    entered, release = asyncio.Event(), asyncio.Event()
    execution = AsyncMock(side_effect=KeyError("not yet created"))

    async def summary(identifier: str) -> inspection.ExecutionSummary:
        entered.set()
        await release.wait()
        raise KeyError(identifier)

    publisher = SnapshotPublisher(
        summary, AsyncMock(), lambda event: None, execution=execution
    )
    publisher.publish("execution", events.Publication(summary=True))
    await entered.wait()
    publisher.publish("execution", events.Publication(execution=True))
    release.set()
    await publisher.close()
    execution.assert_awaited_once_with("execution")


async def test_scalar_mailbox_operations_never_materialize_execution_heads(
    tmp_path: Path,
) -> None:
    database = SQLiteHistory(tmp_path / "history.sqlite3")
    await seed(database)
    for identifier in ("first", "second"):
        await database.append_step(
            models.WorkflowStep(
                id=identifier,
                workflow_id="root",
                operation=models.StepOperation(kind=mailbox.KIND, name="input"),
                outcome=models.StepPending(),
                deferred_delivery=True,
            )
        )
    with patch.object(
        SQLiteHistory, "_execution", side_effect=AssertionError("full execution read")
    ):
        await database.register_input("first", mailbox.InputSpec(channel="approval"))
        await database.register_input("second", mailbox.InputSpec(channel="approval"))
        assert (
            len(
                (
                    await database.list_inputs(
                        mailbox.RequestQuery(execution_id="execution")
                    )
                ).requests
            )
            == 2
        )
        assert await database.set_paused({"first", "second"})
        await database.answer_input(
            mailbox.Answer(
                execution_id="execution",
                request_id="first",
                response="yes",
                idempotency_key="answer",
            )
        )
        assert await database.set_paused({"second"})
        assert await database.wake_execution()
        await database.cancel_inputs("second")
        await database.stop_execution()
    assert (await database.get_execution()).status == "cancelled"


async def test_targets_reused_and_projection_reads_skipped_until_membership_changes(
    tmp_path: Path,
) -> None:
    store = SummaryStore(WorkspaceDatabase(tmp_path / "workspace"))
    database = SQLiteHistory(tmp_path / "history.sqlite3", summary_store=store)
    await seed(database)
    await database.reference_target(LocalExecutionTarget(root=str(tmp_path / "one")))
    await database.append_step(generation("call", streaming=True))
    assert database._summary_runtime is not None
    before = database._summary_runtime.working.snapshot
    assert before is not None
    with (
        patch.object(
            summary_history,
            "sorted",
            side_effect=AssertionError("target sorting"),
            create=True,
        ),
        patch.object(
            SummaryStore,
            "_targets",
            side_effect=AssertionError("target projection read"),
        ),
    ):
        await database.update_step(generation("call", streaming=True, outgoing=50))
        await database.checkpoint_execution("execution")
    current = database._summary_runtime.working.snapshot
    assert current is not None and current.targets is before.targets
    with patch.object(SummaryStore, "_targets", wraps=SummaryStore._targets) as targets:
        await database.reference_target(
            LocalExecutionTarget(root=str(tmp_path / "two"))
        )
        assert targets.call_count == 1
    assert (
        database._summary_runtime.working.snapshot == await database.summary_snapshot()
    )


async def test_target_projection_repair_after_missing_row_and_external_writer(
    tmp_path: Path,
) -> None:
    store = SummaryStore(WorkspaceDatabase(tmp_path / "workspace"))
    database = SQLiteHistory(tmp_path / "history.sqlite3", summary_store=store)
    await seed(database)
    target = LocalExecutionTarget(root=str(tmp_path / "one"))
    await database.reference_target(target)
    await database.append_step(generation("call", streaming=True))

    def remove_projection(connection: sqlite3.Connection) -> None:
        with connection:
            connection.execute("DELETE FROM execution_summaries")
            connection.execute("DELETE FROM execution_targets")

    await store.database.run(remove_projection)
    with patch.object(SummaryStore, "_targets", wraps=SummaryStore._targets) as targets:
        await database.update_step(generation("call", streaming=True, outgoing=50))
        assert targets.call_count == 1
    external = SQLiteHistory(database.path)
    await external.reference_target(
        LocalExecutionTarget(root=str(tmp_path / "external"))
    )
    with patch.object(SummaryStore, "_targets", wraps=SummaryStore._targets) as targets:
        await database.update_step(generation("call", streaming=True, outgoing=60))
        assert targets.call_count == 1
    roots = await store.database.run(
        lambda connection: list(
            connection.execute("SELECT root FROM execution_targets ORDER BY root")
        )
    )
    assert roots == [(str(tmp_path / "external"),), (target.root,)]
    with patch.object(
        store, "record", new=AsyncMock(side_effect=RuntimeError("offline"))
    ):
        await database.reference_target(
            LocalExecutionTarget(root=str(tmp_path / "retry"))
        )
    assert database._summary_runtime is not None
    await database._summary_runtime.flush()
    count = await store.database.run(
        lambda connection: connection.execute(
            "SELECT count(*) FROM execution_targets"
        ).fetchone()[0]
    )
    assert count == 3


async def test_new_references_publish_before_worktree_ownership_checks(
    tmp_path: Path,
) -> None:
    store = SummaryStore(WorkspaceDatabase(tmp_path / "workspace"))
    async with SQLiteHistory(
        tmp_path / "history.sqlite3", summary_store=store
    ) as database:
        await seed(database)
        assert database._summary_runtime is not None
        database._summary_runtime.interval = 60
        target = LocalExecutionTarget(root=str(tmp_path / "child"))
        await database.save_workflow(
            models.Workflow(id="child", definition_ref="test", target=target)
        )
        rows = await store.database.run(
            lambda connection: list(
                connection.execute("SELECT root FROM execution_targets")
            )
        )
        assert rows == [(target.root,)]
