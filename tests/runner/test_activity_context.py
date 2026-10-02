import asyncio
from collections.abc import AsyncGenerator
from pathlib import Path
import typing

import pydantic
import pytest

from tests.fixtures.history import MemoryHistory
from tests.fixtures.probe import ExecutionProbe
from vectorops.runner import models
from vectorops.runner.api import composition as c
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.cached_history import CachedHistory
from vectorops.runner.runner.errors import ReplayMismatchError
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner.runner.workflow import WorkflowRunner


async def test_context_matches_durable_step_and_is_excluded_from_inputs(
    runner: WorkflowRunner, history: MemoryHistory, execution_probe: ExecutionProbe
) -> None:
    await runner.run("activities", input={"value": 3}, workflow_id="root")
    records = history.path(history.heads["root"])
    assert [ctx.step_id for ctx in execution_probe.contexts] == [
        step.id for step in records[:2]
    ]
    assert all(
        ctx.workflow_id == "root" and ctx.execution_id is None
        for ctx in execution_probe.contexts
    )
    assert records[0].operation.model_dump()["input"] == {"value": 3, "amount": 1}
    before = list(execution_probe.contexts)
    await runner.run("activities", continue_from_step_id=records[-1].id)
    assert execution_probe.contexts == before
    await runner.run("activities", continue_from_step_id=records[0].id)
    assert execution_probe.contexts[-1].step_id != records[1].id


@pytest.mark.parametrize(
    "callable_name,unfinished", [("workflow", "pending"), ("streaming", "streaming")]
)
async def test_persistent_recovery_reuses_same_context_identity(
    workspace: Path,
    execution_probe: ExecutionProbe,
    callable_name: str,
    unfinished: str,
) -> None:
    runner = WorkflowRunner(workspace)
    task = asyncio.create_task(
        runner.run("idempotency", callable_name, workflow_id="root")
    )
    await asyncio.wait_for(execution_probe.started.get(), 2)
    first = execution_probe.contexts[-1]
    assert first.execution_id == runner.last_execution_id
    assert first.workflow_id == "root"
    database = SQLiteHistory(
        workspace / "data/executions" / str(first.execution_id) / "history.sqlite3"
    )
    record = await database.get_step(first.step_id)
    assert record.outcome.kind == unfinished
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    execution_probe.gate.set()
    assert await WorkflowRunner(workspace).resume() == 1
    assert execution_probe.contexts[-1] == first
    assert len(execution_probe.contexts) == 2
    assert (await database.get_step(first.step_id)).outcome == models.StepCompleted(
        result=1
    )
    if callable_name == "workflow":
        assert execution_probe.effects == {first.step_id: 1}
    assert await WorkflowRunner(workspace).resume() == 1
    assert len(execution_probe.contexts) == 2


async def test_child_context_shares_execution_but_has_distinct_workflow_and_step_ids(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    runner = WorkflowRunner(workspace)
    assert await runner.run(
        "children", "parent", input={"values": [1, 2]}, workflow_id="root"
    ) == [2, 4]
    contexts = execution_probe.contexts
    assert len(contexts) == 2
    assert all(ctx.execution_id == runner.last_execution_id for ctx in contexts)
    assert all(ctx.workflow_id != "root" for ctx in contexts)
    assert len({ctx.workflow_id for ctx in contexts}) == 2
    assert len({ctx.step_id for ctx in contexts}) == 2


async def test_failed_pending_append_never_invokes_activity(
    runner: WorkflowRunner, history: MemoryHistory, execution_probe: ExecutionProbe
) -> None:
    history.fail_append = True
    with pytest.raises(OSError):
        await runner.run("activities")
    assert execution_probe.contexts == []
    assert execution_probe.calls == []


async def test_terminal_update_failure_leaves_recoverable_identity(
    runner: WorkflowRunner, history: MemoryHistory, execution_probe: ExecutionProbe
) -> None:
    history.fail_update = True
    with pytest.raises(OSError):
        await runner.run("activities", workflow_id="root")
    first = execution_probe.contexts[-1]
    assert history.steps[first.step_id].outcome.kind == "pending"
    history.fail_update = False
    assert await runner.run("activities", continue_from_step_id=first.step_id) == 2
    assert execution_probe.contexts[1] == first


async def test_pending_activity_cannot_have_replayed_descendants(
    runner: WorkflowRunner, history: MemoryHistory
) -> None:
    await runner.run("activities", workflow_id="root")
    path = history.path(history.heads["root"])
    history.replay_override = [
        path[0].model_copy(update={"outcome": models.StepPending()}),
        *path[1:],
    ]
    with pytest.raises(ReplayMismatchError, match="replay head"):
        await runner.run("activities", continue_from_step_id=path[-1].id)


def test_activity_decorators_require_first_positional_context() -> None:
    async def missing() -> int:
        return 1

    async def keyword_only(*, ctx: ActivityExecutionContext) -> int:
        return 1

    async def missing_stream() -> AsyncGenerator[c.ActivityFinal[int], None]:
        yield c.final(1)

    with pytest.raises(TypeError, match="context parameter"):
        c.activity()(typing.cast(typing.Any, missing))
    with pytest.raises(TypeError, match="context parameter"):
        c.activity()(typing.cast(typing.Any, keyword_only))
    with pytest.raises(TypeError, match="context parameter"):
        c.streamable_activity()(typing.cast(typing.Any, missing_stream))


def test_pending_outcome_roundtrip_and_activity_only() -> None:
    step = models.WorkflowStep(
        id="s",
        workflow_id="w",
        operation=models.StepOperation(kind="activity", name="work"),
        outcome=models.StepPending(),
    )
    assert models.WorkflowStep.model_validate_json(step.model_dump_json()) == step
    with pytest.raises(pydantic.ValidationError):
        models.WorkflowStep(
            id="s",
            workflow_id="w",
            operation=models.WorkflowCompleteOperation(),
            outcome=models.StepPending(),
        )


async def test_pending_identity_and_terminal_result_write_through_cache(
    tmp_path: Path,
) -> None:
    database = SQLiteHistory(tmp_path / "history.sqlite3")
    record = models.WorkflowStep(
        id="step",
        workflow_id="root",
        operation=models.StepOperation(kind="activity", name="work"),
        outcome=models.StepPending(),
    )
    async with CachedHistory(database, flush_interval=60) as cache:
        await cache.save_workflow(
            models.Workflow(id="root", definition_ref="file:workflow")
        )
        await cache.append_step(record)
        assert (
            await SQLiteHistory(database.path).get_step(record.id)
        ).outcome.kind == "pending"
        completed = record.model_copy(
            update={"outcome": models.StepCompleted(result=1)}
        )
        await cache.update_step(completed)
        assert await SQLiteHistory(database.path).get_step(record.id) == completed
        with pytest.raises(ValueError, match="terminal"):
            await cache.update_step(record)


async def test_pending_identity_cannot_have_descendants(tmp_path: Path) -> None:
    database = SQLiteHistory(tmp_path / "history.sqlite3")
    await database.save_workflow(
        models.Workflow(id="root", definition_ref="file:workflow")
    )
    record = models.WorkflowStep(
        id="pending",
        workflow_id="root",
        operation=models.StepOperation(kind="activity", name="work"),
        outcome=models.StepPending(),
    )
    await database.append_step(record)
    with pytest.raises(ValueError, match="unfinished"):
        await database.append_step(
            record.model_copy(
                update={
                    "id": "descendant",
                    "previous_step_id": "pending",
                    "outcome": models.StepCompleted(result=1),
                }
            )
        )
