import asyncio
from pathlib import Path
import sqlite3

import pydantic
import pytest

from tests.runner.test_session import unused_child
from vectorops.runner import models
from vectorops.runner.runner import branching, context, execution_state
from vectorops.runner.runner.cached_history import CachedHistory
from vectorops.runner.runner.checkpoint import ExecutionCheckpointer
from vectorops.runner.runner.errors import ReplayMismatchError
from vectorops.runner.runner.session import ExecutionSession
from vectorops.runner.runner.sqlite_history import SQLiteHistory


@pytest.mark.parametrize("deferred", [False, True])
async def test_replay_failure_completion_preserves_pending_retry_boundary(
    tmp_path: Path, deferred: bool
) -> None:
    database = SQLiteHistory(tmp_path / "history.sqlite3")
    workflow = models.Workflow(id="root", definition_ref="example:workflow")
    await database.save_workflow(workflow)
    await database.save_execution(
        execution_state.ExecutionRecord(id="execution", root_workflow_id="root")
    )
    first = models.WorkflowStep(
        id="first",
        workflow_id="root",
        operation=models.StepOperation(kind="activity", name="first"),
        outcome=models.StepCompleted(result=1),
    )
    pending = models.WorkflowStep(
        id="pending",
        workflow_id="root",
        previous_step_id=first.id,
        operation=models.StepOperation(kind="activity", name="pending"),
        outcome=models.StepPending(),
        deferred_delivery=deferred,
    )
    await database.append_step(first)
    await database.append_step(pending)
    async with CachedHistory(database) as history:
        reporter = ExecutionCheckpointer(
            database, history, await database.get_execution()
        )
        session = ExecutionSession(
            history,
            workflow,
            unused_child,
            continue_from_step_id=pending.id,
            child_heads={},
            reporter=reporter,
        )

        async def forbidden(ctx: object) -> pydantic.JsonValue:
            raise AssertionError("replay must not execute")

        async def mismatch() -> pydantic.JsonValue:
            assert isinstance(first.operation, models.StepOperation)
            await session.submit(context.ActivityRequest(first.operation, forbidden))
            await reporter.persist()
            assert reporter.heads == {"root": pending.id}
            await session.submit(
                context.ActivityRequest(
                    models.StepOperation(kind="activity", name="changed"), forbidden
                )
            )
            return None

        with pytest.raises(ReplayMismatchError):
            await session.run(mismatch)
        await reporter.persist("failed")
    record = await database.get_execution()
    assert record.completion_step_id is not None
    completion = await database.get_step(record.completion_step_id)
    assert completion.previous_step_id == pending.id
    assert completion.resume_step_id == pending.id
    assert completion.outcome.kind == "failed"
    assert completion.outcome.error.type == "ReplayMismatchError"
    assert await database.get_step(first.id) == first
    assert await database.get_step(pending.id) == pending
    with sqlite3.connect(database.path) as connection:
        raw = execution_state.ExecutionRecord.model_validate_json(
            connection.execute("SELECT payload FROM execution").fetchone()[0]
        )
    assert raw.checkpoint.heads == record.checkpoint.heads
    assert raw.completion_step_id == completion.id
    await database.accept_branch(
        branching.Branch(
            execution_id=record.id,
            step_id=completion.id,
            idempotency_key="retry",
            action=branching.Restart(),
        )
    )
    operation = await database.apply_branch()
    assert operation is not None and operation.status == "applied"
    resumed = ExecutionSession(
        database,
        workflow,
        unused_child,
        continue_from_step_id=pending.id,
        child_heads={},
    )
    calls = 0

    async def execute(ctx: object) -> pydantic.JsonValue:
        nonlocal calls
        calls += 1
        return 2

    async def invoke() -> pydantic.JsonValue:
        assert isinstance(first.operation, models.StepOperation)
        assert isinstance(pending.operation, models.StepOperation)
        await resumed.submit(context.ActivityRequest(first.operation, forbidden))
        response = await resumed.submit(
            context.ActivityRequest(pending.operation, execute)
        )
        assert response.kind == "activity" and response.outcome.kind == "completed"
        return response.outcome.result

    result = await asyncio.wait_for(resumed.run(invoke), 2)
    assert result.outcome == models.StepCompleted(result=2)
    assert calls == 1
    assert await database.get_step(completion.id) == completion


@pytest.mark.parametrize("kind", ["llm.generate", "llm.tool", "llm.compact"])
def test_llm_replay_metadata_is_informational(kind: str) -> None:
    recorded = models.StepOperation(
        kind=kind,
        name="invoke",
        version=99,
        input={"request_digest": "old", "tool_digest": "old", "tool_version": 1},
    )
    current = models.StepOperation(
        kind=kind,
        name="invoke",
        version=1,
        input={"request_digest": "new", "tool_digest": "new", "tool_version": 9},
    )
    assert current.matches_replay(recorded)
    assert not current.matches_replay(recorded.model_copy(update={"name": "other"}))


def test_failure_resume_boundary_validation() -> None:
    with pytest.raises(pydantic.ValidationError, match="resume boundary"):
        models.WorkflowStep(
            id="completion",
            workflow_id="root",
            previous_step_id="previous",
            operation=models.WorkflowCompleteOperation(),
            outcome=models.StepCompleted(),
            resume_step_id="previous",
        )
