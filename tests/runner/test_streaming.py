import asyncio
from collections.abc import AsyncGenerator, Callable
import typing

import pydantic
import pytest

from tests.fixtures.history import MemoryHistory
from tests.fixtures.probe import ExecutionProbe
from vectorops.runner import models
from vectorops.runner.api import composition as c
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.errors import ActivityError, ReplayMismatchError
from vectorops.runner.runner.workflow import WorkflowRunner


async def test_stream_replaces_one_record_with_latest_state(
    runner: WorkflowRunner, history: MemoryHistory, execution_probe: ExecutionProbe
) -> None:
    task = asyncio.create_task(
        runner.run("streaming", input={"mode": "blocked"}, workflow_id="root")
    )
    assert await asyncio.wait_for(execution_probe.started.get(), 2) == 0
    head = history.heads["root"]
    snapshot = history.steps[head]
    assert snapshot.outcome == models.StepStreaming(state={"index": 0})
    assert (
        models.WorkflowStep.model_validate_json(snapshot.model_dump_json()) == snapshot
    )
    assert len(history.steps) == 1
    execution_probe.gate.set()
    assert await asyncio.wait_for(task, 2) == {"count": 2}
    assert history.steps[head].outcome == models.StepCompleted(result={"count": 2})
    assert len(history.steps) == 2
    assert history.appended[0] == head
    assert history.updated == 4
    assert execution_probe.cancelled == 1


@pytest.mark.parametrize("callable_name", ["workflow", "parent"])
async def test_completed_stream_replay_never_reinvokes_or_updates(
    runner: WorkflowRunner,
    history: MemoryHistory,
    execution_probe: ExecutionProbe,
    callable_name: str,
) -> None:
    assert await runner.run("streaming", callable_name, workflow_id="root") == {
        "count": 2
    }
    appended = list(history.appended)
    updated = history.updated
    assert await runner.run(
        "streaming", callable_name, continue_from_step_id=history.heads["root"]
    ) == {"count": 2}
    assert history.appended == appended
    assert history.updated == updated
    assert execution_probe.calls == [2]


@pytest.mark.parametrize(
    "mode,message",
    [
        ("missing_final", "without final"),
        ("duplicate_final", "after final"),
        ("after_final", "after final"),
        ("failure", "stream failed"),
        ("failure_after_final", "failed after final"),
        ("bad_delta", "ValidationError"),
        ("bad_final", "ValidationError"),
        ("unsupported", "unsupported item"),
    ],
)
async def test_invalid_streams_record_one_failure_and_close_generator(
    runner: WorkflowRunner,
    history: MemoryHistory,
    execution_probe: ExecutionProbe,
    mode: str,
    message: str,
) -> None:
    with pytest.raises(ActivityError, match=message):
        await runner.run("streaming", input={"mode": mode}, workflow_id="root")
    assert len(history.steps) == 2
    assert all(step.outcome.kind == "failed" for step in history.steps.values())
    assert execution_probe.cancelled == 1
    updated = history.updated
    with pytest.raises(ActivityError, match=message):
        await runner.run("streaming", continue_from_step_id=history.heads["root"])
    assert history.updated == updated
    assert execution_probe.calls == [2]


async def test_null_final_is_valid(
    runner: WorkflowRunner, history: MemoryHistory
) -> None:
    assert await runner.run("streaming", input={"mode": "null"}) is None


async def test_cancelled_stream_keeps_latest_snapshot_and_restarts_same_step(
    runner: WorkflowRunner, history: MemoryHistory, execution_probe: ExecutionProbe
) -> None:
    task = asyncio.create_task(
        runner.run("streaming", input={"mode": "blocked"}, workflow_id="root")
    )
    await asyncio.wait_for(execution_probe.started.get(), 2)
    head = history.heads["root"]
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 2)
    assert history.steps[head].outcome == models.StepStreaming(state={"index": 0})
    assert len(history.steps) == 1
    assert execution_probe.cancelled == 1
    execution_probe.gate.set()
    assert await runner.run("streaming", continue_from_step_id=head) == {"count": 2}
    assert history.steps[head].outcome == models.StepCompleted(result={"count": 2})
    assert history.appended[0] == head
    assert len(history.steps) == 2
    assert execution_probe.calls == [2, 2]
    assert execution_probe.cancelled == 2


async def test_update_storage_failure_is_fatal_and_closes_generator(
    runner: WorkflowRunner, history: MemoryHistory, execution_probe: ExecutionProbe
) -> None:
    history.fail_update = True
    with pytest.raises(OSError, match="storage"):
        await runner.run("streaming", workflow_id="root")
    assert len(history.steps) == 1
    assert history.steps[history.heads["root"]].outcome.kind == "streaming"
    assert execution_probe.cancelled == 1


async def test_many_updates_do_not_accumulate_step_versions(
    runner: WorkflowRunner, history: MemoryHistory
) -> None:
    assert await runner.run("streaming", input={"count": 1000}) == {"count": 1000}
    assert len(history.steps) == 2
    assert len(history.appended) == 2
    assert history.updated == 1002


async def test_fork_after_stream_reuses_final_result_without_updating_history(
    runner: WorkflowRunner, history: MemoryHistory, execution_probe: ExecutionProbe
) -> None:
    await runner.run("streaming", workflow_id="root")
    original = history.path(history.heads["root"])
    updated = history.updated
    assert await runner.run("streaming", continue_from_step_id=original[0].id) == {
        "count": 2
    }
    fork = history.path(history.heads["root"])
    assert fork[0] == original[0]
    assert fork[1].id != original[1].id
    assert history.steps[original[1].id] == original[1]
    assert history.updated == updated
    assert execution_probe.calls == [2]


async def test_terminal_stream_record_cannot_be_rewritten(
    runner: WorkflowRunner, history: MemoryHistory
) -> None:
    await runner.run("streaming", workflow_id="root")
    step = history.path(history.heads["root"])[0]
    with pytest.raises(ValueError, match="terminal"):
        await history.update_step(
            step.model_copy(update={"outcome": models.StepStreaming()})
        )


async def test_unfinished_stream_cannot_have_replayed_descendants(
    runner: WorkflowRunner, history: MemoryHistory
) -> None:
    await runner.run("streaming", workflow_id="root")
    path = history.path(history.heads["root"])
    history.replay_override = [
        path[0].model_copy(update={"outcome": models.StepStreaming()}),
        path[1],
    ]
    with pytest.raises(ReplayMismatchError, match="replay head"):
        await runner.run("streaming", continue_from_step_id=path[-1].id)


async def test_streaming_requires_active_context() -> None:
    @c.streamable_activity()
    async def example(
        ctx: ActivityExecutionContext,
    ) -> AsyncGenerator[c.ActivityFinal[int], None]:
        yield c.final(1)

    with pytest.raises(RuntimeError, match="active workflow"):
        await example()


def test_streaming_requires_async_generator() -> None:
    async def ordinary() -> int:
        return 1

    with pytest.raises(TypeError, match="async generator"):
        c.streamable_activity()(
            typing.cast(
                Callable[..., AsyncGenerator[c.ActivityFinal[int], None]], ordinary
            )
        )


def test_streaming_state_is_json_safe_and_activity_only() -> None:
    with pytest.raises(pydantic.ValidationError):
        models.StepStreaming(state=typing.cast(pydantic.JsonValue, object()))
    with pytest.raises(pydantic.ValidationError):
        models.WorkflowStep(
            id="s",
            workflow_id="w",
            operation=models.WorkflowCompleteOperation(),
            outcome=models.StepStreaming(),
        )
