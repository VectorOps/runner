import asyncio
from pathlib import Path
import typing

import pytest

from tests.fixtures.history import MemoryHistory
from tests.fixtures.probe import ExecutionProbe
from vectorops.runner import models as m
from vectorops.runner.runner.errors import ActivityError, ReplayMismatchError
from vectorops.runner.runner.workflow import WorkflowRunner


async def test_records_activities_in_order(
    runner: WorkflowRunner, history: MemoryHistory, execution_probe: ExecutionProbe
) -> None:
    assert await runner.run("activities", input={"value": 3}, workflow_id="root") == 5
    steps = history.path(history.heads["root"])
    assert [step.operation.kind for step in steps] == [
        "test.increment",
        "workflow.resume",
        "test.increment",
        "workflow.resume",
        "workflow.complete",
    ]
    assert steps[0].previous_step_id is None
    assert steps[1].previous_step_id == steps[0].id
    assert steps[0].operation.model_dump()["input"] == {"value": 3, "amount": 1}
    assert execution_probe.calls == [3, 4]


async def test_no_activity_workflow_records_completion(
    runner: WorkflowRunner, history: MemoryHistory, workspace: Path
) -> None:
    assert await runner.run("activities", "empty", workflow_id="root") == {
        "id": "root",
        "data": str(workspace / "data"),
    }
    assert len(history.steps) == 1


async def test_result_is_recorded_before_next_activity(
    runner: WorkflowRunner, history: MemoryHistory, execution_probe: ExecutionProbe
) -> None:
    def check() -> None:
        assert len(history.steps) == 2 * len(execution_probe.calls) + 1
        assert (
            history.steps[execution_probe.contexts[-1].step_id].outcome.kind
            == "pending"
        )

    execution_probe.before_call = check
    assert await runner.run("activities") == 2


async def test_full_replay_does_not_reinvoke_or_append(
    runner: WorkflowRunner, history: MemoryHistory, execution_probe: ExecutionProbe
) -> None:
    assert await runner.run("activities", workflow_id="root") == 2
    head = history.heads["root"]
    before = list(history.appended)
    assert await runner.run("activities", continue_from_step_id=head) == 2
    assert history.appended == before
    assert execution_probe.calls == [0, 1]
    assert history.closed_paths == 1


async def test_fork_preserves_existing_descendants(
    runner: WorkflowRunner, history: MemoryHistory, execution_probe: ExecutionProbe
) -> None:
    await runner.run("activities", workflow_id="root")
    original = history.path(history.heads["root"])
    assert await runner.run("activities", continue_from_step_id=original[0].id) == 2
    fork = history.path(history.heads["root"])
    assert fork[0] == original[0]
    assert fork[1].id != original[1].id
    assert history.steps[original[1].id] == original[1]
    assert execution_probe.calls == [0, 1, 1]


@pytest.mark.parametrize(
    "kwargs",
    [{"workflow_id": "other"}, {"input": {"value": 10}}, {"callable_name": "empty"}],
)
async def test_resume_configuration_mismatch(
    runner: WorkflowRunner, history: MemoryHistory, kwargs: dict[str, object]
) -> None:
    await runner.run("activities", workflow_id="root")
    with pytest.raises(ValueError):
        await runner.run(
            "activities",
            continue_from_step_id=history.heads["root"],
            **typing.cast(dict[str, typing.Any], kwargs),
        )


async def test_unknown_and_empty_resume_step(runner: WorkflowRunner) -> None:
    with pytest.raises(ValueError):
        await runner.run("activities", continue_from_step_id="")
    with pytest.raises(KeyError):
        await runner.run("activities", continue_from_step_id="missing")


@pytest.mark.parametrize(
    "field,value",
    [
        ("name", "different"),
        ("kind", "different"),
        ("input", {"value": 10, "amount": 1}),
    ],
)
async def test_operation_mismatch_is_fatal(
    runner: WorkflowRunner, history: MemoryHistory, field: str, value: object
) -> None:
    await runner.run("activities", workflow_id="root")
    path = history.path(history.heads["root"])
    operation = path[0].operation.model_dump()
    operation[field] = value
    history.replay_override = [
        path[0].model_copy(
            update={"operation": m.StepOperation.model_validate(operation)}
        ),
        *path[1:],
    ]
    before = list(history.appended)
    with pytest.raises(ReplayMismatchError):
        await runner.run("activities", continue_from_step_id=path[-1].id)
    assert history.appended == before
    assert history.closed_paths == 1


@pytest.mark.parametrize(
    "mutation", ["wrong_workflow", "wrong_predecessor", "truncated", "extra"]
)
async def test_invalid_replay_path(
    runner: WorkflowRunner, history: MemoryHistory, mutation: str
) -> None:
    await runner.run("activities", workflow_id="root")
    path = history.path(history.heads["root"])
    if mutation == "wrong_workflow":
        history.replay_override = [
            path[0].model_copy(update={"workflow_id": "other"}),
            *path[1:],
        ]
    elif mutation == "wrong_predecessor":
        history.replay_override = [
            path[0].model_copy(update={"previous_step_id": "missing"}),
            *path[1:],
        ]
    elif mutation == "truncated":
        history.replay_override = path[:1]
    else:
        history.replay_override = [
            *path,
            path[-1].model_copy(
                update={"id": "extra", "previous_step_id": path[-1].id}
            ),
        ]
    with pytest.raises(ReplayMismatchError):
        await runner.run("activities", continue_from_step_id=path[-1].id)


async def test_changed_workflow_return_is_mismatch(
    runner: WorkflowRunner, history: MemoryHistory, workspace: Path
) -> None:
    await runner.run("activities", "empty", workflow_id="root")
    completion = history.steps[history.heads["root"]]
    history.replay_override = [
        completion.model_copy(update={"outcome": m.StepCompleted(result="different")})
    ]
    with pytest.raises(ReplayMismatchError, match="completion"):
        await runner.run("activities", "empty", continue_from_step_id=completion.id)


@pytest.mark.parametrize(
    "callable_name,message",
    [
        ("unhandled", "activity failed"),
        ("root_failure", "workflow failed"),
        ("invalid", "validation"),
        ("nested", "active workflow"),
    ],
)
async def test_failures_record_terminal_outcomes(
    runner: WorkflowRunner, history: MemoryHistory, callable_name: str, message: str
) -> None:
    with pytest.raises(ActivityError, match=message):
        await runner.run("activities", callable_name, workflow_id="root")
    terminal = history.steps[history.heads["root"]]
    assert terminal.operation.kind == "workflow.complete"
    assert terminal.outcome.kind == "failed"
    with pytest.raises(ActivityError, match=message):
        await runner.run("activities", callable_name, continue_from_step_id=terminal.id)


async def test_activity_failure_can_be_caught_and_replayed(
    runner: WorkflowRunner, history: MemoryHistory
) -> None:
    assert (
        await runner.run("activities", "caught", workflow_id="root")
        == "activity failed"
    )
    assert (
        await runner.run(
            "activities", "caught", continue_from_step_id=history.heads["root"]
        )
        == "activity failed"
    )


@pytest.mark.parametrize("failure", ["save", "append", "replay"])
async def test_history_failures_are_fatal(
    runner: WorkflowRunner, history: MemoryHistory, failure: str
) -> None:
    head = None
    if failure == "replay":
        await runner.run("activities", workflow_id="root")
        head = history.heads["root"]
        history.replay_error = OSError("replay failed")
    history.fail_save = failure == "save"
    history.fail_append = failure == "append"
    with pytest.raises(OSError):
        await runner.run("activities", continue_from_step_id=head)


async def test_cancellation_retains_pending_activity_identity(
    runner: WorkflowRunner, history: MemoryHistory, execution_probe: ExecutionProbe
) -> None:
    before = set(asyncio.all_tasks())
    task = asyncio.create_task(runner.run("activities", "wait", workflow_id="root"))
    await asyncio.wait_for(execution_probe.started.get(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 2)
    assert len(history.steps) == 1
    assert history.steps[history.heads["root"]].outcome.kind == "pending"
    assert execution_probe.active == 0
    assert execution_probe.cancelled == 1
    assert not set(asyncio.all_tasks()) - before


async def test_parallel_activities_run_without_deadlock(
    runner: WorkflowRunner, execution_probe: ExecutionProbe
) -> None:
    task = asyncio.create_task(runner.run("activities", "parallel"))
    await asyncio.wait_for(execution_probe.started.get(), 2)
    await asyncio.wait_for(execution_probe.started.get(), 2)
    assert execution_probe.active == 2
    execution_probe.gate.set()
    assert await asyncio.wait_for(task, 2) == [1, 2]


async def test_stop_is_safe_when_idle_and_runner_can_be_reused(
    runner: WorkflowRunner,
) -> None:
    runner.stop()
    assert await runner.run("activities") == 2
    runner.stop()
    assert await runner.run("activities") == 2


async def test_stop_cancels_workflow_loop_and_preserves_history(
    runner: WorkflowRunner, history: MemoryHistory, execution_probe: ExecutionProbe
) -> None:
    before = set(asyncio.all_tasks())
    task = asyncio.create_task(runner.run("activities", "loop", workflow_id="root"))
    await asyncio.wait_for(execution_probe.started.get(), 2)
    runner.stop()
    runner.stop()
    assert execution_probe.active == 1
    assert not task.done()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 2)
    assert execution_probe.active == 0
    assert execution_probe.cancelled == 1
    assert len(history.steps) == 1
    assert history.steps[history.heads["root"]].outcome.kind == "pending"
    assert not set(asyncio.all_tasks()) - before
    runner.stop()


async def test_stop_cancels_all_concurrent_root_executions(
    runner: WorkflowRunner, execution_probe: ExecutionProbe
) -> None:
    tasks = [asyncio.create_task(runner.run("activities", "wait")) for _ in range(2)]
    for _ in tasks:
        await asyncio.wait_for(execution_probe.started.get(), 2)
    runner.stop()
    for task in tasks:
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 2)
    assert execution_probe.active == 0
    assert execution_probe.cancelled == 2


async def test_long_history_is_consumed_incrementally(
    runner: WorkflowRunner, history: MemoryHistory, execution_probe: ExecutionProbe
) -> None:
    assert (
        await runner.run("activities", "many", input={"count": 300}, workflow_id="root")
        == 300
    )
    execution_probe.calls.clear()
    before = len(history.steps)
    assert (
        await runner.run(
            "activities", "many", continue_from_step_id=history.heads["root"]
        )
        == 300
    )
    assert execution_probe.calls == []
    assert len(history.consumed) == 601
    assert len(history.steps) == before
