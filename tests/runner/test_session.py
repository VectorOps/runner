import asyncio
from collections.abc import AsyncGenerator, Mapping
import typing

import pydantic
import pytest

from tests.fixtures.history import MemoryHistory
from vectorops.runner import models
from vectorops.runner.runner import context
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.session import ExecutionSession


async def unused_child(
    configuration: models.Workflow,
    *,
    continue_from_step_id: typing.Optional[str] = None,
    child_heads: typing.Optional[Mapping[str, str]] = None,
) -> models.WorkflowStep:
    raise AssertionError("no children expected")


async def test_closed_session_rejects_requests(history: MemoryHistory) -> None:
    session = ExecutionSession(
        history,
        models.Workflow(id="w", definition_ref="file:workflow"),
        unused_child,
        continue_from_step_id=None,
        child_heads={},
    )

    async def invoke() -> int:
        return 1

    await session.run(invoke)
    assert session.closed

    async def activity_invoke(ctx: ActivityExecutionContext) -> int:
        return 1

    with pytest.raises(RuntimeError, match="closed"):
        await session.submit(
            context.ActivityRequest(
                models.StepOperation(kind="activity", name="work"), activity_invoke
            )
        )


class GeneratedHistory:
    """Generate replay records without storing or materializing the history path."""

    def __init__(self, count: int) -> None:
        self.count = count
        self.yielded = 0
        self.requests = 0
        self.closed = False
        self.appended = 0

    async def save_workflow(self, workflow: models.Workflow) -> None:
        pass

    async def get_workflow(self, workflow_id: str) -> models.Workflow:
        raise AssertionError("replay should not query unrelated configuration")

    async def get_step(self, step_id: str) -> models.WorkflowStep:
        raise AssertionError("replay must use the stream")

    async def append_step(self, step: models.WorkflowStep) -> None:
        self.appended += 1

    async def update_step(self, step: models.WorkflowStep) -> None:
        raise AssertionError("completed replay records cannot be updated")

    async def replay_path(
        self, workflow_id: str, head_step_id: str
    ) -> AsyncGenerator[models.WorkflowStep, None]:
        try:
            for index in range(self.count):
                assert self.requests == index + 1
                self.yielded += 1
                yield models.WorkflowStep(
                    id=str(index),
                    workflow_id=workflow_id,
                    previous_step_id=None if index == 0 else str(index - 1),
                    operation=models.StepOperation(
                        kind="activity", name="increment", input={"value": index}
                    ),
                    outcome=models.StepCompleted(result=index + 1),
                )
        finally:
            self.closed = True


async def test_replay_does_not_prefetch_or_materialize_history() -> None:
    history = GeneratedHistory(1000)
    session = ExecutionSession(
        history,
        models.Workflow(id="w", definition_ref="file:workflow"),
        unused_child,
        continue_from_step_id="999",
        child_heads={},
    )

    async def forbidden(ctx: ActivityExecutionContext) -> pydantic.JsonValue:
        raise AssertionError("replayed activity must not execute")

    async def invoke() -> int:
        value = 0
        for index in range(history.count):
            history.requests += 1
            response = await session.submit(
                context.ActivityRequest(
                    models.StepOperation(
                        kind="activity", name="increment", input={"value": index}
                    ),
                    forbidden,
                )
            )
            assert response.kind == "activity"
            assert response.outcome.kind == "completed"
            assert response.outcome.result == index + 1
            value += 1
        return value

    completion = await session.run(invoke)
    assert completion.outcome == models.StepCompleted(result=1000)
    assert history.yielded == 1000
    assert history.appended == 1
    assert history.closed
    assert session.queue.empty()


class PausedReplayHistory(MemoryHistory):
    def __init__(self) -> None:
        super().__init__()
        self.at_end = asyncio.Event()
        self.release = asyncio.Event()

    async def replay_path(
        self, workflow_id: str, head_step_id: str
    ) -> AsyncGenerator[models.WorkflowStep, None]:
        try:
            for step in self.path(head_step_id):
                yield step
            self.at_end.set()
            await self.release.wait()
        finally:
            self.closed_paths += 1


@pytest.mark.parametrize("cancel_caller", [False, True])
async def test_replay_exhaustion_delivers_ready_invocations_once(
    cancel_caller: bool,
) -> None:
    history = PausedReplayHistory()
    workflow = models.Workflow(id="w", definition_ref="file:workflow")
    await history.save_workflow(workflow)
    operations = [models.StepOperation(kind="activity", name=str(i)) for i in range(2)]
    for index, operation in enumerate(operations):
        await history.append_step(
            models.WorkflowStep(
                id=str(index),
                workflow_id="w",
                previous_step_id=None if index == 0 else "0",
                operation=operation,
                outcome=models.StepCompleted(result=index),
                deferred_delivery=True,
            )
        )
    session = ExecutionSession(
        history, workflow, unused_child, continue_from_step_id="1", child_heads={}
    )
    callers: list[asyncio.Task[context.OperationResponse]] = []

    async def forbidden(ctx: ActivityExecutionContext) -> pydantic.JsonValue:
        raise AssertionError("completed activities must not execute")

    async def invoke() -> int:
        callers.extend(
            asyncio.create_task(
                session.submit(context.ActivityRequest(operation, forbidden))
            )
            for operation in operations
        )
        results = await asyncio.gather(*callers, return_exceptions=True)
        return sum(isinstance(result, context.ActivityResponse) for result in results)

    before = set(asyncio.all_tasks())
    run = asyncio.create_task(session.run(invoke))
    try:
        await asyncio.wait_for(history.at_end.wait(), 2)
        workers = [
            item.task for item in session.invocations.values() if item.task is not None
        ]
        await asyncio.wait_for(asyncio.gather(*workers), 2)
        assert not session.live.is_set()
        assert session.queue.empty()
        assert not any(caller.done() for caller in callers)
        if cancel_caller:
            callers[0].cancel()
            await asyncio.gather(callers[0], return_exceptions=True)
        history.release.set()
        completion = await asyncio.wait_for(run, 2)
    finally:
        run.cancel()
        await asyncio.gather(run, return_exceptions=True)
    assert completion.outcome == models.StepCompleted(result=1 if cancel_caller else 2)
    delivered = [
        step.operation.step_id
        for step in history.path(completion.id)
        if isinstance(step.operation, models.WorkflowResumeOperation)
    ]
    assert delivered == (["1"] if cancel_caller else ["0", "1"])
    assert not session.invocations
    assert session.queue.empty()
    assert history.closed_paths == 1
    assert not set(asyncio.all_tasks()) - before


async def test_cancellation_during_replay_never_starts_unfinished_activity() -> None:
    history = PausedReplayHistory()
    workflow = models.Workflow(id="w", definition_ref="file:workflow")
    operation = models.StepOperation(kind="activity", name="blocked")
    await history.save_workflow(workflow)
    await history.append_step(
        models.WorkflowStep(
            id="pending",
            workflow_id="w",
            operation=operation,
            outcome=models.StepPending(),
            deferred_delivery=True,
        )
    )
    session = ExecutionSession(
        history, workflow, unused_child, continue_from_step_id="pending", child_heads={}
    )
    executed = False

    async def activity(ctx: ActivityExecutionContext) -> pydantic.JsonValue:
        nonlocal executed
        executed = True
        return None

    async def invoke() -> pydantic.JsonValue:
        await session.submit(context.ActivityRequest(operation, activity))
        return None

    before = set(asyncio.all_tasks())
    run = asyncio.create_task(session.run(invoke))
    try:
        await asyncio.wait_for(history.at_end.wait(), 2)
    finally:
        run.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(run, 2)
    assert not executed
    assert not session.invocations
    assert session.queue.empty()
    assert history.closed_paths == 1
    assert list(history.steps) == ["pending"]
    assert not set(asyncio.all_tasks()) - before
