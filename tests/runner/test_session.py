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
