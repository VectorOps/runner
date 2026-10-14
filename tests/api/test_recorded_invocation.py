import pydantic
import pytest

from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from vectorops.runner import models
from vectorops.runner.api import composition as c
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.errors import ReplayMismatchError


@pytest.mark.parametrize("reconstruct", [False, True])
async def test_recorded_invocation_replay_strategy(
    history: MemoryHistory, reconstruct: bool
) -> None:
    executions = 0
    reconstructions = 0

    @c.activity(name="child", kind="test.child")
    async def child(ctx: ActivityExecutionContext) -> pydantic.JsonValue:
        nonlocal executions
        executions += 1
        return "recorded"

    async def invoke(
        ctx: ActivityExecutionContext, step: models.WorkflowStep
    ) -> pydantic.JsonValue:
        nonlocal reconstructions
        reconstructions += 1
        return await child() if reconstruct else "recorded"

    async def cached(ctx: ActivityExecutionContext) -> pydantic.JsonValue:
        nonlocal reconstructions
        reconstructions += 1
        return "recorded"

    async def workflow() -> pydantic.JsonValue:
        operation = models.StepOperation(kind="test.invocation", name="invoke")
        if reconstruct:
            return await c.run_recorded_invocation(operation, invoke)
        return await c.workflow_context().run_activity(operation, cached)

    first = await replay.run(history, workflow)
    assert first.outcome == models.StepCompleted(result="recorded")
    before = dict(history.steps)
    assert await replay.run(history, workflow, first.id) == first
    assert history.steps == before
    assert reconstructions == (2 if reconstruct else 1)
    assert executions == (1 if reconstruct else 0)


async def test_recorded_invocation_rejects_reconstruction_mismatch(
    history: MemoryHistory,
) -> None:
    value = "first"

    async def invoke(
        ctx: ActivityExecutionContext, step: models.WorkflowStep
    ) -> pydantic.JsonValue:
        return value

    async def workflow() -> pydantic.JsonValue:
        return await c.run_recorded_invocation(
            models.StepOperation(kind="test.invocation", name="invoke"), invoke
        )

    first = await replay.run(history, workflow)
    value = "changed"
    with pytest.raises(ReplayMismatchError, match="activity outcome differs"):
        await replay.run(history, workflow, first.id)


async def test_recorded_invocation_can_reuse_selected_history(
    history: MemoryHistory,
) -> None:
    operation = models.StepOperation(kind="test.invocation", name="invoke")

    async def old(ctx: ActivityExecutionContext) -> pydantic.JsonValue:
        return "original"

    async def original() -> pydantic.JsonValue:
        return await c.workflow_context().run_activity(operation, old)

    first = await replay.run(history, original)

    async def reconstruct(
        ctx: ActivityExecutionContext, step: models.WorkflowStep
    ) -> pydantic.JsonValue:
        raise AssertionError("selected history must be reused")

    async def current() -> pydantic.JsonValue:
        return await c.run_recorded_invocation(
            operation, reconstruct, replay_when=lambda step: False
        )

    before = dict(history.steps)
    assert await replay.run(history, current, first.id) == first
    assert history.steps == before
