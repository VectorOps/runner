from collections.abc import AsyncGenerator

import pydantic
import pytest

from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from vectorops.runner import models
from vectorops.runner.api import composition as c
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.errors import ActivityError, ReplayMismatchError
from vectorops.runner.runner.streaming import StreamItem


@pytest.mark.parametrize("streaming", [False, True])
async def test_wrapper_rebuilds_state_without_reinvoking(
    history: MemoryHistory, streaming: bool
) -> None:
    calls: list[str] = []
    states: list[list[int]] = []

    class State:
        def __init__(self) -> None:
            self.values: list[int] = []

        @c.activity_wrapper(kind="test.state", version=2)
        async def append(self, call: c.ActivityCall, opaque: object) -> tuple[int, ...]:
            async def invoke(ctx: ActivityExecutionContext) -> int:
                assert c._CURRENT_CONTEXT.get() is None
                calls.append(ctx.step_id)
                return 7

            async def stream(
                ctx: ActivityExecutionContext,
            ) -> AsyncGenerator[StreamItem, None]:
                yield c.delta("working")
                yield c.final(await invoke(ctx))

            value = (
                await call.stream(input={"key": "small"}, invoke=stream)
                if streaming
                else await call.run(input={"key": "small"}, invoke=invoke)
            )
            assert isinstance(value, int)
            self.values.append(value)
            return tuple(self.values)

    async def workflow() -> pydantic.JsonValue:
        state = State()
        assert await state.append(object()) == (7,)
        states.append(state.values)
        return None

    first = await replay.run(history, workflow)
    second = await replay.run(history, workflow, first.id)
    assert second.outcome == first.outcome
    assert len(calls) == 1
    assert states == [[7], [7]]
    operation = history.path(first.id)[0].operation
    assert isinstance(operation, models.StepOperation)
    assert operation.kind == "test.state"
    assert operation.input == {"key": "small"}
    assert operation.version == 2


async def test_wrapper_explicit_input_detects_mismatch(history: MemoryHistory) -> None:
    value = 1

    @c.activity_wrapper(kind="test.call")
    async def wrapped(call: c.ActivityCall) -> int:
        async def invoke(ctx: ActivityExecutionContext) -> int:
            return value

        return await call.run(input={"value": value}, invoke=invoke)

    first = await replay.run(history, wrapped)
    value = 2
    with pytest.raises(ReplayMismatchError):
        await replay.run(history, wrapped, first.id)


async def test_helper_lifetime_and_single_submission(history: MemoryHistory) -> None:
    saved: list[c.ActivityCall] = []

    async def invoke(ctx: ActivityExecutionContext) -> None:
        return None

    @c.activity_wrapper()
    async def wrapped(call: c.ActivityCall) -> None:
        saved.append(call)
        await call.run(input={}, invoke=invoke)
        with pytest.raises(RuntimeError, match="one submission"):
            await call.run(input={}, invoke=invoke)

    result = await replay.run(history, wrapped)
    assert result.outcome.kind == "completed"
    with pytest.raises(RuntimeError, match="no longer active"):
        await saved[0].run(input={}, invoke=invoke)
    with pytest.raises(RuntimeError, match="active workflow"):
        await wrapped()


async def test_wrapper_failure_replays_without_post_success_mutation(
    history: MemoryHistory,
) -> None:
    callbacks: list[str] = []
    mutations: list[str] = []

    @c.activity_wrapper(kind="test.failure")
    async def wrapped(call: c.ActivityCall) -> None:
        async def invoke(ctx: ActivityExecutionContext) -> None:
            callbacks.append(ctx.step_id)
            raise ValueError("failed")

        await call.run(input={}, invoke=invoke)
        mutations.append("success")

    async def workflow() -> pydantic.JsonValue:
        with pytest.raises(ActivityError):
            await wrapped()
        return None

    first = await replay.run(history, workflow)
    await replay.run(history, workflow, first.id)
    assert len(callbacks) == 1
    assert mutations == []


async def test_stream_callback_cannot_start_nested_activity(
    history: MemoryHistory,
) -> None:
    @c.activity_wrapper()
    async def inner(call: c.ActivityCall) -> None:
        raise AssertionError("wrapper must not run inside activity")

    @c.activity_wrapper()
    async def outer(call: c.ActivityCall) -> None:
        async def invoke(
            ctx: ActivityExecutionContext,
        ) -> AsyncGenerator[StreamItem, None]:
            with pytest.raises(RuntimeError, match="active workflow"):
                await inner()
            yield c.final(None)

        await call.stream(input={}, invoke=invoke)

    first = await replay.run(history, outer)
    assert first.outcome.kind == "completed"
