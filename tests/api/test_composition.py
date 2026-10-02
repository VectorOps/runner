from collections.abc import Awaitable, Callable
import typing

import pytest

from vectorops.runner.api.composition import activity
from vectorops.runner.runner.activity_context import ActivityExecutionContext


async def test_activity_requires_workflow_context() -> None:
    @activity()
    async def add(ctx: ActivityExecutionContext, value: int) -> int:
        return value + 1

    with pytest.raises(RuntimeError, match="active workflow"):
        await add(1)


def test_activity_requires_async_implementation() -> None:
    def synchronous() -> int:
        return 1

    with pytest.raises(TypeError, match="async"):
        activity()(typing.cast(Callable[..., Awaitable[int]], synchronous))


def test_default_and_explicit_activity_metadata() -> None:
    @activity()
    async def default(ctx: ActivityExecutionContext) -> int:
        return 1

    @activity(name="custom", kind="test.custom")
    async def custom(ctx: ActivityExecutionContext) -> int:
        return 2

    assert (default.name, default.kind) == ("default", "activity")
    assert (custom.name, custom.kind) == ("custom", "test.custom")
