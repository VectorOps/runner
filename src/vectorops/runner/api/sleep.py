"""Durable workflow sleeps using persisted activity creation times."""

from datetime import datetime, timedelta
import math

from vectorops.runner.api.composition import ActivityCall, activity_wrapper
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.scheduler import normalize_deadline


@activity_wrapper(kind="sleep")
async def sleep(call: ActivityCall, seconds: float) -> None:
    """Wait a finite, nonnegative duration from this activity's durable creation."""
    seconds = float(seconds)
    if not math.isfinite(seconds) or seconds < 0:
        raise ValueError("sleep seconds must be finite and nonnegative")

    async def invoke(ctx: ActivityExecutionContext) -> None:
        if ctx.created_at is None:
            raise ValueError("sleep requires a persisted activity creation time")
        deadline = datetime.fromisoformat(ctx.created_at) + timedelta(seconds=seconds)
        await ctx.scheduler.wait_until(deadline)

    await call.run(input={"seconds": seconds}, invoke=invoke)


@activity_wrapper(kind="sleep_until")
async def sleep_until(call: ActivityCall, deadline: datetime) -> None:
    """Wait until a timezone-aware deadline; past deadlines complete immediately."""
    deadline = normalize_deadline(deadline)

    async def invoke(ctx: ActivityExecutionContext) -> None:
        await ctx.scheduler.wait_until(deadline)

    await call.run(input={"deadline": deadline.isoformat()}, invoke=invoke)
