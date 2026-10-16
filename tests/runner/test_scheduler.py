import asyncio
from datetime import datetime, timezone

import pytest

from vectorops.runner.runner.scheduler import Scheduler


def deadline(seconds: float) -> datetime:
    return datetime.fromtimestamp(seconds, timezone.utc)


async def test_priority_order_and_equal_deadlines() -> None:
    now = 0.0
    scheduler = Scheduler(clock=lambda: now)
    completed: list[int] = []

    async def wait(when: float, identifier: int) -> None:
        await scheduler.wait_until(deadline(when))
        completed.append(identifier)

    tasks = [asyncio.create_task(wait(30, 0))]
    await asyncio.sleep(0)
    tasks.extend([asyncio.create_task(wait(10, 1)), asyncio.create_task(wait(10, 2))])
    await asyncio.sleep(0)
    now = 10
    scheduler._wake()
    await asyncio.sleep(0)
    assert completed == [1, 2]
    assert not tasks[0].done()
    now = 30
    scheduler._wake()
    await asyncio.gather(*tasks)
    assert completed == [1, 2, 0]
    assert scheduler._timer is None


async def test_cancellation_removes_waits_and_rearms_timer() -> None:
    now = 0.0
    scheduler = Scheduler(clock=lambda: now)
    tasks = [
        asyncio.create_task(scheduler.wait_until(deadline(when)))
        for when in (10, 20, 30)
    ]
    await asyncio.sleep(0)
    for index in (1, 0):
        tasks[index].cancel()
        with pytest.raises(asyncio.CancelledError):
            await tasks[index]
    assert len(scheduler._queue) == 1
    assert scheduler._queue[0][0] == 30
    now = 30
    scheduler._wake()
    await tasks[2]
    assert not scheduler._queue
    assert scheduler._timer is None


async def test_cancelling_last_wait_releases_timer() -> None:
    scheduler = Scheduler(clock=lambda: 0.0)
    task = asyncio.create_task(scheduler.wait_until(deadline(100)))
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert scheduler._timer is None
    assert not scheduler._queue


async def test_wall_clock_changes_do_not_wake_early() -> None:
    now = 10.0
    scheduler = Scheduler(clock=lambda: now)
    task = asyncio.create_task(scheduler.wait_until(deadline(20)))
    await asyncio.sleep(0)
    now = 0
    scheduler._wake()
    await asyncio.sleep(0)
    assert not task.done()
    now = 25
    scheduler._wake()
    await task


async def test_past_deadline_and_naive_datetime() -> None:
    scheduler = Scheduler(clock=lambda: 10.0)
    await asyncio.wait_for(scheduler.wait_until(deadline(0)), timeout=1)
    with pytest.raises(ValueError, match="timezone-aware"):
        await scheduler.wait_until(datetime(2025, 1, 1))
