import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from vectorops.runner.api.sleep import sleep, sleep_until
from vectorops.runner.runner.scheduler import Scheduler
from vectorops.runner.runner.workflow import WorkflowRunner


async def test_completed_sleep_replay_does_not_schedule(
    history: MemoryHistory, monkeypatch: pytest.MonkeyPatch
) -> None:
    deadlines: list[datetime] = []

    async def wait(self: Scheduler, deadline: datetime) -> None:
        deadlines.append(deadline)

    monkeypatch.setattr(Scheduler, "wait_until", wait)

    async def workflow() -> None:
        await sleep(12.5)

    first = await replay.run(history, workflow)
    step = next(iter(history.steps.values()))
    assert step.created_at is not None
    assert deadlines == [
        datetime.fromisoformat(step.created_at) + timedelta(seconds=12.5)
    ]
    await replay.run(history, workflow, first.id)
    assert len(deadlines) == 1


@pytest.mark.parametrize("seconds", [-1.0, float("nan"), float("inf")])
async def test_invalid_duration(history: MemoryHistory, seconds: float) -> None:
    async def workflow() -> None:
        with pytest.raises(ValueError, match="finite and nonnegative"):
            await sleep(seconds)

    await replay.run(history, workflow)


async def test_zero_and_past_deadlines(history: MemoryHistory) -> None:
    async def workflow() -> None:
        await sleep(0)
        await sleep_until(datetime(2000, 1, 1, tzinfo=timezone.utc))
        with pytest.raises(ValueError, match="timezone-aware"):
            await sleep_until(datetime(2000, 1, 1))

    await asyncio.wait_for(replay.run(history, workflow), timeout=2)


async def test_sleep_until_normalizes_timezone(
    history: MemoryHistory, monkeypatch: pytest.MonkeyPatch
) -> None:
    deadlines: list[datetime] = []

    async def wait(self: Scheduler, deadline: datetime) -> None:
        deadlines.append(deadline)

    monkeypatch.setattr(Scheduler, "wait_until", wait)
    target = datetime(2030, 1, 1, tzinfo=timezone(timedelta(hours=5)))

    async def workflow() -> None:
        await sleep_until(target)

    first = await replay.run(history, workflow)
    target = target.astimezone(timezone.utc)
    await replay.run(history, workflow, first.id)
    assert deadlines == [target]
    assert deadlines[0].tzinfo is timezone.utc


@pytest.mark.parametrize("absolute", [False, True])
@pytest.mark.parametrize("expired", [False, True])
async def test_sqlite_restart_preserves_deadline(
    workspace: Path,
    monkeypatch: pytest.MonkeyPatch,
    absolute: bool,
    expired: bool,
) -> None:
    (workspace / "workflows" / "sleeper.py").write_text(
        "from datetime import datetime\n"
        "from vectorops.runner.api.sleep import sleep, sleep_until\n"
        "async def workflow(ctx, absolute: bool, target: str):\n"
        "    if absolute:\n"
        "        await sleep_until(datetime.fromisoformat(target))\n"
        "    else:\n"
        "        await sleep(3600)\n"
        "    await sleep(0)\n"
        "    return 'done'\n"
    )
    scheduled: asyncio.Queue[datetime] = asyncio.Queue()
    original_wait = Scheduler.wait_until

    async def wait(self: Scheduler, deadline: datetime) -> None:
        scheduled.put_nowait(deadline)
        await original_wait(self, deadline)

    monkeypatch.setattr(Scheduler, "wait_until", wait)
    runner = WorkflowRunner(workspace)
    target = datetime.now(timezone.utc) + timedelta(hours=1)
    task = asyncio.create_task(
        runner.run(
            "sleeper:workflow",
            input={"absolute": absolute, "target": target.isoformat()},
        )
    )
    first_deadline = await asyncio.wait_for(scheduled.get(), timeout=5)
    execution_id = runner.last_execution_id
    assert execution_id is not None
    runner.request_suspend(execution_id)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert runner._scheduler._timer is None
    assert not runner._scheduler._queue
    assert (await runner.execution_state(execution_id)).status == "running"

    restarted = WorkflowRunner(workspace)
    now = first_deadline.timestamp() + (10 if expired else -10)
    restarted._scheduler._clock = lambda: now
    resumed = asyncio.create_task(restarted.resume(execution_id))
    assert await asyncio.wait_for(scheduled.get(), timeout=5) == first_deadline
    if not expired:
        await asyncio.sleep(0)
        assert not resumed.done()
        now = first_deadline.timestamp() + 10
        restarted._scheduler._wake()
    assert await asyncio.wait_for(resumed, timeout=5) == "done"
    await asyncio.wait_for(scheduled.get(), timeout=1)
    assert scheduled.empty()
    assert await asyncio.wait_for(restarted.resume(execution_id), timeout=5) == "done"
    assert scheduled.empty()
