import asyncio
from contextlib import aclosing
import math
from pathlib import Path
import typing

import pytest

from tests.fixtures.history import MemoryHistory
from tests.fixtures.probe import ExecutionProbe
from vectorops.runner import models as m
from vectorops.runner.runner.cached_history import CachedHistory
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner.runner.workflow import WorkflowRunner


class CountingHistory(MemoryHistory):
    def __init__(self) -> None:
        super().__init__()
        self.workflow_reads = 0
        self.step_reads = 0
        self.workflow_saves = 0
        self.flushed = asyncio.Event()

    async def get_workflow(self, workflow_id: str) -> m.Workflow:
        self.workflow_reads += 1
        return await super().get_workflow(workflow_id)

    async def get_step(self, step_id: str) -> m.WorkflowStep:
        self.step_reads += 1
        return await super().get_step(step_id)

    async def save_workflow(self, workflow: m.Workflow) -> None:
        self.workflow_saves += 1
        await super().save_workflow(workflow)

    async def update_step(self, step: m.WorkflowStep) -> None:
        await super().update_step(step)
        self.flushed.set()


def active(identifier: str = "stream", state: int = 0) -> m.WorkflowStep:
    return m.WorkflowStep(
        id=identifier,
        workflow_id="w",
        operation=m.StepOperation(kind="activity", name="stream"),
        outcome=m.StepStreaming(state=state),
    )


async def seed(history: MemoryHistory) -> None:
    await history.save_workflow(m.Workflow(id="w", definition_ref="file:workflow"))
    await history.append_step(active())


async def test_repeated_reads_and_saves_use_cache() -> None:
    backend = CountingHistory()
    await seed(backend)
    async with CachedHistory(backend) as cache:
        for _ in range(5):
            workflow = await cache.get_workflow("w")
            await cache.save_workflow(workflow)
            assert await cache.get_step("stream") == active()
        assert backend.workflow_reads == 1
        assert backend.step_reads == 1
        assert backend.workflow_saves == 1


async def test_stream_updates_coalesce_and_terminal_write_is_immediate() -> None:
    backend = CountingHistory()
    await seed(backend)
    async with CachedHistory(backend, flush_interval=60) as cache:
        for index in range(1000):
            await cache.update_step(active(state=index))
        assert backend.updated == 0
        assert (await cache.get_step("stream")).outcome == m.StepStreaming(state=999)
        assert backend.steps["stream"].outcome == m.StepStreaming(state=0)
        completed = active().model_copy(
            update={"outcome": m.StepCompleted(result="done")}
        )
        await cache.update_step(completed)
        assert backend.updated == 1
        assert backend.steps["stream"] == completed
    assert backend.updated == 1


async def test_explicit_and_shutdown_flush_latest_state() -> None:
    backend = CountingHistory()
    await seed(backend)
    async with CachedHistory(backend, flush_interval=60) as cache:
        await cache.update_step(active(state=1))
        await cache.update_step(active(state=2))
        await cache.flush()
        assert backend.updated == 1
        assert backend.steps["stream"].outcome == m.StepStreaming(state=2)
        await cache.update_step(active(state=3))
    assert backend.updated == 2
    assert backend.steps["stream"].outcome == m.StepStreaming(state=3)


async def test_periodic_flush() -> None:
    backend = CountingHistory()
    await seed(backend)
    async with CachedHistory(backend, flush_interval=0.01) as cache:
        await cache.update_step(active(state=7))
        await asyncio.wait_for(backend.flushed.wait(), 2)
        assert backend.steps["stream"].outcome == m.StepStreaming(state=7)


async def test_read_cache_is_bounded_and_pending_state_survives_eviction() -> None:
    backend = CountingHistory()
    await seed(backend)
    await backend.append_step(active("other"))
    async with CachedHistory(backend, cache_size=1, flush_interval=60) as cache:
        await cache.update_step(active(state=9))
        await cache.get_step("other")
        assert (await cache.get_step("stream")).outcome == m.StepStreaming(state=9)
        assert backend.step_reads == 2
        await cache.get_step("other")
        assert backend.step_reads == 3


async def test_pending_limit_triggers_flush() -> None:
    backend = CountingHistory()
    await seed(backend)
    await backend.append_step(active("other"))
    async with CachedHistory(backend, max_pending=2, flush_interval=60) as cache:
        await cache.update_step(active(state=1))
        assert backend.updated == 0
        await cache.update_step(active("other", state=2))
        assert backend.updated == 2


async def test_workflow_cache_is_lru_bounded() -> None:
    backend = CountingHistory()
    await seed(backend)
    await backend.save_workflow(m.Workflow(id="other", definition_ref="file:workflow"))
    async with CachedHistory(backend, cache_size=1) as cache:
        await cache.get_workflow("w")
        await cache.get_workflow("other")
        await cache.get_workflow("other")
        await cache.get_workflow("w")
        assert backend.workflow_reads == 3


async def test_cancelled_owner_flushes_pending_state_without_leaking_worker() -> None:
    backend = CountingHistory()
    await seed(backend)
    ready = asyncio.Event()
    before = set(asyncio.all_tasks())

    async def owner() -> None:
        async with CachedHistory(backend, flush_interval=60) as cache:
            await cache.update_step(active(state=8))
            ready.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(owner())
    await asyncio.wait_for(ready.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 2)
    assert backend.steps["stream"].outcome == m.StepStreaming(state=8)
    assert backend.updated == 1
    assert not set(asyncio.all_tasks()) - before


async def test_shutdown_flush_failure_is_reported() -> None:
    backend = CountingHistory()
    await seed(backend)
    cache = CachedHistory(backend, flush_interval=60)
    await cache.update_step(active(state=1))
    backend.fail_update = True
    with pytest.raises(OSError):
        await cache.aclose()


async def test_replay_flushes_pending_snapshot_and_closes_backend_stream() -> None:
    backend = CountingHistory()
    await seed(backend)
    async with CachedHistory(backend, flush_interval=60) as cache:
        await cache.update_step(active(state=9))
        async with aclosing(cache.replay_path("w", "stream")) as path:
            assert (await anext(path)).outcome == m.StepStreaming(state=9)
        assert backend.closed_paths == 1


async def test_mutable_payloads_cannot_modify_cached_snapshots() -> None:
    backend = CountingHistory()
    await seed(backend)
    async with CachedHistory(backend) as cache:
        workflow = await cache.get_workflow("w")
        workflow.input["changed"] = True
        assert (await cache.get_workflow("w")).input == {}
        record = await cache.get_step("stream")
        assert isinstance(record.operation, m.StepOperation)
        record.operation.input["changed"] = True
        assert (await cache.get_step("stream")).operation.model_dump()["input"] == {}


async def test_validation_and_closed_cache() -> None:
    backend = CountingHistory()
    await seed(backend)
    cache = CachedHistory(backend, flush_interval=60)
    with pytest.raises(ValueError, match="identity"):
        await cache.update_step(
            active().model_copy(update={"previous_step_id": "different"})
        )
    completed = active().model_copy(update={"outcome": m.StepCompleted(result=1)})
    await cache.update_step(completed)
    with pytest.raises(ValueError, match="terminal"):
        await cache.update_step(active())
    with pytest.raises(ValueError, match="conflict"):
        await cache.get_workflow("w")
        await cache.save_workflow(m.Workflow(id="w", definition_ref="other:workflow"))
    await cache.aclose()
    await cache.aclose()
    with pytest.raises(RuntimeError, match="closed"):
        await cache.get_step("stream")


async def test_write_failure_is_not_hidden() -> None:
    backend = CountingHistory()
    await seed(backend)
    cache = CachedHistory(backend, flush_interval=60)
    await cache.update_step(active(state=1))
    backend.fail_update = True
    with pytest.raises(OSError):
        await cache.flush()
    backend.fail_update = False
    await cache.flush()
    assert backend.steps["stream"].outcome == m.StepStreaming(state=1)
    await cache.aclose()


async def test_background_failure_surfaces_on_access_and_close() -> None:
    backend = CountingHistory()
    await seed(backend)
    cache = CachedHistory(backend, flush_interval=0.01)
    backend.fail_update = True
    await cache.update_step(active(state=2))
    for _ in range(100):
        await asyncio.sleep(0.005)
        try:
            await cache.get_step("stream")
        except OSError:
            break
    else:
        raise AssertionError("background failure was not surfaced")
    with pytest.raises(OSError):
        await cache.aclose()


@pytest.mark.parametrize(
    "options",
    [
        {"cache_size": 0},
        {"max_pending": 0},
        {"flush_interval": 0},
        {"flush_interval": math.inf},
        {"flush_interval": math.nan},
    ],
)
def test_invalid_cache_options(options: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        CachedHistory(MemoryHistory(), **typing.cast(dict[str, typing.Any], options))


async def test_default_runner_coalesces_sqlite_updates(
    workspace: Path, execution_probe: ExecutionProbe, monkeypatch: pytest.MonkeyPatch
) -> None:
    updates = 0
    original = SQLiteHistory.update_step

    async def update(self: SQLiteHistory, step: m.WorkflowStep) -> None:
        nonlocal updates
        updates += 1
        await original(self, step)

    monkeypatch.setattr(SQLiteHistory, "update_step", update)
    assert await WorkflowRunner(workspace).run(
        "streaming:workflow", input={"count": 1000}
    ) == {"count": 1000}
    assert updates == 1
