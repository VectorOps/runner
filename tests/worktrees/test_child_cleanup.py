import asyncio
from unittest.mock import patch
from uuid import UUID

import pytest

from tests.fixtures import worktree_cases as cases
from vectorops.runner.environment.processes import models as processes
from vectorops.runner.environment.processes.manager import ProcessManager
from vectorops.runner.runner.project import RunnerProject
from vectorops.runner.runner.workflow import WorkflowRunner
from vectorops.runner.worktrees import models as m


@pytest.mark.parametrize(
    "mode", ["success", "failure", "cleanup_failure", "cancel", "repeated_cancel"]
)
async def test_child_cleanup_precedes_environment_close_and_lease_release(
    worktree_project: RunnerProject, worktree_probe: cases.Probe, mode: str
) -> None:
    manager = worktree_project.worktrees
    record = await manager.create(m.Create(name="Child resources"), key="child")
    worktree_probe.gates["child"] = asyncio.Event()
    runner = WorkflowRunner(worktree_project.root, project=worktree_project)
    original_close = ProcessManager.close
    original_release = manager.store.release

    async def close(process_manager: ProcessManager) -> list[processes.KillResult]:
        child = worktree_probe.contexts.get("child")
        is_child = (
            child is not None
            and child.environment is not None
            and child.environment.processes is process_manager
        )
        if is_child:
            assert await manager.store.uses(record.id)
            worktree_probe.events.append("child-process-close-enter")
        results = await original_close(process_manager)
        if is_child:
            assert all(result.termination_confirmed for result in results)
            worktree_probe.events.append("child-process-close-exit")
        return results

    async def release(token: UUID) -> None:
        await original_release(token)
        worktree_probe.events.append("child-lease-released")

    with (
        patch.object(ProcessManager, "close", close),
        patch.object(manager.store, "release", release),
    ):
        task = asyncio.create_task(
            runner.run(
                cases.REFERENCE + "resource_parent",
                input={"worktree_id": str(record.id), "mode": mode},
            )
        )
        try:
            assert await asyncio.wait_for(worktree_probe.started.get(), 5) == "child"
            if mode in ("cancel", "repeated_cancel"):
                task.cancel()
                await asyncio.wait_for(worktree_probe.cleanup_started.wait(), 5)
                if mode == "repeated_cancel":
                    for _ in range(3):
                        task.cancel()
                        await asyncio.sleep(0)
                        assert not task.done()
                    assert await manager.store.uses(record.id)
                    worktree_probe.cleanup_gate.set()
                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(task, 5)
            elif mode == "cleanup_failure":
                result = (
                    await asyncio.wait_for(
                        asyncio.gather(task, return_exceptions=True), 5
                    )
                )[0]
                assert isinstance(result, Exception) and "cleanup" in str(result)
            else:
                assert await asyncio.wait_for(task, 5) == (
                    "failed" if mode == "failure" else "completed"
                )
        finally:
            worktree_probe.cleanup_gate.set()
            worktree_probe.gates["child"].set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    events = worktree_probe.events
    expected_order = [
        "child-cleanup-enter",
        "child-cleanup-used-environment",
        "child-remaining-cleanup",
        "child-process-close-enter",
        "child-process-close-exit",
        "child-lease-released",
    ]
    assert [events.index(event) for event in expected_order] == sorted(
        events.index(event) for event in expected_order
    )
    assert all(events.count(event) == 1 for event in expected_order)
    assert events.index("child-lease-released") < events.index("parent-cleanup")
    if mode in ("success", "failure"):
        assert events.index("child-lease-released") < events.index("parent-after")
    assert await manager.store.uses(record.id) == []
    assert (await manager.get(m.Get(worktree_id=record.id))).status == "ready"
