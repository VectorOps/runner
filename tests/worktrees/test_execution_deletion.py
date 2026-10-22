import asyncio
from pathlib import Path
from uuid import uuid4

import pytest

from tests.fixtures import worktree_cases as cases, worktree_deletion as flows
from tests.worktrees.test_deletion_workflows import start, status
from vectorops.runner.control import protocol as p
from vectorops.runner.control.service import RunnerService
from vectorops.runner.runner.project import RunnerProject
from vectorops.runner.worktrees import models as m


async def test_delete_paused_drains_resources_and_removes_abandoned_leases(
    worktree_project: RunnerProject, worktree_probe: cases.Probe
) -> None:
    async with RunnerService(worktree_project.root) as service:
        project = await service.runner.project()
        manager = project.worktrees
        record = await manager.create(m.Create(name="Shared"), key="shared")
        identifier = await start(
            service,
            flows.REFERENCE + "paused",
            "start",
            input={"cleanup_wait": True},
            worktree_id=str(record.id),
        )
        await status(service, identifier, "paused")
        abandoned, other = uuid4(), uuid4()
        await manager.store.acquire(record.id, abandoned, identifier)
        await manager.store.acquire(record.id, other, "other-execution")
        deleting = asyncio.create_task(
            service.dispatch("executions.delete", {"execution_id": identifier})
        )
        try:
            await asyncio.wait_for(worktree_probe.cleanup_started.wait(), 5)
            assert not deleting.done()
            directory = project.data_path / "executions" / identifier
            assert directory.exists()
            assert await manager.store.execution_uses(identifier)
            resuming = asyncio.create_task(
                service.dispatch(
                    "executions.resume",
                    p.Resume(
                        execution_id=identifier, idempotency_key="resume"
                    ).model_dump(mode="json"),
                )
            )
            await asyncio.sleep(0)
            assert not resuming.done()
            worktree_probe.cleanup_gate.set()
            assert p.Deleted.model_validate(await asyncio.wait_for(deleting, 5)).deleted
            assert not directory.exists()
            assert not await manager.store.execution_uses(identifier)
            assert await manager.store.uses(record.id) == [other]
            assert not (manager.directory / ".locks" / f"use-{abandoned}").exists()
            assert Path(record.path).exists()
            assert (await manager.store.get(record.id)).status == "ready"
            assert not await project.summaries.worktree_executions(
                record.id, record.path
            )
            with pytest.raises(FileNotFoundError):
                await resuming
        finally:
            worktree_probe.cleanup_gate.set()
            await deleting


async def test_missing_history_cleanup_refuses_live_lease_then_retries(
    worktree_project: RunnerProject,
) -> None:
    async with RunnerService(worktree_project.root) as service:
        project = await service.runner.project()
        manager = project.worktrees
        record = await manager.create(m.Create(name="Live"), key="live")
        async with manager.use(record.id, "missing"):
            with pytest.raises(p.RemoteError, match="live worktree lease"):
                await service.dispatch("executions.delete", {"execution_id": "missing"})
            assert await manager.store.execution_uses("missing")
        token = uuid4()
        await manager.store.acquire(record.id, token, "missing")
        assert not p.Deleted.model_validate(
            await service.dispatch("executions.delete", {"execution_id": "missing"})
        ).deleted
        assert not await manager.store.execution_uses("missing")
        assert Path(record.path).exists()
