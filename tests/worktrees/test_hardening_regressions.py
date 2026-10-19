import asyncio
from pathlib import Path
import sys
from typing import Optional
from unittest.mock import patch
from uuid import UUID

import pytest

from tests.fixtures import worktree_cases as cases
from vectorops.runner.environment.configuration import LocalEnvironmentConfig
from vectorops.runner.environment.errors import EnvironmentError, ErrorCode
from vectorops.runner.environment.processes import models as processes
from vectorops.runner.environment.target import LocalExecutionTarget
from vectorops.runner.runner.project import RunnerProject
from vectorops.runner.runner.workflow import WorkflowRunner
from vectorops.runner.worktrees import models as m
from vectorops.runner.worktrees.manager import Manager
from vectorops.runner.worktrees.runtime import ExecutionTargets


async def test_refresh_cannot_resurrect_a_concurrently_deleted_worktree(
    manager: Manager,
) -> None:
    record = await manager.create(m.Create(name="Refresh race"), key="refresh")
    read = asyncio.Event()
    resume = asyncio.Event()
    refreshing: Optional[asyncio.Task[m.Record]] = None
    original = manager.store.get

    async def get(identifier: UUID) -> m.Record:
        snapshot = await original(identifier)
        if asyncio.current_task() is refreshing and not read.is_set():
            read.set()
            await resume.wait()
        return snapshot

    with patch.object(manager.store, "get", get):
        refreshing = asyncio.create_task(
            manager.get(m.Get(worktree_id=record.id, refresh=True))
        )
        try:
            await asyncio.wait_for(read.wait(), 5)
            assert (
                await manager.delete(m.Delete(worktree_id=record.id))
            ).status == "deleted"
            resume.set()
            refreshed = await asyncio.wait_for(refreshing, 5)
        finally:
            resume.set()
            await asyncio.gather(refreshing, return_exceptions=True)
    assert refreshed.status == "deleted"
    assert (await manager.store.get(record.id)).status == "deleted"
    assert (await manager.listing(m.Query())).worktrees == []


@pytest.mark.parametrize("subdirectory", [False, True])
async def test_raw_target_path_protects_its_managed_worktree(
    worktree_project: RunnerProject, worktree_probe: cases.Probe, subdirectory: bool
) -> None:
    manager = worktree_project.worktrees
    record = await manager.create(m.Create(name="Raw path"), key="raw")
    selected = Path(record.path)
    if subdirectory:
        selected = selected / "nested"
        selected.mkdir()
    worktree_probe.gates["leaf"] = asyncio.Event()
    runner = WorkflowRunner(worktree_project.root, project=worktree_project)
    task = asyncio.create_task(
        runner.run(
            cases.REFERENCE + "leaf",
            input={"block": True},
            base_project_path=str(selected),
        )
    )
    try:
        assert await asyncio.wait_for(worktree_probe.started.get(), 5) == "leaf"
        assert await cases.cwd(worktree_probe.contexts["leaf"]) == str(selected)
        with pytest.raises(m.WorktreeError, match="in use"):
            await manager.delete(m.Delete(worktree_id=record.id, force=True))
        assert selected.is_dir()
    finally:
        task.cancel()
        worktree_probe.gates["leaf"].set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("same_worktree", [False, True])
async def test_exec_overrides_share_the_execution_session_limit(
    manager: Manager, repository: Path, same_worktree: bool
) -> None:
    first = await manager.create(m.Create(name="First"), key="first")
    second = (
        first
        if same_worktree
        else await manager.create(m.Create(name="Second"), key="second")
    )
    config = LocalEnvironmentConfig(
        base_project_path=str(repository),
        processes=processes.ProcessConfig(max_sessions=1),
    )
    targets = ExecutionTargets(manager, config, "execution")
    request = processes.ExecRequest(
        command=processes.ArgvCommand(
            argv=[sys.executable, "-c", "import time; time.sleep(60)"]
        ),
        yield_timeout_ms=0,
    )
    try:
        running = await targets.exec(request, first.id)
        assert running.state in ("starting", "running")
        with pytest.raises(EnvironmentError) as error:
            await targets.exec(request, second.id)
        assert error.value.code == ErrorCode.RESOURCE_LIMIT
    finally:
        await targets.close()


async def test_default_target_and_child_attachment_share_exec_capacity(
    manager: Manager, repository: Path
) -> None:
    record = await manager.create(m.Create(name="Child"), key="child")
    config = LocalEnvironmentConfig(
        base_project_path=str(repository),
        processes=processes.ProcessConfig(max_sessions=1),
    )
    targets = ExecutionTargets(manager, config, "execution")
    root = await targets.open(await LocalExecutionTarget.resolve(str(repository)))
    request = processes.ExecRequest(
        command=processes.ArgvCommand(
            argv=[sys.executable, "-c", "import time; time.sleep(60)"]
        ),
        yield_timeout_ms=0,
    )
    try:
        running = await root.processes.exec(request)
        async with targets.attach(await manager.resolve(record.id)) as child:
            with pytest.raises(EnvironmentError) as error:
                await child.processes.exec(request)
            assert error.value.code == ErrorCode.RESOURCE_LIMIT
            with pytest.raises(EnvironmentError) as error:
                await targets.exec(request, record.id)
            assert error.value.code == ErrorCode.RESOURCE_LIMIT
            assert (
                await root.processes.kill(
                    processes.SessionRequest(process_id=running.process_id)
                )
            ).termination_confirmed
            assert not (
                await root.processes.poll(
                    processes.PollRequest(process_id=running.process_id)
                )
            ).retained
            completed = await child.processes.exec(
                processes.ExecRequest(
                    command=processes.ArgvCommand(argv=[sys.executable, "-c", "pass"])
                )
            )
            assert completed.state == "exited" and not completed.retained
    finally:
        await targets.close()


async def test_raw_path_attach_cannot_race_worktree_deletion(manager: Manager) -> None:
    record = await manager.create(m.Create(name="Raw attach race"), key="raw-race")
    target = await LocalExecutionTarget.resolve(record.path)
    targets = ExecutionTargets(
        manager,
        LocalEnvironmentConfig(base_project_path=str(manager.target)),
        "execution",
    )
    identified = asyncio.Event()
    resume = asyncio.Event()
    original = manager.owning_worktree

    async def identify(selected: LocalExecutionTarget) -> Optional[UUID]:
        identifier = await original(selected)
        identified.set()
        await resume.wait()
        return identifier

    with patch.object(manager, "owning_worktree", identify):
        opening = asyncio.create_task(targets.open(target))
        try:
            await asyncio.wait_for(identified.wait(), 5)
            await manager.delete(m.Delete(worktree_id=record.id))
            resume.set()
            with pytest.raises(m.WorktreeError, match="not ready: deleted"):
                await asyncio.wait_for(opening, 5)
            assert await manager.store.uses(record.id) == []
        finally:
            resume.set()
            await asyncio.gather(opening, return_exceptions=True)
            await targets.close()
