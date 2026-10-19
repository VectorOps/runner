import asyncio
from pathlib import Path
import sys
from uuid import UUID

import pytest

from vectorops.runner.worktrees import models as m
from vectorops.runner.worktrees.manager import Manager


async def crash(manager: Manager, mode: str, *arguments: str) -> UUID:
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "tests.fixtures.worktree_crash",
        mode,
        str(manager.workspace),
        str(manager.target),
        *arguments,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        async with asyncio.timeout(15):
            output, error = await process.communicate()
        assert process.returncode == 42, error.decode()
        return UUID(output.decode().strip())
    finally:
        if process.returncode is None:
            process.kill()
            await process.communicate()


async def test_process_crash_after_git_create_keeps_reserved_identity(
    manager: Manager,
) -> None:
    identifier = await crash(manager, "create")
    pending = await manager.get(m.Get(worktree_id=identifier))
    assert pending.status == "creating" and Path(pending.path).is_dir()
    restored = await manager.create(m.Create(name="Crash test"), key="crash")
    assert restored.id == identifier and restored.status == "ready"
    assert len((await manager.listing(m.Query())).worktrees) == 1


async def test_process_crash_after_git_delete_can_finalize_tombstone(
    manager: Manager,
) -> None:
    record = await manager.create(m.Create(name="Crash delete"), key="delete")
    assert await crash(manager, "delete", str(record.id)) == record.id
    assert (await manager.get(m.Get(worktree_id=record.id))).status == "deleting"
    assert not Path(record.path).exists()
    assert (await manager.delete(m.Delete(worktree_id=record.id))).status == "deleted"


async def test_process_crash_releases_os_lock_but_preserves_usage_warning(
    manager: Manager,
) -> None:
    record = await manager.create(m.Create(name="Crash lease"), key="lease")
    assert await crash(manager, "lease", str(record.id)) == record.id
    assert await manager.store.uses(record.id)
    with pytest.raises(m.WorktreeError, match="abandoned"):
        await manager.delete(m.Delete(worktree_id=record.id))
    assert Path(record.path).is_dir()
    assert (
        await manager.delete(m.Delete(worktree_id=record.id, force=True))
    ).status == "deleted"
