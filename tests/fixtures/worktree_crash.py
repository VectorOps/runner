import asyncio
import os
from pathlib import Path
import sys
from uuid import UUID

from vectorops.runner.runner.workspace_database import WorkspaceDatabase
from vectorops.runner.worktrees import models as m
from vectorops.runner.worktrees.git import GitBackend
from vectorops.runner.worktrees.manager import Manager


class CrashAfterGit(GitBackend):
    async def create(self, record: m.Record, *, recover: bool) -> None:
        await super().create(record, recover=recover)
        print(record.id, flush=True)
        os._exit(42)

    async def delete(self, record: m.Record, *, force: bool) -> None:
        await super().delete(record, force=force)
        print(record.id, flush=True)
        os._exit(42)


async def main() -> None:
    mode, data_path, repository = sys.argv[1:4]
    manager = Manager(
        Path(data_path),
        Path(repository),
        WorkspaceDatabase(Path(data_path)),
        m.Settings(),
        backend=CrashAfterGit(),
    )
    if mode == "create":
        await manager.create(m.Create(name="Crash test"), key="crash")
    elif mode == "delete":
        await manager.delete(m.Delete(worktree_id=UUID(sys.argv[4])))
    elif mode == "lease":
        async with manager.use(UUID(sys.argv[4]), "crashed-execution"):
            print(sys.argv[4], flush=True)
            os._exit(42)
    else:
        raise ValueError(mode)


if __name__ == "__main__":
    asyncio.run(main())
