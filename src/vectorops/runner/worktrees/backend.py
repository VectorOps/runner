"""Backend contract for worktree operations, independent of runner orchestration."""

from pathlib import Path
from typing import Optional, Protocol

from . import models as m


class WorktreeBackend(Protocol):
    async def repository(self, target: Path) -> m.Repository: ...

    async def commit(self, repository: m.Repository, reference: str) -> str: ...

    async def validate_branch(self, repository: m.Repository, branch: str) -> None: ...

    async def create(self, record: m.Record, *, recover: bool) -> None: ...

    async def inspect(self, record: m.Record) -> Optional[m.Inspection]: ...

    async def delete(self, record: m.Record, *, force: bool) -> None: ...
