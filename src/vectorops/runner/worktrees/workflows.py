"""Narrow execution-control capability used while deleting a worktree."""

from collections.abc import Awaitable, Callable
from typing import Protocol

from .models import Record


class WorkflowControl(Protocol):
    async def prepare_worktree_deletion(
        self,
        worktree: Record,
        execution_ids: set[str],
        *,
        force: bool,
        cancel: bool,
    ) -> Callable[[], Awaitable[None]]: ...
