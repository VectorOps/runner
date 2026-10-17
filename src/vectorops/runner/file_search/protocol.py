"""Public file-search contract without implementation or transport dependencies."""

from collections.abc import AsyncGenerator
from typing import Protocol, runtime_checkable

from . import models


@runtime_checkable
class FileSearch(Protocol):
    async def refresh(self) -> models.RefreshId:
        """Schedule a background refresh, reusing an active refresh if present."""
        ...

    def search(
        self, query: str, *, limit: int = 20
    ) -> AsyncGenerator[models.SearchUpdate, None]:
        """Stream complete snapshots until the subscription or service closes."""
        ...

    async def close(self) -> None:
        """Cancel work, stop workers, and close subscriptions."""
        ...
