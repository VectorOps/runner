"""Storage-independent access to workflow configuration and replay paths."""

from collections.abc import AsyncGenerator
from typing import Protocol

from vectorops.runner import models


class WorkflowHistory(Protocol):
    async def save_workflow(self, workflow: models.Workflow) -> None:
        """Store immutable configuration, rejecting conflicting existing records."""
        ...

    async def get_workflow(self, workflow_id: str) -> models.Workflow: ...

    async def get_step(self, step_id: str) -> models.WorkflowStep: ...

    async def append_step(self, step: models.WorkflowStep) -> None:
        """Append an immutable record before its outcome is delivered to workflow code."""
        ...

    async def update_step(self, step: models.WorkflowStep) -> None:
        """Replace an unfinished streaming record, preserving its identity and links.

        Keep only the latest snapshot and reject updates to terminal records.
        """
        ...

    def replay_path(
        self, workflow_id: str, head_step_id: str
    ) -> AsyncGenerator[models.WorkflowStep, None]:
        """Stream the complete root-to-head path inclusively in predecessor order."""
        ...
