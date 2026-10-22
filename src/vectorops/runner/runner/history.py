"""Storage-independent access to workflow configuration and replay paths."""

from collections.abc import AsyncGenerator, Sequence
from typing import Optional, Protocol, runtime_checkable

from vectorops.runner import models


@runtime_checkable
class SnapshotBatchWriter(Protocol):
    async def update_steps(self, steps: Sequence[models.WorkflowStep]) -> None:
        """Commit all snapshots atomically, or leave every record unchanged."""
        ...


class WorkflowHistory(Protocol):
    async def save_workflow(self, workflow: models.Workflow) -> None:
        """Store immutable configuration, rejecting conflicting existing records."""
        ...

    async def get_workflow(self, workflow_id: str) -> models.Workflow: ...

    async def get_step(self, step_id: str) -> models.WorkflowStep: ...

    async def first_completion(
        self, workflow_ids: Sequence[str]
    ) -> Optional[models.WorkflowStep]:
        """Read the earliest committed terminal head among the selected workflows."""
        ...

    async def get_step_position(self, step_id: str) -> int:
        """Return the stable insertion position shared by snapshots of this step."""
        ...

    async def record_delivery(self, head_id: str, step_id: str) -> models.WorkflowStep:
        """Atomically append delivery metadata to the current branch head."""
        ...

    async def append_step(self, step: models.WorkflowStep) -> None:
        """Persist a new step, including unfinished identity, before invoking effects."""
        ...

    async def update_step(self, step: models.WorkflowStep) -> None:
        """Replace a pending or streaming record, preserving its identity and links.

        Keep only the latest snapshot and reject updates to terminal records.
        """
        ...

    def replay_path(
        self, workflow_id: str, head_step_id: str
    ) -> AsyncGenerator[models.WorkflowStep, None]:
        """Stream the complete root-to-head path inclusively in predecessor order."""
        ...
