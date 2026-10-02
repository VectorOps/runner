from collections.abc import AsyncGenerator
from typing import Optional

from vectorops.runner import models


class MemoryHistory:
    """Test-only history with serialized copies and controllable replay failures."""

    def __init__(self) -> None:
        self.workflows: dict[str, models.Workflow] = {}
        self.steps: dict[str, models.WorkflowStep] = {}
        self.heads: dict[str, str] = {}
        self.appended: list[str] = []
        self.consumed: list[str] = []
        self.closed_paths = 0
        self.fail_append = False
        self.fail_save = False
        self.fail_update = False
        self.updated = 0
        self.replay_override: Optional[list[models.WorkflowStep]] = None
        self.replay_error: Optional[Exception] = None

    async def save_workflow(self, workflow: models.Workflow) -> None:
        if self.fail_save:
            raise OSError("configuration storage failed")
        existing = self.workflows.get(workflow.id)
        if existing is not None and existing != workflow:
            raise ValueError("workflow configuration conflict")
        self.workflows[workflow.id] = models.Workflow.model_validate_json(
            workflow.model_dump_json()
        )

    async def get_workflow(self, workflow_id: str) -> models.Workflow:
        return self.workflows[workflow_id]

    async def get_step(self, step_id: str) -> models.WorkflowStep:
        return self.steps[step_id]

    async def append_step(self, step: models.WorkflowStep) -> None:
        if self.fail_append:
            raise OSError("step storage failed")
        if step.id in self.steps:
            raise ValueError("duplicate step ID")
        self.steps[step.id] = models.WorkflowStep.model_validate_json(
            step.model_dump_json()
        )
        self.heads[step.workflow_id] = step.id
        self.appended.append(step.id)

    def path(self, head_step_id: str) -> list[models.WorkflowStep]:
        path: list[models.WorkflowStep] = []
        current: Optional[str] = head_step_id
        while current is not None:
            step = self.steps[current]
            path.append(step)
            current = step.previous_step_id
        return list(reversed(path))

    async def update_step(self, step: models.WorkflowStep) -> None:
        if self.fail_update:
            raise OSError("stream state storage failed")
        existing = self.steps[step.id]
        if existing.outcome.kind not in ("streaming", "pending", "waiting"):
            raise ValueError("terminal steps cannot be updated")
        if (existing.workflow_id, existing.previous_step_id, existing.operation) != (
            step.workflow_id,
            step.previous_step_id,
            step.operation,
        ):
            raise ValueError("step identity and links cannot change")
        self.steps[step.id] = models.WorkflowStep.model_validate_json(
            step.model_dump_json()
        )
        self.updated += 1

    async def replay_path(
        self, workflow_id: str, head_step_id: str
    ) -> AsyncGenerator[models.WorkflowStep, None]:
        try:
            if self.replay_error is not None:
                raise self.replay_error
            path = (
                self.path(head_step_id)
                if self.replay_override is None
                else self.replay_override
            )
            for step in path:
                self.consumed.append(step.id)
                yield step
        finally:
            self.closed_paths += 1
