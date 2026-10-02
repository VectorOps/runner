"""Runtime context passed to workflow entry points."""

from dataclasses import dataclass
from pathlib import Path

from vectorops.runner import models
from vectorops.runner.runner.project import RunnerProject


@dataclass(frozen=True)
class WorkflowExecutionContext:
    project: RunnerProject
    workflow: models.Workflow

    @property
    def data_path(self) -> Path:
        return self.project.data_path
