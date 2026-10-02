"""Runtime identity supplied explicitly to activity implementations."""

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ActivityExecutionContext:
    execution_id: Optional[str]
    workflow_id: str
    step_id: str
