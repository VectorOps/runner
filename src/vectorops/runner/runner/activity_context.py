"""Runtime identity supplied explicitly to activity implementations."""

from dataclasses import dataclass, field
from typing import Optional

from vectorops.runner.authentication.service import AuthenticationService
from vectorops.runner.environment.protocol import TargetEnvironment
from vectorops.runner.runner.mailbox import Mailbox
from vectorops.runner.runner.resources import CleanupRegistry


@dataclass(frozen=True)
class ActivityExecutionContext:
    execution_id: Optional[str]
    workflow_id: str
    step_id: str
    mailbox: Optional[Mailbox] = field(default=None, compare=False, repr=False)
    authentication: Optional[AuthenticationService] = field(
        default=None, compare=False, repr=False
    )
    environment: Optional[TargetEnvironment] = field(
        default=None, compare=False, repr=False
    )
    resources: Optional[CleanupRegistry] = field(
        default=None, compare=False, repr=False
    )
