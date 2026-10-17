"""Runtime identity supplied explicitly to activity implementations."""

from dataclasses import dataclass, field
from collections.abc import Awaitable, Callable
from typing import Optional

from vectorops.runner.authentication.service import AuthenticationService
from vectorops.runner.environment.local import LocalEnvironment
from vectorops.runner.mcp.runtime import MCPRuntime
from vectorops.runner.runner.mailbox import Mailbox
from vectorops.runner.runner.resources import CleanupRegistry
from vectorops.runner.runner.errors import AuthorizationPersistenceError
from vectorops.runner.runner.scheduler import Scheduler


@dataclass(frozen=True)
class ActivityExecutionContext:
    execution_id: Optional[str]
    workflow_id: str
    step_id: str
    mailbox: Optional[Mailbox] = field(default=None, compare=False, repr=False)
    authentication: Optional[AuthenticationService] = field(
        default=None, compare=False, repr=False
    )
    environment: Optional[LocalEnvironment] = field(
        default=None, compare=False, repr=False
    )
    resources: Optional[CleanupRegistry] = field(
        default=None, compare=False, repr=False
    )
    mcp: Optional[MCPRuntime] = field(default=None, compare=False, repr=False)
    authorized: bool = False
    persist_authorization: Optional[Callable[[], Awaitable[None]]] = field(
        default=None, compare=False, repr=False
    )
    created_at: Optional[str] = None
    scheduler: Scheduler = field(default_factory=Scheduler, compare=False, repr=False)

    async def record_authorization(self) -> None:
        if self.persist_authorization is None:
            raise AuthorizationPersistenceError(
                "authorization persistence is unavailable"
            )
        try:
            await self.persist_authorization()
        except Exception as exc:
            raise AuthorizationPersistenceError(
                "cannot persist tool authorization"
            ) from exc
