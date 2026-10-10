"""Narrow credential acquisition capabilities consumed by transport adapters."""

from typing import Optional, Protocol

from . import models


class CredentialResolver(Protocol):
    async def acquire(
        self,
        profile: models.Profile,
        execution_id: Optional[str] = None,
        *,
        rejected: Optional[str] = None,
        interactive: bool = True,
    ) -> models.Credential: ...


class TokenSource(Protocol):
    async def acquire(
        self,
        execution_id: Optional[str] = None,
        *,
        rejected: Optional[str] = None,
        interactive: bool = True,
    ) -> models.Credential: ...
