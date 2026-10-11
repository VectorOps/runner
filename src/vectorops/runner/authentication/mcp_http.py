"""MCP HTTP token attachment with one bounded retry after a Bearer 401."""

from collections.abc import AsyncGenerator
from typing import Optional

import httpx

from . import contracts, models


class MCPHTTPAuth(httpx.Auth):
    def __init__(
        self,
        source: contracts.TokenSource,
        resource: str,
        execution_id: Optional[str],
    ) -> None:
        self.source = source
        self.resource = httpx.URL(resource)
        self.execution_id = execution_id
        self.value: Optional[models.Credential] = None
        self.rejected: Optional[str] = None

    async def prepare(self, execution_id: Optional[str] = None) -> None:
        """Resolve preconfigured credentials without requesting host interaction."""
        value = await self._acquire(
            self.rejected, execution_id or self.execution_id, interactive=False
        )
        if value.kind != "oauth2":
            raise models.AuthenticationError("MCP OAuth requires an OAuth credential")
        self.value = value
        self.rejected = None

    async def _acquire(
        self, rejected: Optional[str], execution_id: Optional[str], *, interactive: bool
    ) -> models.Credential:
        return await self.source.acquire(
            execution_id,
            rejected=rejected,
            interactive=interactive,
        )

    async def async_auth_flow(
        self, request: httpx.Request
    ) -> AsyncGenerator[httpx.Request, httpx.Response]:
        if request.url != self.resource:
            raise models.AuthenticationError("MCP OAuth request destination mismatch")
        if self.value is None:
            raise models.AuthenticationError("MCP OAuth credential is unavailable")
        sent = self.value.secret
        request.headers["Authorization"] = f"Bearer {sent}"
        response = yield request
        if response.status_code != 401:
            return
        self.rejected = sent
        challenge = response.headers.get("WWW-Authenticate", "")
        if not challenge.lower().startswith("bearer"):
            return
        await response.aread()
        value = await self._acquire(sent, self.execution_id, interactive=False)
        if value.kind != "oauth2":
            raise models.AuthenticationError("MCP OAuth requires an OAuth credential")
        self.value = value
        self.rejected = None
        request.headers["Authorization"] = f"Bearer {value.secret}"
        response = yield request
        if response.status_code == 401:
            self.rejected = value.secret
