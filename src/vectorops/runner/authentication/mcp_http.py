"""MCP HTTP token attachment with one bounded retry after a Bearer 401."""

from collections.abc import AsyncGenerator
from typing import Optional

import httpx

from . import mcp_oauth, models
from .service import AuthenticationService


class MCPHTTPAuth(httpx.Auth):
    def __init__(
        self,
        service: AuthenticationService,
        adapter: mcp_oauth.MCPAdapter,
        execution_id: Optional[str],
    ) -> None:
        self.service = service
        self.adapter = adapter
        self.execution_id = execution_id
        self.value: Optional[models.Credential] = None
        self.rejected: Optional[str] = None

    async def prepare(self) -> None:
        """Allow host interaction only before MCP startup/request deadlines begin."""
        value = await self.service.acquire(
            self.adapter.profile, self.execution_id, rejected=self.rejected
        )
        if value.kind != "oauth2":
            raise models.AuthenticationError("MCP OAuth requires an OAuth credential")
        self.value = value
        self.rejected = None

    async def async_auth_flow(
        self, request: httpx.Request
    ) -> AsyncGenerator[httpx.Request, httpx.Response]:
        if request.url != httpx.URL(self.adapter.settings.resource):
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
        value = await self.service.acquire(
            self.adapter.profile,
            self.execution_id,
            rejected=sent,
            interactive=False,
        )
        if value.kind != "oauth2":
            raise models.AuthenticationError("MCP OAuth requires an OAuth credential")
        self.value = value
        self.rejected = None
        request.headers["Authorization"] = f"Bearer {value.secret}"
        response = yield request
        if response.status_code == 401:
            self.rejected = value.secret
