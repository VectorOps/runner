"""Daemon-owned sessions shared by management and execution-scoped leases."""

import asyncio
from collections.abc import Callable
from typing import Optional
from uuid import uuid4

import pydantic

from vectorops.runner.authentication.service import AuthenticationService
from vectorops.runner.runner.resources import finish_cleanup
from . import models
from .runtime import Connection, MCPRuntime
from .transport import MCPError


class SessionStatus(models.Model):
    session_id: str
    connected: bool
    leases: int
    roots: list[models.Root]
    catalog_stale: bool


class SharedPool:
    def __init__(
        self,
        authentication: AuthenticationService,
        event_sink: Optional[Callable[[dict[str, pydantic.JsonValue]], None]] = None,
    ) -> None:
        self.runtime = MCPRuntime(authentication)
        self.authentication = authentication
        self.event_sink = event_sink
        self._leases: dict[str, set[str]] = {}
        self._overrides: dict[str, models.ServerConfig] = {}
        self._lock = asyncio.Lock()
        self._closed = False

    def effective(self, server: models.ServerConfig) -> models.ServerConfig:
        return self._overrides.get(
            models.digest(server.model_dump(mode="json")), server
        ).model_copy(deep=True)

    async def borrow(
        self, server: models.ServerConfig, owner: str, execution_id: Optional[str]
    ) -> Connection:
        key = models.digest(server.model_dump(mode="json"))
        pending = uuid4().hex
        async with self._lock:
            if self._closed:
                raise MCPError("MCP pool is closed")
            self._leases.setdefault(key, set()).add(pending)
        try:
            connection = await self.runtime._connection(server, execution_id)
            connection.changed = lambda: self._emit(key)
            async with self._lock:
                self._leases[key].discard(pending)
                self._leases[key].add(owner)
            return connection
        except BaseException:
            async with self._lock:
                self._leases.get(key, set()).discard(pending)
            raise

    def _emit(self, key: str) -> None:
        if self.event_sink is not None:
            try:
                self.event_sink({"kind": "mcp.catalog_changed", "session_id": key})
            except Exception:
                pass

    async def release(self, owner: str) -> None:
        async with self._lock:
            for leases in self._leases.values():
                leases.discard(owner)

    def status(self, server: models.ServerConfig) -> SessionStatus:
        key = models.digest(server.model_dump(mode="json"))
        connection = self.runtime._connections.get(key)
        return SessionStatus(
            session_id=key,
            connected=connection is not None and connection.usable,
            leases=len(self._leases.get(key, set())),
            roots=server.roots,
            catalog_stale=connection.catalog_stale if connection else False,
        )

    async def _disconnect(self, server: models.ServerConfig) -> None:
        key = models.digest(server.model_dump(mode="json"))
        if self._leases.get(key):
            raise MCPError("MCP session is leased by active operations or workflows")
        connection = self.runtime._connections.pop(key, None)
        self.runtime._authorizations.pop(key, None)
        if connection is not None:
            await finish_cleanup(asyncio.create_task(connection.close()))

    async def disconnect(self, server: models.ServerConfig) -> None:
        async with self._lock:
            await self._disconnect(server)

    async def roots(
        self, base: models.ServerConfig, roots: Optional[list[models.Root]]
    ) -> models.ServerConfig:
        """Reconfigure an idle shared session; existing workflow snapshots stay pinned."""
        async with self._lock:
            current = self.effective(base)
            key = models.digest(current.model_dump(mode="json"))
            if self._leases.get(key):
                raise MCPError("cannot change roots on a leased MCP session")
            updated = (
                base.model_copy(update={"roots": roots}, deep=True)
                if roots is not None
                else base.model_copy(deep=True)
            )
            next_key = models.digest(updated.model_dump(mode="json"))
            if next_key != key and next_key in self.runtime._connections:
                raise MCPError("an MCP session with the requested roots already exists")
            connection = self.runtime._connections.get(key)
            if connection is not None and connection.usable and next_key != key:
                connection.server = updated.model_copy(deep=True)
                try:
                    async with asyncio.timeout(updated.limits.request_seconds):
                        await (await connection.session()).send_roots_list_changed()
                except BaseException:
                    await self._disconnect(current)
                    raise
                self.runtime._connections.pop(key)
                self.runtime._connections[next_key] = connection
                authorization = self.runtime._authorizations.pop(key, None)
                if authorization is not None:
                    self.runtime._authorizations[next_key] = authorization
                connection.catalog_stale = True
                connection.changed = lambda: self._emit(next_key)
            base_key = models.digest(base.model_dump(mode="json"))
            if roots is None:
                self._overrides.pop(base_key, None)
            else:
                self._overrides[base_key] = updated
            return updated

    async def logout(self, server: models.ServerConfig) -> None:
        """Forget local tokens only when no pooled sessions could still use them."""
        async with self._lock:
            if any(self._leases.values()):
                raise MCPError(
                    "logout requires all MCP workflow/operation leases to be released"
                )
            for connection in list(self.runtime._connections.values()):
                await self._disconnect(connection.server)
            self.runtime._authorizations.clear()
            if isinstance(server, models.HTTPServer):
                await self.authentication.mcp.forget(server.authentication_settings())

    async def close(self) -> None:
        async with self._lock:
            self._closed = True
        await self.runtime.close()
