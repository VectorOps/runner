"""Typed MCP administration shared by Python hosts and control RPC dispatch."""

import asyncio
from typing import Literal, Optional

import pydantic

from vectorops.runner.runner.project import Configuration

from . import models
from .content import ContentCatalog
from .pool import SessionStatus, SharedPool
from .runtime import MCPRuntime
from .transport import MCPError


class Query(models.Model):
    server: str = pydantic.Field(pattern=r"^[A-Za-z0-9_-]{1,64}$")
    component: str = "mcp"


class RootUpdate(Query):
    roots: Optional[list[models.Root]] = None


class Listing(models.Model):
    component: str = "mcp"


class ServerStatus(models.Model):
    name: str
    transport: Literal["stdio", "http"]
    session: SessionStatus


class ServerPage(models.Model):
    servers: list[ServerStatus]


class Management:
    def __init__(self, configuration: Configuration, pool: SharedPool) -> None:
        self.configuration, self.pool = configuration, pool
        self._tasks: set[asyncio.Task[object]] = set()
        self._closed = False

    async def settings(self, component: str) -> models.Settings:
        try:
            return self.configuration.get_validated(component, models.Settings)
        except Exception:
            raise MCPError("invalid MCP component configuration") from None

    async def server(self, query: Query) -> models.ServerConfig:
        settings = await self.settings(query.component)
        if query.server not in settings.servers:
            raise MCPError("MCP server is not configured")
        return settings.server(query.server)

    async def dispatch(
        self, method: str, params: dict[str, pydantic.JsonValue]
    ) -> pydantic.JsonValue:
        if self._closed:
            raise MCPError("MCP management is closed")
        task = asyncio.current_task()
        assert task is not None
        self._tasks.add(task)
        try:
            if method == "mcp.list":
                settings = await self.settings(Listing.model_validate(params).component)
                return ServerPage(
                    servers=[
                        ServerStatus(
                            name=name,
                            transport=server.kind,
                            session=self.pool.status(self.pool.effective(server)),
                        )
                        for name in sorted(settings.servers)
                        for server in [settings.server(name)]
                    ]
                ).model_dump(mode="json")
            query = (
                RootUpdate.model_validate(params)
                if method == "mcp.roots.set"
                else Query.model_validate(params)
            )
            base = await self.server(query)
            server = self.pool.effective(base)
            if method == "mcp.status":
                return self.pool.status(server).model_dump(mode="json")
            if method == "mcp.disconnect":
                await self.pool.disconnect(server)
                return None
            if method == "mcp.logout":
                await self.pool.logout(server)
                return None
            if method == "mcp.roots.set":
                assert isinstance(query, RootUpdate)
                updated = await self.pool.roots(base, query.roots)
                return self.pool.status(updated).model_dump(mode="json")
            runtime = MCPRuntime(self.pool.authentication, pool=self.pool)
            try:
                if method in ("mcp.connect", "mcp.login"):
                    await runtime._connection(server)
                    return self.pool.status(server).model_dump(mode="json")
                if method in ("mcp.discover", "mcp.refresh"):
                    return (await runtime.discover(query.server, server)).model_dump(
                        mode="json"
                    )
                if method == "mcp.content":
                    return (
                        await runtime.discover_content(query.server, server)
                    ).model_dump(mode="json")
                raise MCPError("unknown MCP management method")
            finally:
                await runtime.close()
        finally:
            self._tasks.discard(task)

    async def close(self) -> None:
        self._closed = True
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def listing(self, component: str = "mcp") -> ServerPage:
        return ServerPage.model_validate(
            await self.dispatch("mcp.list", {"component": component})
        )

    async def status(self, query: Query) -> SessionStatus:
        return SessionStatus.model_validate(
            await self.dispatch("mcp.status", query.model_dump(mode="json"))
        )

    async def connect(self, query: Query) -> SessionStatus:
        return SessionStatus.model_validate(
            await self.dispatch("mcp.connect", query.model_dump(mode="json"))
        )

    async def disconnect(self, query: Query) -> None:
        await self.dispatch("mcp.disconnect", query.model_dump(mode="json"))

    async def discover(self, query: Query) -> models.Catalog:
        return models.Catalog.model_validate(
            await self.dispatch("mcp.discover", query.model_dump(mode="json"))
        )

    async def content(self, query: Query) -> ContentCatalog:
        return ContentCatalog.model_validate(
            await self.dispatch("mcp.content", query.model_dump(mode="json"))
        )

    async def roots(self, query: RootUpdate) -> SessionStatus:
        return SessionStatus.model_validate(
            await self.dispatch("mcp.roots.set", query.model_dump(mode="json"))
        )

    async def logout(self, query: Query) -> None:
        await self.dispatch("mcp.logout", query.model_dump(mode="json"))
