"""Lazy root-owned SDK sessions with same-task context-manager lifetime."""

import asyncio
from contextlib import AsyncExitStack
from datetime import timedelta
from typing import Literal, Optional, TYPE_CHECKING
from collections.abc import Callable

import httpx
from mcp import ClientSession, types
from mcp.client.streamable_http import streamable_http_client
from mcp.shared.context import RequestContext
from mcp.shared.session import RequestResponder
import pydantic

from vectorops.runner.authentication.service import AuthenticationService
from vectorops.runner.authentication.mcp_http import MCPHTTPAuth
from vectorops.runner.authentication import mcp_settings
from vectorops.runner.authentication.mcp_oauth import MCPAdapter
from vectorops.runner.runner.resources import finish_cleanup
from . import models, schema, transport
from . import content, content_runtime

if TYPE_CHECKING:
    from .pool import SharedPool


class Connection:
    def __init__(
        self,
        server: models.ServerConfig,
        authentication: Optional[AuthenticationService],
        authorization: Optional[MCPHTTPAuth] = None,
        changed: Optional[Callable[[], None]] = None,
    ) -> None:
        self.server = server.model_copy(deep=True)
        self.authentication = authentication
        self.authorization = authorization
        self.changed = changed
        self.catalog_revision = 0
        self.catalog_stale = False
        self.tools_list_changed = False
        self.ready: asyncio.Future[ClientSession] = (
            asyncio.get_running_loop().create_future()
        )
        self.stop = asyncio.Event()
        self.protocol_version = ""
        self.supports_tools = False
        self.supports_prompts = False
        self.supports_resources = False
        self.task = asyncio.create_task(self._own())

    async def _roots(
        self, context: RequestContext[ClientSession, None]
    ) -> types.ListRootsResult:
        return types.ListRootsResult(
            roots=[
                types.Root.model_validate(root.model_dump())
                for root in self.server.roots
            ]
        )

    async def _secret(self, profile: str) -> str:
        if self.authentication is None:
            raise transport.MCPError("MCP credential service is unavailable")
        return await self.authentication.mcp_secret(profile)

    async def _message(
        self,
        message: (
            RequestResponder[types.ServerRequest, types.ClientResult]
            | types.ServerNotification
            | Exception
        ),
    ) -> None:
        if (
            isinstance(message, types.ServerNotification)
            and isinstance(message.root, types.ToolListChangedNotification)
            and self.tools_list_changed
        ):
            self.catalog_revision += 1
            self.catalog_stale = True
            if self.changed is not None:
                self.changed()

    async def _own(self) -> None:
        """Enter and exit all SDK cancel scopes in one dedicated owner task."""
        try:
            async with AsyncExitStack() as stack:
                server = self.server
                if isinstance(server, models.StdioServer):
                    credentials = {
                        name: await self._secret(profile)
                        for name, profile in server.env_credentials.items()
                    }
                    read, write = await stack.enter_async_context(
                        transport.stdio(server, credentials)
                    )
                else:
                    headers = {**server.headers, "Accept-Encoding": "identity"}
                    for header, reference in server.header_credentials.items():
                        headers[header] = await self._secret(reference)
                    if isinstance(server.auth, mcp_settings.HeaderAuth):
                        headers[server.auth.header] = (
                            server.auth.prefix
                            + await self._secret(server.auth.credential)
                        )
                    if server.credential is not None:
                        headers["Authorization"] = (
                            f"Bearer {await self._secret(server.credential)}"
                        )
                    client = await stack.enter_async_context(
                        httpx.AsyncClient(
                            headers=headers,
                            auth=self.authorization,
                            transport=transport.LimitedHTTPTransport(
                                server.limits.max_bytes
                            ),
                            timeout=httpx.Timeout(server.limits.request_seconds),
                            follow_redirects=False,
                            trust_env=False,
                        )
                    )
                    read, write, _ = await stack.enter_async_context(
                        streamable_http_client(server.url, http_client=client)
                    )
                session = await stack.enter_async_context(
                    ClientSession(
                        read,
                        write,
                        read_timeout_seconds=timedelta(
                            seconds=server.limits.request_seconds
                        ),
                        list_roots_callback=self._roots,
                        message_handler=self._message,
                    )
                )
                initialized = await session.initialize()
                self.protocol_version = str(initialized.protocolVersion)
                self.supports_tools = initialized.capabilities.tools is not None
                self.tools_list_changed = bool(
                    initialized.capabilities.tools
                    and initialized.capabilities.tools.listChanged
                )
                self.supports_prompts = initialized.capabilities.prompts is not None
                self.supports_resources = initialized.capabilities.resources is not None
                self.ready.set_result(session)
                await self.stop.wait()
        except asyncio.CancelledError:
            if not self.ready.done():
                self.ready.set_exception(transport.MCPError("MCP startup cancelled"))
            raise
        except Exception:
            if not self.ready.done():
                self.ready.set_exception(
                    transport.MCPError("MCP session initialization failed")
                )

    async def session(self) -> ClientSession:
        if self.task.done():
            raise transport.MCPError(
                "MCP session disconnected; remote outcome may be unknown"
            )
        return await asyncio.shield(self.ready)

    async def close(self) -> None:
        self.stop.set()
        try:
            await asyncio.wait_for(
                asyncio.shield(self.task), self.server.limits.shutdown_seconds
            )
        except TimeoutError:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        finally:
            if self.ready.done() and not self.ready.cancelled():
                self.ready.exception()


class MCPRuntime:
    def __init__(
        self,
        authentication: Optional[AuthenticationService] = None,
        execution_id: Optional[str] = None,
        pool: Optional["SharedPool"] = None,
    ) -> None:
        self.authentication = authentication
        self.execution_id = execution_id
        self.pool = pool
        self.lease_id = str(id(self))
        self._authorizations: dict[str, MCPHTTPAuth] = {}
        self._connections: dict[str, Connection] = {}
        self._lock = asyncio.Lock()
        self._closed = False

    async def _connection(
        self, server: models.ServerConfig, execution_id: Optional[str] = None
    ) -> Connection:
        if self.pool is not None:
            if self._closed:
                raise transport.MCPError("MCP runtime is closed")
            return await self.pool.borrow(server, self.lease_id, self.execution_id)
        key = models.digest(server.model_dump(mode="json"))
        if self._closed:
            raise transport.MCPError("MCP runtime is closed")
        authorization = None
        if isinstance(server, models.HTTPServer) and (
            server.oauth is not None
            or isinstance(
                server.auth,
                (mcp_settings.AuthorizationCode, mcp_settings.ClientCredentials),
            )
        ):
            if self.authentication is None:
                raise transport.MCPError("MCP credential service is unavailable")
            authorization = self._authorizations.get(key)
            if authorization is None:
                if isinstance(server.auth, mcp_settings.ClientCredentials):
                    adapter: mcp_settings.ClientCredentials | MCPAdapter = server.auth
                else:
                    oauth_settings = (
                        server.auth
                        if isinstance(server.auth, mcp_settings.AuthorizationCode)
                        else server.oauth
                    )
                    assert oauth_settings is not None
                    adapter = await self.authentication.mcp_adapter(oauth_settings)
                authorization = self._authorizations.setdefault(
                    key, MCPHTTPAuth(self.authentication, adapter, None)
                )
            await authorization.prepare(execution_id or self.execution_id)
        async with self._lock:
            if self._closed:
                raise transport.MCPError("MCP runtime is closed")
            connection = self._connections.get(key)
            if connection is not None and connection.task.done():
                self._connections.pop(key)
                await connection.close()
                connection = None
            if connection is None:
                connection = Connection(server, self.authentication, authorization)
                self._connections[key] = connection
                try:
                    async with asyncio.timeout(server.limits.startup_seconds):
                        await connection.session()
                except BaseException:
                    self._connections.pop(key, None)
                    await finish_cleanup(asyncio.create_task(connection.close()))
                    raise
            return connection

    async def discover_current(
        self, name: str, server: models.ServerConfig
    ) -> models.Catalog:
        """Snapshot current pool overrides; existing catalogs continue using pinned discovery."""
        effective = self.pool.effective(server) if self.pool is not None else server
        return await self.discover(name, effective)

    async def discover_current_content(
        self, name: str, server: models.ServerConfig
    ) -> content.ContentCatalog:
        """Snapshot content using current configuration without rebinding recorded catalogs."""
        effective = self.pool.effective(server) if self.pool is not None else server
        return await self.discover_content(name, effective)

    async def discover(self, name: str, server: models.ServerConfig) -> models.Catalog:
        try:
            connection = await self._connection(server)
            revision = connection.catalog_revision
            session = await connection.session()
            tools: list[models.Descriptor] = []
            seen: set[str] = set()
            cursors: set[str] = set()
            cursor: Optional[str] = None
            size = 0
            if connection.supports_tools:
                async with asyncio.timeout(server.limits.request_seconds):
                    for _ in range(server.limits.max_pages):
                        page = await session.list_tools(cursor=cursor)
                        size += len(page.model_dump_json().encode())
                        if size > server.limits.max_bytes:
                            raise transport.MCPError(
                                "MCP catalog exceeds the byte limit"
                            )
                        for item in page.tools:
                            if item.name in seen:
                                raise transport.MCPError(
                                    "MCP catalog contains duplicate tool names"
                                )
                            seen.add(item.name)
                            schema.validator(item.inputSchema)
                            if item.outputSchema is not None:
                                schema.validator(item.outputSchema)
                            tools.append(
                                models.Descriptor(
                                    remote_name=item.name,
                                    name=models.tool_name(name, server, item.name),
                                    description=item.description or "",
                                    input_schema=item.inputSchema,
                                    output_schema=item.outputSchema,
                                    title=item.title,
                                )
                            )
                            if len(tools) > server.limits.max_tools:
                                raise transport.MCPError(
                                    "MCP catalog exceeds the tool limit"
                                )
                        cursor = page.nextCursor
                        if not cursor:
                            break
                        if cursor in cursors:
                            raise transport.MCPError(
                                "MCP catalog repeated a pagination cursor"
                            )
                        cursors.add(cursor)
                    else:
                        raise transport.MCPError("MCP catalog exceeds the page limit")
            connection.catalog_stale = connection.catalog_revision != revision
            return models.Catalog(
                server_name=name,
                server=server,
                protocol_version=connection.protocol_version,
                tools=sorted(tools, key=lambda item: item.remote_name),
            )
        except transport.MCPError:
            raise
        except Exception:
            raise transport.MCPError("MCP discovery failed") from None

    async def call(
        self,
        server: models.ServerConfig,
        name: str,
        arguments: dict[str, pydantic.JsonValue],
    ) -> types.CallToolResult:
        try:
            if (
                len(
                    pydantic.TypeAdapter(dict[str, pydantic.JsonValue]).dump_json(
                        arguments
                    )
                )
                > server.limits.max_bytes
            ):
                raise transport.MCPError("MCP arguments exceed the byte limit")
            connection = await self._connection(server)
            session = await connection.session()
            async with asyncio.timeout(server.limits.request_seconds):
                result = await session.send_request(
                    types.ClientRequest(
                        types.CallToolRequest(
                            params=types.CallToolRequestParams(
                                name=name, arguments=arguments
                            )
                        )
                    ),
                    types.CallToolResult,
                )
            if len(result.model_dump_json().encode()) > server.limits.max_bytes:
                raise transport.MCPError("MCP result exceeds the byte limit")
            return result
        except transport.MCPError:
            raise
        except Exception:
            raise transport.MCPError(
                "MCP call failed; remote outcome may be unknown"
            ) from None

    async def discover_content(
        self, name: str, server: models.ServerConfig
    ) -> content.ContentCatalog:
        try:
            connection = await self._connection(server)
            return await content_runtime.catalog(
                await connection.session(),
                name,
                server,
                prompts=connection.supports_prompts,
                resources=connection.supports_resources,
            )
        except transport.MCPError:
            raise
        except Exception:
            raise transport.MCPError("MCP content discovery failed") from None

    async def retrieve_content(
        self,
        server: models.ServerConfig,
        kind: Literal["prompt", "resource"],
        identity: str,
        arguments: dict[str, str],
    ) -> content.PromptResult | content.ResourceResult:
        try:
            connection = await self._connection(server)
            if not (
                connection.supports_prompts
                if kind == "prompt"
                else connection.supports_resources
            ):
                raise transport.MCPError(
                    "MCP server does not advertise the requested capability"
                )
            return await content_runtime.retrieve(
                await connection.session(), server, kind, identity, arguments
            )
        except transport.MCPError:
            raise
        except Exception:
            raise transport.MCPError("MCP content retrieval failed") from None

    async def close(self) -> None:
        if self.pool is not None:
            self._closed = True
            await self.pool.release(self.lease_id)
            return
        async with self._lock:
            self._closed = True
            connections = list(self._connections.values())
            self._connections.clear()
            self._authorizations.clear()
        await asyncio.gather(*(connection.close() for connection in connections))
