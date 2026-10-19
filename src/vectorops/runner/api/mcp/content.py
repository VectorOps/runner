"""Recorded MCP prompt and resource discovery and retrieval."""

from collections.abc import Sequence
from typing import Literal, Optional

import pydantic

from vectorops.runner.api import composition as c
from vectorops.runner.api.tool_contracts import ToolBinding
from vectorops.runner.mcp import models
from vectorops.runner.mcp import content, content_binding
from vectorops.runner.mcp.transport import MCPError
from vectorops.runner.runner.activity_context import ActivityExecutionContext

from . import configuration


class Content:
    """A detached, selected catalog; workflow retrieval is recorded, tools require approval.

    Prompt roles are returned as data and never automatically added to an LLM.
    Only listed resource URIs are accepted; templates and linked-URL fetching are
    deliberately unsupported. Direct workflow calls are trusted workflow code.
    """

    def __init__(self, catalog: content.ContentCatalog) -> None:
        self._catalog = catalog.model_copy(deep=True)

    @property
    def catalog(self) -> content.ContentCatalog:
        return self._catalog.model_copy(deep=True)

    def tools(self) -> list[ToolBinding]:
        bindings: list[ToolBinding] = []
        if self._catalog.prompts:
            bindings.append(content_binding.ContentTool(self._catalog, "prompt"))
        if self._catalog.resources:
            bindings.append(content_binding.ContentTool(self._catalog, "resource"))
        return bindings

    async def get_prompt(
        self, name: str, arguments: Optional[dict[str, str]] = None
    ) -> content.PromptResult:
        result = await _retrieve(self._catalog, "prompt", name, dict(arguments or {}))
        return content.PromptResult.model_validate(result)

    async def read_resource(self, uri: str) -> content.ResourceResult:
        result = await _retrieve(self._catalog, "resource", uri, {})
        return content.ResourceResult.model_validate(result)


async def discover_content(
    server: str,
    *,
    prompts: Sequence[str] = (),
    resources: Sequence[str] = (),
    component: str = "mcp",
) -> Content:
    """Record a catalog and select exact prompt names/resource URIs or explicit '*'.

    Empty selections grant no access. This does not change existing tool discovery
    or its recorded contracts. Catalog limits reuse max_tools as a total item cap.
    """
    settings = await configuration.settings(component)
    config = settings.servers.get(server)
    if config is None:
        raise MCPError(f"MCP server is not configured: {server}")
    if not prompts and not resources:
        return Content(content.ContentCatalog(server_name=server, server=config))
    catalog = await _content_catalog(server, config)
    return Content(catalog.select(prompts=prompts, resources=resources))


@c.activity_wrapper(name="catalog", kind="mcp.content_catalog", version=1)
async def _content_catalog(
    call: c.ActivityCall, name: str, server: models.ServerConfig
) -> content.ContentCatalog:
    async def invoke(ctx: ActivityExecutionContext) -> pydantic.JsonValue:
        if ctx.mcp is None:
            raise MCPError("MCP discovery requires an execution-owned runtime")
        return (await ctx.mcp.discover_current_content(name, server)).model_dump(
            mode="json"
        )

    result = await call.run(
        input={"server_name": name, "server": server.model_dump(mode="json")},
        invoke=invoke,
    )
    return content.ContentCatalog.model_validate(result)


async def get_prompt(
    server: str,
    name: str,
    arguments: Optional[dict[str, str]] = None,
    *,
    component: str = "mcp",
) -> content.PromptResult:
    selected = await discover_content(server, prompts=[name], component=component)
    return await selected.get_prompt(name, arguments)


async def read_resource(
    server: str, uri: str, *, component: str = "mcp"
) -> content.ResourceResult:
    selected = await discover_content(server, resources=[uri], component=component)
    return await selected.read_resource(uri)


async def list_prompts(server: str, *, component: str = "mcp") -> list[content.Prompt]:
    return (
        await discover_content(server, prompts=["*"], component=component)
    ).catalog.prompts


async def list_resources(
    server: str, *, component: str = "mcp"
) -> list[content.Resource]:
    return (
        await discover_content(server, resources=["*"], component=component)
    ).catalog.resources


@c.activity_wrapper(name="retrieve", kind="mcp.content", version=1)
async def _retrieve(
    call: c.ActivityCall,
    catalog: content.ContentCatalog,
    kind: Literal["prompt", "resource"],
    identity: str,
    arguments: dict[str, str],
) -> pydantic.JsonValue:
    async def invoke(ctx: ActivityExecutionContext) -> pydantic.JsonValue:
        return (
            await content_binding.retrieve(ctx, catalog, kind, identity, arguments)
        ).model_dump(mode="json")

    return await call.run(
        input={
            "catalog_digest": models.digest(catalog.model_dump(mode="json")),
            "kind": kind,
            "identity": identity,
            "arguments": dict(arguments),
        },
        invoke=invoke,
    )
