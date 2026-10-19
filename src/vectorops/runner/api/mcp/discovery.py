"""Recorded MCP tool selection and static or dynamic exposure."""

from collections.abc import Sequence
from typing import Optional

import pydantic

from vectorops.runner.api import composition as c
from vectorops.runner.api.tool_contracts import ToolBinding, ToolSet
from vectorops.runner.api.tool_registry import ToolConfig
from vectorops.runner.api.tool_sets import Static
from vectorops.runner.mcp import models
from vectorops.runner.mcp.binding import MCPTool
from vectorops.runner.mcp.discovery import Discovery, Ranking
from vectorops.runner.mcp.transport import MCPError
from vectorops.runner.runner.activity_context import ActivityExecutionContext

from . import configuration

Selection = models.Selection


class ToolSelectionConfig(ToolConfig):
    selections: Optional[list[Selection]] = None
    component: str = "mcp"


async def configured_tool_set(config: ToolSelectionConfig) -> ToolSet:
    """Apply configured exposure mode without creating a search tool for empty catalogs."""
    settings = await configuration.settings(config.component)
    selected = settings.selections() if config.selections is None else config.selections
    bindings = await _bindings(selected, settings)
    if not bindings or settings.mode == "static":
        return Static(tuple(bindings))
    return Discovery(bindings, max_active=settings.max_active)


async def configured_tools(config: ToolSelectionConfig) -> list[ToolBinding]:
    if config.selections is None:
        settings = await configuration.settings(config.component)
        return await _bindings(settings.selections(), settings)
    return await discover(config.selections, component=config.component)


async def discover(
    selections: Sequence[Selection], *, component: str = "mcp"
) -> list[ToolBinding]:
    """Snapshot selected servers and return stable, independently approved bindings.

    Empty selections expose no tools. Settings contain only credential references;
    launches and credential reads happen inside live activities, never on replay.
    """
    selected = [item.model_copy(deep=True) for item in selections]
    if not selected:
        return []
    return await _bindings(selected, await configuration.settings(component))


async def _bindings(
    selected: Sequence[Selection], settings: models.Settings
) -> list[ToolBinding]:
    """Bind selected tools within server filters, skipping disabled servers before I/O."""
    bindings: list[ToolBinding] = []
    names: set[str] = set()
    for name in sorted({item.server for item in selected}):
        server = settings.servers.get(name)
        if server is None:
            raise MCPError(f"MCP server is not configured: {name}")
        policy = settings.tool_settings(name)
        if not policy.enabled or not policy.tools or "*" in policy.exclude:
            continue
        selectors = [item for item in selected if item.server == name]
        if not any(item.tools for item in selectors):
            continue
        catalog = await _discover(name, server)
        for descriptor in catalog.tools:
            if not policy.allows(descriptor.remote_name):
                continue
            if not any(item.allows(descriptor.remote_name) for item in selectors):
                continue
            if descriptor.name in names:
                raise MCPError(
                    "MCP selected catalog contains an exposed-name collision"
                )
            names.add(descriptor.name)
            bindings.append(MCPTool(catalog, descriptor))
    return bindings


async def dynamic(
    selections: Sequence[Selection],
    *,
    component: str = "mcp",
    max_active: int = 32,
    ranking: Optional[Ranking] = None,
) -> Discovery:
    """Snapshot permitted tools for LLM.run(tool_set=...), initially search-only.

    Search calls are recorded and approved normally. Their successful results
    activate concrete bindings at the next round boundary, never bypassing the
    concrete tool's approval. Call again explicitly for a fresh catalog.
    """
    return Discovery(
        await discover(selections, component=component),
        max_active=max_active,
        ranking=ranking,
    )


@c.activity_wrapper(name="discover", kind="mcp.discover", version=1)
async def _discover(
    call: c.ActivityCall, name: str, server: models.ServerConfig
) -> models.Catalog:
    async def invoke(ctx: ActivityExecutionContext) -> pydantic.JsonValue:
        if ctx.mcp is None:
            raise MCPError("MCP discovery requires an execution-owned runtime")
        return (await ctx.mcp.discover_current(name, server)).model_dump(mode="json")

    value = await call.run(
        input={"server_name": name, "server": server.model_dump(mode="json")},
        invoke=invoke,
    )
    return models.Catalog.model_validate(value)


async def tools(server: str, *names: str, component: str = "mcp") -> list[ToolBinding]:
    """Discover all tools on one server, or an explicit set of remote names."""
    return await discover(
        [Selection(server=server, tools=list(names) or ["*"])], component=component
    )
