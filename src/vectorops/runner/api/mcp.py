"""Workflow-side MCP discovery; completed catalog reads replay without server I/O."""

from collections.abc import Sequence
from typing import Final, Literal, Optional

import pydantic

from vectorops.runner.api import composition as c, configuration
from vectorops.runner.api.llm.tools import ToolBinding
from vectorops.runner.mcp import models
from vectorops.runner.mcp import content, content_binding
from vectorops.runner.mcp.binding import MCPTool
from vectorops.runner.mcp.discovery import Discovery, Ranking
from vectorops.runner.mcp.transport import MCPError
from vectorops.runner.runner.activity_context import ActivityExecutionContext

Selection = models.Selection
_SETTINGS_CONTRACT: Final = (
    "e27b17f60c6866b249b37b466f640e1123e34cfe37c0fc41eb7103c36cbd58e3"
)


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
    settings = models.Settings.model_validate(
        await configuration.get_validated(
            component, models.Settings, contract_digest=_SETTINGS_CONTRACT
        )
    )
    bindings: list[ToolBinding] = []
    names: set[str] = set()
    for name in sorted({item.server for item in selected}):
        server = settings.servers.get(name)
        if server is None:
            raise MCPError(f"MCP server is not configured: {name}")
        selectors = [item for item in selected if item.server == name]
        if not any(item.tools for item in selectors):
            continue
        catalog = await _discover(name, server)
        for descriptor in catalog.tools:
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
        effective = (
            ctx.mcp.pool.effective(server) if ctx.mcp.pool is not None else server
        )
        return (await ctx.mcp.discover(name, effective)).model_dump(mode="json")

    value = await call.run(
        input={"server_name": name, "server": server.model_dump(mode="json")},
        invoke=invoke,
    )
    return models.Catalog.model_validate(value)


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
    prompt_names, resource_uris = set(prompts), set(resources)
    settings = models.Settings.model_validate(
        await configuration.get_validated(
            component, models.Settings, contract_digest=_SETTINGS_CONTRACT
        )
    )
    config = settings.servers.get(server)
    if config is None:
        raise MCPError(f"MCP server is not configured: {server}")
    if not prompt_names and not resource_uris:
        return Content(content.ContentCatalog(server_name=server, server=config))
    catalog = await _content_catalog(server, config)
    return Content(
        catalog.model_copy(
            update={
                "prompts": [
                    item
                    for item in catalog.prompts
                    if "*" in prompt_names or item.name in prompt_names
                ],
                "resources": [
                    item
                    for item in catalog.resources
                    if "*" in resource_uris or item.uri in resource_uris
                ],
            }
        )
    )


@c.activity_wrapper(name="catalog", kind="mcp.content_catalog", version=1)
async def _content_catalog(
    call: c.ActivityCall, name: str, server: models.ServerConfig
) -> content.ContentCatalog:
    async def invoke(ctx: ActivityExecutionContext) -> pydantic.JsonValue:
        if ctx.mcp is None:
            raise MCPError("MCP discovery requires an execution-owned runtime")
        effective = (
            ctx.mcp.pool.effective(server) if ctx.mcp.pool is not None else server
        )
        return (await ctx.mcp.discover_content(name, effective)).model_dump(mode="json")

    result = await call.run(
        input={"server_name": name, "server": server.model_dump(mode="json")},
        invoke=invoke,
    )
    return content.ContentCatalog.model_validate(result)


async def tools(server: str, *names: str, component: str = "mcp") -> list[ToolBinding]:
    """Discover all tools on one server, or an explicit set of remote names."""
    return await discover(
        [Selection(server=server, tools=list(names) or ["*"])], component=component
    )


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


async def servers(*, component: str = "mcp") -> list[str]:
    settings = models.Settings.model_validate(
        await configuration.get_validated(
            component, models.Settings, contract_digest=_SETTINGS_CONTRACT
        )
    )
    return sorted(settings.servers)


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
