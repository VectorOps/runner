"""Bounded capability-aware prompt/resource operations over an owned SDK session."""

import asyncio
from typing import Literal, Optional

from mcp import ClientSession, types
import pydantic

from . import content, models
from .transport import MCPError


async def catalog(
    session: ClientSession,
    name: str,
    server: models.ServerConfig,
    *,
    prompts: bool,
    resources: bool,
) -> content.ContentCatalog:
    """Bound the combined catalog, including pagination across capabilities."""
    found_prompts: list[content.Prompt] = []
    found_resources: list[content.Resource] = []
    size = 0
    async with asyncio.timeout(server.limits.request_seconds):
        for kind, enabled in (("prompts", prompts), ("resources", resources)):
            if not enabled:
                continue
            cursor: Optional[str] = None
            cursors: set[str] = set()
            identities: set[str] = set()
            for _ in range(server.limits.max_pages):
                if kind == "prompts":
                    page = await session.list_prompts(cursor=cursor)
                    entries = [
                        content.Prompt(
                            name=item.name,
                            description=item.description,
                            arguments=[
                                content.PromptArgument(
                                    name=arg.name,
                                    description=arg.description,
                                    required=bool(arg.required),
                                )
                                for arg in item.arguments or []
                            ],
                        )
                        for item in page.prompts
                    ]
                    keys = [item.name for item in entries]
                    for prompt in entries:
                        if len({arg.name for arg in prompt.arguments}) != len(
                            prompt.arguments
                        ):
                            raise MCPError(
                                "MCP prompt contains duplicate argument names"
                            )
                    found_prompts.extend(entries)
                    size += len(page.model_dump_json().encode())
                    cursor = page.nextCursor
                else:
                    resource_page = await session.list_resources(cursor=cursor)
                    resource_entries = [
                        content.Resource(
                            uri=str(item.uri),
                            name=item.name,
                            description=item.description,
                            mime_type=item.mimeType,
                        )
                        for item in resource_page.resources
                    ]
                    keys = [item.uri for item in resource_entries]
                    found_resources.extend(resource_entries)
                    size += len(resource_page.model_dump_json().encode())
                    cursor = resource_page.nextCursor
                for key in keys:
                    if key in identities:
                        raise MCPError(
                            "MCP content catalog contains duplicate identities"
                        )
                    identities.add(key)
                if (
                    size > server.limits.max_bytes
                    or len(found_prompts) + len(found_resources)
                    > server.limits.max_tools
                ):
                    raise MCPError("MCP content catalog exceeds configured limits")
                if not cursor:
                    break
                if cursor in cursors:
                    raise MCPError("MCP content catalog repeated a pagination cursor")
                cursors.add(cursor)
            else:
                raise MCPError("MCP content catalog exceeds the page limit")
    return content.ContentCatalog(
        server_name=name,
        server=server,
        prompts=sorted(found_prompts, key=lambda item: item.name),
        resources=sorted(found_resources, key=lambda item: item.uri),
    )


async def retrieve(
    session: ClientSession,
    server: models.ServerConfig,
    kind: Literal["prompt", "resource"],
    identity: str,
    arguments: dict[str, str],
) -> content.PromptResult | content.ResourceResult:
    request: dict[str, pydantic.JsonValue] = {
        "identity": identity,
        "arguments": dict(arguments),
    }
    if (
        len(pydantic.TypeAdapter(dict[str, pydantic.JsonValue]).dump_json(request))
        > server.limits.max_bytes
    ):
        raise MCPError("MCP content request exceeds the byte limit")
    async with asyncio.timeout(server.limits.request_seconds):
        if kind == "prompt":
            prompt = await session.get_prompt(identity, arguments)
            if len(prompt.model_dump_json().encode()) > server.limits.max_bytes:
                raise MCPError("MCP prompt result exceeds the byte limit")
            converted = content.prompt_result(prompt)
            if len(converted.model_dump_json().encode()) > server.limits.max_bytes:
                raise MCPError("MCP converted prompt exceeds the byte limit")
            return converted
        resource = await session.read_resource(pydantic.AnyUrl(identity))
        if len(resource.model_dump_json().encode()) > server.limits.max_bytes:
            raise MCPError("MCP resource result exceeds the byte limit")
        converted_resource = content.resource_result(resource)
        if len(converted_resource.model_dump_json().encode()) > server.limits.max_bytes:
            raise MCPError("MCP converted resource exceeds the byte limit")
        return converted_resource
