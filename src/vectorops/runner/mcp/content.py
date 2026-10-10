"""Role-preserving prompt and resource snapshots with explicit binary fallbacks."""

import base64
from collections.abc import Sequence
import json
from typing import Literal, Optional

import connect
from mcp import types
import pydantic

from .models import Model, ServerConfig
from .transport import MCPError


class PromptArgument(Model):
    name: str
    description: Optional[str] = None
    required: bool = False


class Prompt(Model):
    name: str
    description: Optional[str] = None
    arguments: list[PromptArgument] = pydantic.Field(default_factory=list)


class Resource(Model):
    uri: str
    name: str
    description: Optional[str] = None
    mime_type: Optional[str] = None


class ContentCatalog(Model):
    version: Literal[1] = 1
    server_name: str
    server: ServerConfig
    prompts: list[Prompt] = pydantic.Field(default_factory=list)
    resources: list[Resource] = pydantic.Field(default_factory=list)

    def select(
        self, *, prompts: Sequence[str] = (), resources: Sequence[str] = ()
    ) -> "ContentCatalog":
        """Return a detached catalog limited to exact identities or explicit wildcards."""
        prompt_names, resource_uris = set(prompts), set(resources)
        return self.model_copy(
            update={
                "prompts": [
                    item
                    for item in self.prompts
                    if "*" in prompt_names or item.name in prompt_names
                ],
                "resources": [
                    item
                    for item in self.resources
                    if "*" in resource_uris or item.uri in resource_uris
                ],
            },
            deep=True,
        )


class PromptMessage(Model):
    role: Literal["user", "assistant"]
    content: list[connect.ToolResultContentBlock]


class PromptResult(Model):
    description: Optional[str] = None
    messages: list[PromptMessage]


class ResourceContent(Model):
    uri: str
    mime_type: Optional[str] = None
    content: list[connect.ToolResultContentBlock]


class ResourceResult(Model):
    contents: list[ResourceContent]


def image(data: str, mime_type: Optional[str]) -> Optional[connect.ImageBlock]:
    try:
        base64.b64decode(data, validate=True)
    except ValueError:
        raise MCPError("MCP content contains invalid base64") from None
    if mime_type in ("image/png", "image/jpeg", "image/gif", "image/webp"):
        return connect.ImageBlock(mime_type=mime_type, data=data)
    return None


def resource_content(
    value: types.TextResourceContents | types.BlobResourceContents,
) -> ResourceContent:
    if isinstance(value, types.TextResourceContents):
        block: connect.ToolResultContentBlock = connect.TextBlock(text=value.text)
    else:
        block = image(value.blob, value.mimeType) or connect.TextBlock(
            text=json.dumps(
                {"base64": value.blob, "mime_type": value.mimeType}, sort_keys=True
            )
        )
    return ResourceContent(
        uri=str(value.uri), mime_type=value.mimeType, content=[block]
    )


def block_content(
    value: (
        types.TextContent
        | types.ImageContent
        | types.AudioContent
        | types.ResourceLink
        | types.EmbeddedResource
    ),
) -> list[connect.ToolResultContentBlock]:
    if isinstance(value, types.TextContent):
        return [connect.TextBlock(text=value.text)]
    if isinstance(value, types.ImageContent):
        native = image(value.data, value.mimeType)
        if native is not None:
            return [native]
    if isinstance(value, types.EmbeddedResource):
        resource = resource_content(value.resource)
        return [
            connect.TextBlock(
                text=json.dumps({"uri": resource.uri, "mime_type": resource.mime_type})
            ),
            *resource.content,
        ]
    if isinstance(value, types.AudioContent):
        image(value.data, value.mimeType)
    return [
        connect.TextBlock(
            text=json.dumps(
                {
                    "mcp_content": value.model_dump(
                        mode="json", by_alias=True, exclude_none=True
                    )
                },
                sort_keys=True,
            )
        )
    ]


def prompt_result(value: types.GetPromptResult) -> PromptResult:
    return PromptResult(
        description=value.description,
        messages=[
            PromptMessage(role=message.role, content=block_content(message.content))
            for message in value.messages
        ],
    )


def resource_result(value: types.ReadResourceResult) -> ResourceResult:
    return ResourceResult(contents=[resource_content(item) for item in value.contents])
