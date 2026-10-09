"""Immutable MCP adapters using the existing tool execution and approval boundary."""

import base64
import json
from typing import Optional

import connect
from mcp import types
import pydantic

from vectorops.runner.api.llm import tools
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from . import models, schema
from .transport import MCPError


def convert(result: types.CallToolResult) -> tools.ToolResponse:
    content: list[connect.ToolResultContentBlock] = []
    for block in result.content:
        if isinstance(block, types.TextContent):
            content.append(connect.TextBlock(text=block.text))
        elif isinstance(block, types.ImageContent) and block.mimeType in (
            "image/png",
            "image/jpeg",
            "image/gif",
            "image/webp",
        ):
            try:
                base64.b64decode(block.data, validate=True)
            except ValueError:
                raise MCPError("MCP image contains invalid base64") from None
            content.append(
                connect.ImageBlock(mime_type=block.mimeType, data=block.data)
            )
        else:
            content.append(
                connect.TextBlock(
                    text=json.dumps(
                        {
                            "mcp_content": block.model_dump(
                                mode="json", by_alias=True, exclude_none=True
                            )
                        },
                        sort_keys=True,
                    )
                )
            )
    if result.structuredContent is not None:
        content.append(
            connect.TextBlock(
                text=json.dumps(
                    {"structuredContent": result.structuredContent}, sort_keys=True
                )
            )
        )
    return tools.ToolResponse(content=content, is_error=result.isError)


class MCPTool:
    def __init__(self, catalog: models.Catalog, descriptor: models.Descriptor) -> None:
        self._catalog = catalog.model_copy(deep=True)
        self._descriptor = descriptor.model_copy(deep=True)
        self._validator = schema.validator(self._descriptor.input_schema)

    @property
    def source(self) -> str:
        return self._catalog.server_name

    @property
    def descriptor(self) -> models.Descriptor:
        return self._descriptor.model_copy(deep=True)

    @property
    def definition(self) -> connect.ToolSpec:
        return connect.ToolSpec.external(
            name=self._descriptor.name,
            description=self._descriptor.description.strip()
            or self._descriptor.remote_name,
            input_schema=self._descriptor.model_copy(deep=True).input_schema,
        )

    @property
    def version(self) -> int:
        return 1

    @property
    def approval(self) -> Optional[tools.ToolApproval]:
        return tools.ToolApproval()

    def validate(self, arguments: dict[str, pydantic.JsonValue]) -> None:
        try:
            self._validator.validate(arguments)
        except Exception:
            raise tools.ToolExecutionError(
                "MCP arguments do not match the recorded schema"
            ) from None

    async def execute(
        self,
        context: ActivityExecutionContext,
        arguments: dict[str, pydantic.JsonValue],
    ) -> tools.ToolResponse:
        self.validate(arguments)
        if context.mcp is None:
            raise MCPError("MCP tools require an execution-owned runtime")
        current = await context.mcp.discover(
            self._catalog.server_name, self._catalog.server
        )
        descriptor = next(
            (
                item
                for item in current.tools
                if item.remote_name == self._descriptor.remote_name
            ),
            None,
        )
        if descriptor is None or descriptor.model_dump(
            exclude={"title"}
        ) != self._descriptor.model_dump(exclude={"title"}):
            raise MCPError(
                "MCP tool changed since discovery; explicitly discover a new catalog"
            )
        result = await context.mcp.call(
            self._catalog.server, self._descriptor.remote_name, arguments
        )
        if not result.isError and self._descriptor.output_schema is not None:
            try:
                schema.validator(self._descriptor.output_schema).validate(
                    result.structuredContent
                )
            except Exception:
                raise MCPError(
                    "MCP result does not match the recorded output schema"
                ) from None
        return convert(result)
