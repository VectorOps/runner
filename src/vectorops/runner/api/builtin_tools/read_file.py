"""Bounded text and binary file reads with native image tool results."""

import asyncio
import base64
import json
import mimetypes
from typing import Literal, Optional

import connect
import pydantic

from vectorops.runner.api.tool_registry import register_tool
from vectorops.runner.environment import models, paths
from vectorops.runner.environment import text as file_text
from vectorops.runner.environment.errors import EnvironmentError
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.api import tool_contracts as tools
from .environment import environment


class ReadFileArguments(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", strict=True)

    path: str = pydantic.Field(min_length=1)
    mode: Literal["auto", "text", "binary", "lines"] = "auto"
    offset: int = pydantic.Field(default=0, ge=0)
    limit: int = pydantic.Field(default=65536, gt=0)
    start_line: int = pydantic.Field(default=1, ge=1)
    max_lines: int = pydantic.Field(default=100, gt=0)

    @pydantic.model_validator(mode="after")
    def validate_range(self) -> "ReadFileArguments":
        if self.mode == "lines" and self.offset:
            raise ValueError("offset cannot be used in lines mode")
        if self.mode != "lines" and (self.start_line != 1 or self.max_lines != 100):
            raise ValueError("start_line and max_lines require lines mode")
        return self


@register_tool("read_file")
def read_file_tool(
    *, approval: Optional[tools.ToolApproval] = None
) -> tools.Tool[ReadFileArguments]:
    async def auth_context(
        context: ActivityExecutionContext, arguments: ReadFileArguments
    ) -> tools.AuthContext:
        scope = await asyncio.to_thread(
            paths.path_scope, environment(context).root, arguments.path
        )
        return tools.AuthContext(properties={"scope": scope})

    async def execute(
        context: ActivityExecutionContext, arguments: ReadFileArguments
    ) -> tools.ToolResponse:
        target = environment(context)
        paths.check_limit(arguments.limit, target.config.max_read_bytes)

        def read() -> models.ReadResult:
            with paths.open_regular(
                paths.resolve_path(target.root, arguments.path, allow_external=True)
            ) as stream:
                stream.seek(arguments.offset)
                content = stream.read(arguments.limit + 1)
                return models.ReadResult(
                    content=content[: arguments.limit],
                    eof=len(content) <= arguments.limit,
                )

        def read_lines() -> models.LineReadResult:
            with paths.open_regular(
                paths.resolve_path(target.root, arguments.path, allow_external=True)
            ) as stream:
                return file_text.read_lines(
                    stream,
                    models.LineReadRequest(
                        path=arguments.path,
                        start_line=arguments.start_line,
                        max_lines=arguments.max_lines,
                        byte_limit=arguments.limit,
                    ),
                    chunk_size=min(65536, target.config.max_read_bytes),
                )

        if arguments.mode == "lines":
            try:
                lines = await asyncio.to_thread(read_lines)
            except OSError as exc:
                raise EnvironmentError.from_os_error(exc) from exc
            return tools.ToolResponse(
                content=[connect.TextBlock(text=lines.model_dump_json())]
            )
        try:
            result = await asyncio.to_thread(read)
        except OSError as exc:
            raise EnvironmentError.from_os_error(exc) from exc
        mime = mimetypes.guess_type(arguments.path)[0] or "application/octet-stream"
        metadata: dict[str, pydantic.JsonValue] = {
            "path": arguments.path,
            "mime_type": mime,
            "offset": arguments.offset,
            "bytes_read": len(result.content),
            "next_offset": arguments.offset + len(result.content),
            "eof": result.eof,
            "truncated": not result.eof,
        }
        image = mime in ("image/png", "image/jpeg", "image/gif", "image/webp")
        encoded = base64.b64encode(result.content).decode("ascii")
        if (
            arguments.mode == "auto"
            and image
            and arguments.offset == 0
            and result.eof
            and result.content
        ):
            return tools.ToolResponse(
                content=[
                    connect.TextBlock(text=json.dumps(metadata)),
                    connect.ImageBlock(mime_type=mime, data=encoded),
                ]
            )
        text: Optional[str] = None
        if arguments.mode != "binary" and not (arguments.mode == "auto" and image):
            try:
                text = result.content.decode("utf-8")
            except UnicodeDecodeError:
                if arguments.mode == "text":
                    raise tools.ToolExecutionError(
                        "Byte range is not valid UTF-8; use auto, binary, or lines mode"
                    )
            if arguments.mode == "auto" and text is not None and "\x00" in text:
                text = None
        metadata.update(
            {
                "encoding": "utf-8" if text is not None else "base64",
                "content": text if text is not None else encoded,
            }
        )
        return tools.ToolResponse(
            content=[connect.TextBlock(text=json.dumps(metadata))]
        )

    return tools.Tool(
        "read_file",
        "Read a file using a target-relative, absolute, or home-relative path, including outside the target root, "
        "with bounded byte ranges (default 64 KiB), or UTF-8 lines. "
        "Auto returns text, complete supported images as image blocks, or lossless base64 for binary data. "
        "Partial images are base64, not image blocks. Use next_offset/eof to continue; limits are capped "
        "by the target environment. PDFs and other binary formats are not extracted into text.",
        ReadFileArguments,
        execute,
        approval=approval,
        contextual_auth_context=auth_context,
    )
