"""Bounded text and binary file reads with native image tool results."""

import base64
import json
import mimetypes
from typing import Literal, Optional

import connect
import pydantic

from vectorops.runner.environment import models
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from . import tools
from .environment_tools import environment


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


def read_file_tool(
    *, approval: Optional[tools.ToolApproval] = None
) -> tools.Tool[ReadFileArguments]:
    async def execute(
        context: ActivityExecutionContext, arguments: ReadFileArguments
    ) -> tools.ToolResponse:
        target = environment(context)
        if arguments.mode == "lines":
            lines = await target.read_lines(
                models.LineReadRequest(
                    path=arguments.path,
                    start_line=arguments.start_line,
                    max_lines=arguments.max_lines,
                    byte_limit=arguments.limit,
                )
            )
            return tools.ToolResponse(
                content=[connect.TextBlock(text=lines.model_dump_json())]
            )
        result = await target.read_file(
            models.ReadRequest(
                path=arguments.path,
                offset=arguments.offset,
                limit=arguments.limit,
            )
        )
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
        "Read a target-relative file with bounded byte ranges (default 64 KiB), or UTF-8 lines. "
        "Auto returns text, complete supported images as image blocks, or lossless base64 for binary data. "
        "Partial images are base64, not image blocks. Use next_offset/eof to continue; limits are capped "
        "by the target environment. PDFs and other binary formats are not extracted into text.",
        ReadFileArguments,
        execute,
        approval=approval,
    )
