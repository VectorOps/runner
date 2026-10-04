"""Bounded UTF-8 line reading shared by filesystem backends."""

import codecs
from typing import Protocol

from . import models as m
from .errors import EnvironmentError, ErrorCode


class ByteReader(Protocol):
    async def read_file(self, request: m.ReadRequest) -> m.ReadResult: ...


async def read_lines(
    reader: ByteReader, request: m.LineReadRequest, *, chunk_size: int = 65536
) -> m.LineReadResult:
    """Count skipped bytes toward the budget and preserve LF/CRLF text exactly."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    decoder = codecs.getincrementaldecoder("utf-8")("strict")
    offset = 0
    line = 1
    complete = 0
    output: list[str] = []
    pending_line = False
    eof = False
    while offset < request.byte_limit:
        result = await reader.read_file(
            m.ReadRequest(
                path=request.path,
                offset=offset,
                limit=min(chunk_size, request.byte_limit - offset),
            )
        )
        offset += len(result.content)
        if not result.content and not result.eof:
            raise EnvironmentError(ErrorCode.IO, "byte reader made no progress")
        try:
            text = decoder.decode(result.content, final=result.eof)
        except UnicodeDecodeError as exc:
            raise EnvironmentError(
                ErrorCode.INVALID_TEXT, "file is not valid UTF-8"
            ) from exc
        for index, character in enumerate(text):
            if line >= request.start_line:
                output.append(character)
                pending_line = character != "\n"
            if character == "\n":
                if line >= request.start_line:
                    complete += 1
                    if complete == request.max_lines:
                        return m.LineReadResult(
                            text="".join(output),
                            complete_lines=complete,
                            bytes_read=offset,
                            eof=result.eof and index == len(text) - 1,
                            truncated=False,
                        )
                line += 1
        eof = result.eof
        if eof:
            if pending_line:
                complete += 1
            break
    return m.LineReadResult(
        text="".join(output),
        complete_lines=complete,
        bytes_read=offset,
        eof=eof,
        truncated=not eof,
    )
