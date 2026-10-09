"""Bounded host-side transports for the MCP SDK."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
import os
import signal
from typing import Optional

import anyio
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
import httpx
from mcp import types
from mcp.client.stdio import get_default_environment
from mcp.shared.message import SessionMessage

from .models import StdioServer


class MCPError(RuntimeError):
    """A sanitized MCP failure safe for activity history and diagnostics."""


class LimitedStream(httpx.AsyncByteStream):
    def __init__(self, stream: httpx.AsyncByteStream, limit: int) -> None:
        self.stream = stream
        self.limit = limit

    async def __aiter__(self) -> AsyncIterator[bytes]:
        total = 0
        async for chunk in self.stream:
            total += len(chunk)
            if total > self.limit:
                raise MCPError("MCP HTTP response exceeds the byte limit")
            yield chunk

    async def aclose(self) -> None:
        await self.stream.aclose()


class LimitedHTTPTransport(httpx.AsyncBaseTransport):
    def __init__(self, limit: int) -> None:
        self.transport = httpx.AsyncHTTPTransport(retries=0)
        self.limit = limit

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        response = await self.transport.handle_async_request(request)
        if response.headers.get("content-encoding", "identity") != "identity":
            await response.aclose()
            raise MCPError("compressed MCP HTTP responses are not supported")
        assert isinstance(response.stream, httpx.AsyncByteStream)
        response.stream = LimitedStream(response.stream, self.limit)
        return response

    async def aclose(self) -> None:
        await self.transport.aclose()


@asynccontextmanager
async def stdio(server: StdioServer, credentials: dict[str, str]) -> AsyncIterator[
    tuple[
        MemoryObjectReceiveStream[SessionMessage | Exception],
        MemoryObjectSendStream[SessionMessage],
    ]
]:
    """Own a POSIX process group and bounded JSONL streams; discard server stderr."""
    if os.name != "posix":
        raise MCPError("MCP stdio currently requires POSIX")
    incoming, reader = anyio.create_memory_object_stream[SessionMessage | Exception](0)
    writer, outgoing = anyio.create_memory_object_stream[SessionMessage](0)
    process: Optional[asyncio.subprocess.Process] = None
    tasks: list[asyncio.Task[None]] = []

    async def read() -> None:
        assert process is not None and process.stdout is not None
        try:
            while line := await process.stdout.readline():
                if len(line) > server.limits.max_bytes:
                    raise MCPError("MCP stdio frame exceeds the byte limit")
                message = types.JSONRPCMessage.model_validate_json(line)
                await incoming.send(SessionMessage(message))
        except Exception:
            try:
                await incoming.send(MCPError("MCP stdio stream is invalid"))
            except (anyio.BrokenResourceError, anyio.ClosedResourceError):
                pass
        finally:
            await incoming.aclose()

    async def write() -> None:
        assert process is not None and process.stdin is not None
        try:
            async with outgoing:
                async for message in outgoing:
                    encoded = (
                        message.message.model_dump_json(
                            by_alias=True, exclude_none=True
                        ).encode()
                        + b"\n"
                    )
                    if len(encoded) > server.limits.max_bytes:
                        raise MCPError("MCP stdio request exceeds the byte limit")
                    process.stdin.write(encoded)
                    await process.stdin.drain()
        except Exception:
            await incoming.aclose()

    try:
        process = await asyncio.create_subprocess_exec(
            server.command,
            *server.args,
            cwd=server.cwd,
            env={**get_default_environment(), **server.env, **credentials},
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
            start_new_session=True,
            limit=server.limits.max_bytes,
        )
        tasks = [asyncio.create_task(read()), asyncio.create_task(write())]
        yield reader, writer
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if process is not None:
            if process.stdin is not None:
                process.stdin.close()
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(
                    process.wait(), server.limits.shutdown_seconds / 2
                )
            except TimeoutError:
                pass
            finally:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                await process.wait()
        await incoming.aclose()
        await reader.aclose()
        await writer.aclose()
        await outgoing.aclose()
