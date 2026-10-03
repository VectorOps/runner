"""Framed byte transports shared by embedded and TCP protocol sessions."""

import asyncio
import json
import struct
import typing

from vectorops.runner.control import protocol as p


class ByteStream(typing.Protocol):
    async def readexactly(self, size: int) -> bytes: ...
    async def write(self, data: bytes) -> None: ...
    async def aclose(self) -> None: ...


class TCPStream:
    def __init__(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self.reader = reader
        self.writer = writer

    async def readexactly(self, size: int) -> bytes:
        return await self.reader.readexactly(size)

    async def write(self, data: bytes) -> None:
        self.writer.write(data)
        await self.writer.drain()

    async def aclose(self) -> None:
        self.writer.close()
        try:
            await self.writer.wait_closed()
        except (ConnectionError, OSError):
            pass


class MemoryStream:
    def __init__(self) -> None:
        self.incoming: asyncio.Queue[bytes] = asyncio.Queue(maxsize=16)
        self.closed = asyncio.Event()
        self.buffer = bytearray()
        self.peer: typing.Optional[MemoryStream] = None

    async def readexactly(self, size: int) -> bytes:
        while len(self.buffer) < size:
            if self.closed.is_set() and self.incoming.empty():
                raise asyncio.IncompleteReadError(bytes(self.buffer), size)
            read = asyncio.create_task(self.incoming.get())
            closed = asyncio.create_task(self.closed.wait())
            try:
                await asyncio.wait((read, closed), return_when=asyncio.FIRST_COMPLETED)
                if read.done():
                    self.buffer.extend(read.result())
            finally:
                read.cancel()
                closed.cancel()
                await asyncio.gather(read, closed, return_exceptions=True)
        result = bytes(self.buffer[:size])
        del self.buffer[:size]
        return result

    async def write(self, data: bytes) -> None:
        assert self.peer is not None
        if self.closed.is_set():
            raise ConnectionError("stream closed")
        write = asyncio.create_task(self.peer.incoming.put(data))
        closed = asyncio.create_task(self.closed.wait())
        try:
            await asyncio.wait((write, closed), return_when=asyncio.FIRST_COMPLETED)
            if closed.done():
                raise ConnectionError("stream closed")
        finally:
            write.cancel()
            closed.cancel()
            await asyncio.gather(write, closed, return_exceptions=True)

    async def aclose(self) -> None:
        self.closed.set()
        if self.peer is not None:
            self.peer.closed.set()


def memory_pair() -> tuple[MemoryStream, MemoryStream]:
    left, right = MemoryStream(), MemoryStream()
    left.peer, right.peer = right, left
    return left, right


class Connection:
    def __init__(self, stream: ByteStream) -> None:
        self.stream = stream
        self._write_lock = asyncio.Lock()

    async def receive(self) -> p.Packet:
        length = struct.unpack("!I", await self.stream.readexactly(4))[0]
        if not 0 < length <= p.MAX_FRAME:
            raise ValueError("invalid frame size")
        raw = await self.stream.readexactly(length)
        value = json.loads(raw.decode("utf-8"), parse_constant=self._invalid_constant)
        json.dumps(value, allow_nan=False)
        return p.PACKET.validate_python(value)

    @staticmethod
    def _invalid_constant(value: str) -> typing.NoReturn:
        raise ValueError("nonfinite JSON number")

    async def send(self, packet: p.Packet) -> None:
        raw = json.dumps(
            packet.model_dump(mode="json"), allow_nan=False, separators=(",", ":")
        ).encode("utf-8")
        if len(raw) > p.MAX_FRAME:
            raise p.RemoteError("frame_too_large", "packet exceeds frame limit")
        async with self._write_lock:
            async with asyncio.timeout(30):
                await self.stream.write(struct.pack("!I", len(raw)) + raw)

    async def aclose(self) -> None:
        await self.stream.aclose()
