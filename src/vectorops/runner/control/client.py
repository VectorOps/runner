"""Async host client with response correlation and independent live events."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
import secrets
import typing
from uuid import uuid4

import pydantic

from vectorops.runner.control import auth, protocol as p, transport
from vectorops.runner.control.server import RunnerServer
from vectorops.runner.control.service import RunnerService
from vectorops.runner.runner import execution_state, history_query


class RunnerClient:
    def __init__(self, connection: transport.Connection, instance_id: str) -> None:
        self.connection, self.instance_id = connection, instance_id
        self.pending: dict[str, asyncio.Future[pydantic.JsonValue]] = {}
        self.events: asyncio.Queue[p.Event] = asyncio.Queue(maxsize=256)
        self.closed = asyncio.Event()
        self.failure: Exception = ConnectionError("runner connection closed")
        self._reader = asyncio.create_task(self._read())

    @classmethod
    async def connect(cls, stream: transport.ByteStream, token: str) -> typing.Self:
        connection = transport.Connection(stream)
        try:
            instance_id = await auth.connect(connection, token)
        except BaseException:
            await connection.aclose()
            raise
        return cls(connection, instance_id)

    @classmethod
    async def tcp(
        cls, port: int, token: str, *, host: str = "127.0.0.1"
    ) -> typing.Self:
        if host not in ("127.0.0.1", "::1"):
            raise ValueError("unencrypted runner TCP requires a loopback endpoint")
        reader, writer = await asyncio.open_connection(host, port)
        return await cls.connect(transport.TCPStream(reader, writer), token)

    async def _read(self) -> None:
        try:
            while True:
                packet = await self.connection.receive()
                if isinstance(packet, p.Event):
                    try:
                        self.events.put_nowait(packet)
                    except asyncio.QueueFull as exc:
                        raise p.RemoteError(
                            "resync_required",
                            "client event queue overflowed; reconnect and reload history",
                        ) from exc
                elif isinstance(packet, (p.Response, p.Error, p.Pong)):
                    future = (
                        self.pending.get(packet.id) if packet.id is not None else None
                    )
                    if future is not None and not future.done():
                        if isinstance(packet, p.Error):
                            future.set_exception(
                                p.RemoteError(packet.code, packet.message)
                            )
                        else:
                            future.set_result(
                                packet.result
                                if isinstance(packet, p.Response)
                                else None
                            )
                else:
                    raise ValueError("unexpected runner packet")
        except Exception as exc:
            self.failure = exc
        finally:
            self.closed.set()
            for future in self.pending.values():
                if not future.done():
                    future.set_exception(self.failure)
            await self.connection.aclose()

    async def request(
        self,
        method: str,
        params: typing.Optional[dict[str, pydantic.JsonValue]] = None,
        *,
        timeout: float = 30,
    ) -> pydantic.JsonValue:
        if self.closed.is_set():
            raise self.failure
        identifier = uuid4().hex
        future: asyncio.Future[pydantic.JsonValue] = (
            asyncio.get_running_loop().create_future()
        )
        self.pending[identifier] = future
        try:
            async with asyncio.timeout(timeout):
                await self.connection.send(
                    p.Request(
                        id=identifier,
                        method=method,
                        params={} if params is None else params,
                    )
                )
                return await future
        finally:
            self.pending.pop(identifier, None)
            if not future.done():
                future.cancel()
            elif not future.cancelled():
                future.exception()

    async def next_event(self) -> p.Event:
        if not self.events.empty():
            return self.events.get_nowait()
        if self.closed.is_set():
            raise self.failure
        event = asyncio.create_task(self.events.get())
        closed = asyncio.create_task(self.closed.wait())
        try:
            await asyncio.wait((event, closed), return_when=asyncio.FIRST_COMPLETED)
            if event.done():
                return event.result()
            raise self.failure
        finally:
            event.cancel()
            closed.cancel()
            await asyncio.gather(event, closed, return_exceptions=True)

    async def start(self, params: p.Start) -> p.Accepted:
        return p.Accepted.model_validate(
            await self.request("executions.start", params.model_dump(mode="json"))
        )

    async def iter_events(self) -> AsyncIterator[p.Event]:
        while True:
            yield await self.next_event()

    async def info(self) -> pydantic.JsonValue:
        return await self.request("runner.info")

    async def executions(
        self, params: typing.Optional[p.ExecutionList] = None
    ) -> pydantic.JsonValue:
        return await self.request(
            "executions.list", (params or p.ExecutionList()).model_dump(mode="json")
        )

    async def shutdown(
        self, mode: typing.Literal["cancel", "drain"] = "cancel"
    ) -> None:
        await self.request("runner.shutdown", {"mode": mode})

    async def resume(self, params: p.Resume) -> p.Accepted:
        return p.Accepted.model_validate(
            await self.request("executions.resume", params.model_dump(mode="json"))
        )

    async def current(self) -> typing.Optional[p.Active]:
        result = await self.request("executions.current")
        return None if result is None else p.Active.model_validate(result)

    async def execution(self, execution_id: str) -> execution_state.ExecutionRecord:
        return execution_state.ExecutionRecord.model_validate(
            await self.request("executions.get", {"execution_id": execution_id})
        )

    async def stop(self, execution_id: str) -> p.Active:
        return p.Active.model_validate(
            await self.request("executions.stop", {"execution_id": execution_id})
        )

    async def history(
        self, query: history_query.HistoryQuery
    ) -> history_query.HistoryPage:
        return history_query.HistoryPage.model_validate(
            await self.request("history.query", query.model_dump(mode="json"))
        )

    async def subscribe(self, params: typing.Optional[p.Subscribe] = None) -> str:
        result = await self.request(
            "events.subscribe", (params or p.Subscribe()).model_dump(mode="json")
        )
        return p.Unsubscribe.model_validate(result).subscription_id

    async def unsubscribe(self, subscription_id: str) -> None:
        await self.request("events.unsubscribe", {"subscription_id": subscription_id})

    async def aclose(self) -> None:
        self._reader.cancel()
        await asyncio.gather(self._reader, return_exceptions=True)

    async def __aenter__(self) -> typing.Self:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()


@asynccontextmanager
async def embedded(path: Path) -> AsyncIterator[RunnerClient]:
    token = secrets.token_urlsafe(32)
    async with RunnerService(path) as service:
        server = RunnerServer(service, token)
        host, worker = transport.memory_pair()
        server.attach(worker)
        try:
            async with await RunnerClient.connect(host, token) as client:
                yield client
        finally:
            await server.aclose()
