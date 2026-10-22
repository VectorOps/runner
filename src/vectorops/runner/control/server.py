"""Authenticated concurrent request sessions with bounded event delivery."""

import asyncio
from functools import partial
import logging
import typing
from uuid import uuid4

import pydantic

from vectorops.runner import diagnostics
from vectorops.runner.control import auth, protocol as p, transport
from vectorops.runner.control.service import RunnerService


class Session:
    def __init__(
        self, service: RunnerService, connection: transport.Connection, token: str
    ) -> None:
        self.service, self.connection, self.token = service, connection, token
        self.subscriptions: dict[str, p.Subscribe] = {}
        self.sequence = 0
        self.outgoing: asyncio.Queue[p.Event] = asyncio.Queue(maxsize=256)
        self.overflow = asyncio.Event()
        self.requests: dict[str, asyncio.Task[None]] = {}
        self.session_id = uuid4().hex

    def event(self, event: dict[str, pydantic.JsonValue]) -> None:
        for identifier, subscription in tuple(self.subscriptions.items()):
            if (
                subscription.execution_id is not None
                and subscription.execution_id != event.get("execution_id")
            ):
                continue
            if subscription.kinds and event.get("kind") not in subscription.kinds:
                continue
            self.sequence += 1
            try:
                self.outgoing.put_nowait(
                    p.Event(
                        subscription_id=identifier, sequence=self.sequence, event=event
                    )
                )
            except asyncio.QueueFull:
                self.overflow.set()
                return

    async def _events(self) -> None:
        while True:
            event = await self.outgoing.get()
            try:
                await self.connection.send(event)
            finally:
                self.outgoing.task_done()

    async def _request(self, request: p.Request) -> None:
        try:
            if request.method == "events.subscribe":
                subscription = p.Subscribe.model_validate(request.params)
                if len(self.subscriptions) >= 16:
                    raise p.RemoteError("resource_limit", "too many subscriptions")
                identifier = uuid4().hex
                self.subscriptions[identifier] = subscription
                result: pydantic.JsonValue = {"subscription_id": identifier}
            elif request.method == "events.unsubscribe":
                selected = p.Unsubscribe.model_validate(request.params)
                self.subscriptions.pop(selected.subscription_id, None)
                result = None
            else:
                result = await self.service.dispatch(
                    request.method, request.params, session_id=self.session_id
                )
            await self.connection.send(p.Response(id=request.id, result=result))
        except p.RemoteError as exc:
            await self.connection.send(
                p.Error(
                    id=request.id, code=exc.code, message=str(exc), details=exc.details
                )
            )
        except (ValueError, TypeError) as exc:
            await self.connection.send(
                p.Error(id=request.id, code="invalid_argument", message=str(exc))
            )
        except (KeyError, FileNotFoundError):
            await self.connection.send(
                p.Error(
                    id=request.id,
                    code="not_found",
                    message="requested record or file does not exist",
                )
            )
        except Exception as exc:
            self.service.logs.emit(
                "control.request_failed", level=diagnostics.Level.ERROR, error=exc
            )
            logging.getLogger(__name__).exception("Control request failed")
            await self.connection.send(
                p.Error(
                    id=request.id,
                    code="internal_error",
                    message="runner operation failed",
                )
            )

    def _done(self, identifier: str, task: asyncio.Task[None]) -> None:
        self.requests.pop(identifier, None)
        if not task.cancelled() and task.exception() is not None:
            self.overflow.set()

    async def _read(self) -> None:
        while True:
            packet = await self.connection.receive()
            if isinstance(packet, p.Ping):
                await self.connection.send(p.Pong(id=packet.id))
            elif isinstance(packet, p.Request):
                if packet.id in self.requests:
                    raise ValueError("duplicate outstanding request ID")
                if len(self.requests) >= 128:
                    await self.connection.send(
                        p.Error(
                            id=packet.id,
                            code="resource_limit",
                            message="too many outstanding requests",
                        )
                    )
                    continue
                task = asyncio.create_task(self._request(packet))
                self.requests[packet.id] = task
                task.add_done_callback(partial(self._done, packet.id))
            else:
                raise ValueError("unexpected session packet")

    async def run(self) -> None:
        tasks: list[asyncio.Task[typing.Union[None, bool]]] = []
        try:
            await auth.accept(self.connection, self.token, self.service.instance_id)
            self.service.listen(self.event)
            tasks = [
                asyncio.create_task(self._read()),
                asyncio.create_task(self._events()),
                asyncio.create_task(self.overflow.wait()),
            ]
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        except (
            ValueError,
            ConnectionError,
            asyncio.IncompleteReadError,
            TimeoutError,
            p.RemoteError,
        ):
            pass
        finally:
            self.service.unlisten(self.event)
            self.service.authentication.disconnect(self.session_id)
            for task in tasks:
                task.cancel()
            await self.connection.aclose()
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.gather(*tuple(self.requests.values()), return_exceptions=True)


class RunnerServer:
    def __init__(self, service: RunnerService, token: str) -> None:
        self.service, self.token = service, auth.validate_token(token)
        self.sessions: set[asyncio.Task[None]] = set()
        self.connections: set[Session] = set()
        self.listener: typing.Optional[asyncio.Server] = None
        self.port: typing.Optional[int] = None
        self.closed = False

    def attach(self, stream: transport.ByteStream) -> asyncio.Task[None]:
        async def serve() -> None:
            if self.closed or len(self.sessions) > 64:
                await stream.aclose()
                return
            session = Session(self.service, transport.Connection(stream), self.token)
            self.connections.add(session)
            try:
                await session.run()
            finally:
                self.connections.discard(session)

        task = asyncio.create_task(serve())
        self.sessions.add(task)
        task.add_done_callback(self.sessions.discard)
        return task

    async def listen(self, port: int = 0) -> int:
        def accept(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            self.attach(transport.TCPStream(reader, writer))

        self.listener = await asyncio.start_server(accept, host="127.0.0.1", port=port)
        self.port = int(self.listener.sockets[0].getsockname()[1])
        return self.port

    async def finish_requests(self) -> None:
        if self.listener is not None:
            self.listener.close()
            await self.listener.wait_closed()
        requests = [
            task
            for session in tuple(self.connections)
            for task in tuple(session.requests.values())
        ]
        await asyncio.gather(*requests, return_exceptions=True)

    async def flush_events(self) -> None:
        try:
            async with asyncio.timeout(1):
                await asyncio.gather(
                    *(session.outgoing.join() for session in tuple(self.connections))
                )
        except TimeoutError:
            pass

    async def aclose(self) -> None:
        self.closed = True
        if self.listener is not None:
            self.listener.close()
            await self.listener.wait_closed()
        tasks = tuple(self.sessions)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
