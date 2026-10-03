"""Disposable foreground daemons with harness-provided endpoints and tokens."""

import asyncio
import json
import os
from pathlib import Path
import typing

import pydantic

from vectorops.runner.control import auth, client, protocol as p
from vectorops.runner.control.server import RunnerServer
from vectorops.runner.control.service import RunnerService


class Endpoint(p.Model):
    host: typing.Literal["127.0.0.1"] = "127.0.0.1"
    port: int = pydantic.Field(ge=1, le=65535)
    instance_id: str
    workspace: str
    pid: int


async def serve(workspace: Path, token: str, port: int = 0) -> None:
    token = auth.validate_token(token)
    service = RunnerService(workspace)
    async with service:
        server = RunnerServer(service, token)
        try:
            selected = await server.listen(port)
            endpoint = Endpoint(
                port=selected,
                instance_id=service.instance_id,
                workspace=str(service.path),
                pid=os.getpid(),
            )
            print(endpoint.model_dump_json(), flush=True)
            await service.shutdown_requested.wait()
            await server.finish_requests()
            await service.aclose()
            await server.flush_events()
        finally:
            await server.aclose()


async def dispatch(
    workspace: Path,
    command: str,
    *,
    token: str,
    port: int = 0,
    host: str = "127.0.0.1",
    mode: typing.Literal["cancel", "drain"] = "cancel",
) -> None:
    if command == "serve":
        await serve(workspace, token, port)
        return
    async with await client.RunnerClient.tcp(port, token, host=host) as connection:
        if command == "status":
            print(
                json.dumps(
                    {
                        "endpoint": {
                            "host": host,
                            "port": port,
                            "instance_id": connection.instance_id,
                        },
                        "runner": await connection.info(),
                        "current": (
                            p.json_value(current)
                            if (current := await connection.current())
                            else None
                        ),
                    }
                )
            )
        elif command == "stop":
            print(
                json.dumps(await connection.request("runner.shutdown", {"mode": mode}))
            )
        elif command == "connect":
            await connection.subscribe()
            print(json.dumps(await connection.info()), flush=True)
            print(
                json.dumps(
                    p.json_value(current)
                    if (current := await connection.current())
                    else None
                ),
                flush=True,
            )
            while True:
                event = await connection.next_event()
                print(event.model_dump_json(), flush=True)
        else:
            raise ValueError("unknown daemon command")
