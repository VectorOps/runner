"""Bearer authentication before operational packet processing."""

import asyncio
import hmac
from uuid import uuid4

from vectorops.runner.control import protocol as p
from vectorops.runner.control.transport import Connection


def validate_token(token: str) -> str:
    try:
        return p.Authenticate(token=token).token
    except ValueError:
        raise ValueError(
            "token must contain 43-4096 URL-safe characters; generate it from at least 32 random bytes"
        ) from None


async def accept(connection: Connection, token: str, instance_id: str) -> None:
    async with asyncio.timeout(p.HANDSHAKE_TIMEOUT):
        authentication = await connection.receive()
        if not isinstance(authentication, p.Authenticate) or not hmac.compare_digest(
            authentication.token, token
        ):
            raise p.RemoteError("authentication_failed", "authentication failed")
        session_id = uuid4().hex
        await connection.send(
            p.Authenticated(
                session_id=session_id,
                instance_id=instance_id,
            )
        )


async def connect(connection: Connection, token: str) -> str:
    token = validate_token(token)
    async with asyncio.timeout(p.HANDSHAKE_TIMEOUT):
        await connection.send(p.Authenticate(token=token))
        try:
            result = await connection.receive()
        except (ValueError, ConnectionError, asyncio.IncompleteReadError):
            raise p.RemoteError(
                "authentication_failed", "runner rejected authentication"
            ) from None
        if not isinstance(result, p.Authenticated):
            raise p.RemoteError(
                "authentication_failed", "expected authentication acknowledgement"
            )
        return result.instance_id
