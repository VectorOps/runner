"""Optional host-side browser and loopback callback coordination."""

import asyncio
from collections.abc import Awaitable, Callable
import secrets
import time
from urllib.parse import urlsplit
import webbrowser

from aiohttp import web

from vectorops.runner.authentication import models as m
from vectorops.runner.control.client import RunnerClient


async def open_browser(url: str) -> None:
    if not await asyncio.to_thread(webbrowser.open, url):
        raise RuntimeError("could not open authorization browser")


async def authenticate(
    client: RunnerClient,
    identifier: str,
    *,
    launch: Callable[[str], Awaitable[None]] = open_browser,
) -> None:
    """Listen on the host, relay one callback, and never receive provider tokens."""
    claim = await client.claim_authentication(identifier)
    redirect = urlsplit(claim.redirect_uri)
    if (
        redirect.scheme != "http"
        or redirect.hostname not in ("localhost", "127.0.0.1", "::1")
        or not redirect.port
    ):
        raise ValueError("OAuth callback must use a host loopback HTTP listener")
    callback: asyncio.Future[m.Callback] = asyncio.get_running_loop().create_future()

    async def receive(request: web.Request) -> web.Response:
        if not secrets.compare_digest(request.query.get("state", ""), claim.state):
            return web.Response(status=400, text="Invalid OAuth state")
        if callback.done():
            return web.Response(status=409, text="Callback already received")
        code, error = request.query.get("code"), request.query.get("error")
        if not code and not error:
            return web.Response(status=400, text="Missing OAuth result")
        try:
            value = m.Callback(
                id=claim.id,
                attempt=claim.attempt,
                state=claim.state,
                code=code,
                error=error,
            )
        except ValueError:
            return web.Response(status=400, text="Invalid OAuth result")
        callback.set_result(value)
        return web.Response(
            text="Authorization received. Return to the host to check completion."
        )

    app = web.Application(client_max_size=32768)
    app.router.add_get(redirect.path, receive)
    server = web.AppRunner(app, access_log=None)
    disconnected = asyncio.create_task(client.closed.wait())
    try:
        await server.setup()
        await web.TCPSite(server, redirect.hostname, redirect.port).start()
        attempt = m.Attempt(id=claim.id, attempt=claim.attempt)
        async with asyncio.timeout(max(0, claim.expires_at - time.time())):
            await launch(await client.authentication_ready(attempt))
            await asyncio.wait(
                (callback, disconnected), return_when=asyncio.FIRST_COMPLETED
            )
            if disconnected.done():
                raise ConnectionError("runner disconnected during authentication")
            await client.complete_authentication(callback.result())
    except BaseException:
        if not client.closed.is_set():
            try:
                await client.request(
                    "auth.release", {"id": claim.id, "attempt": claim.attempt}
                )
            except Exception:
                pass
        raise
    finally:
        callback.cancel()
        disconnected.cancel()
        await asyncio.gather(disconnected, return_exceptions=True)
        await server.cleanup()
