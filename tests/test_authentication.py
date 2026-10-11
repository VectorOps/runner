import asyncio
from pathlib import Path
import time
from urllib.parse import parse_qs, urlencode, urlsplit

import aiohttp
import pytest

from tests.fixtures import sources
from vectorops.runner import global_config as g
from vectorops.runner.authentication import models as m
from vectorops.runner.authentication.service import AuthenticationService
from vectorops.runner.control import client, oauth_host, protocol as p
from vectorops.runner.control.server import RunnerServer
from vectorops.runner.control.service import RunnerService


class OAuth:
    redirect_uri = "http://127.0.0.1:1455/auth/callback"

    def __init__(self) -> None:
        self.refreshes = 0
        self.exchanges = 0
        self.invalid = False
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.release.set()

    def authorization_url(self, challenge: str, state: str) -> str:
        return "https://example.test/authorize?" + urlencode(
            {"state": state, "code_challenge": challenge}
        )

    async def exchange(
        self, profile: m.Profile, code: str, verifier: str
    ) -> m.Credential:
        assert code == "private-code"
        assert len(verifier) >= 43
        self.exchanges += 1
        return m.Credential(
            provider=profile.provider,
            profile=profile.profile,
            kind="oauth2",
            secret="private-access",
            refresh_token="private-refresh",
            expires_at=time.time() + 3600,
            account_id="account",
        )

    async def refresh(self, credential: m.Credential) -> m.Credential:
        self.refreshes += 1
        self.started.set()
        await self.release.wait()
        if self.invalid:
            raise m.LoginRequired("authorization required")
        return credential.model_copy(
            update={
                "secret": "refreshed-access",
                "refresh_token": "rotated-refresh",
                "expires_at": time.time() + 3600,
            }
        )


async def pending(service: AuthenticationService) -> m.Requirement:
    async with asyncio.timeout(3):
        while not service.pending():
            await asyncio.sleep(0.01)
    return service.pending()[0]


def service(tmp_path: Path, adapter: OAuth) -> AuthenticationService:
    return AuthenticationService(
        g.GlobalConfiguration(tmp_path / "config"), adapters={"chatgpt": adapter}
    )


async def test_missing_api_key_fails_without_interaction(tmp_path: Path) -> None:
    auth = service(tmp_path, OAuth())
    with pytest.raises(m.AuthenticationError, match="not configured"):
        await auth.transport(None, "openai/gpt-test")
    assert auth.pending() == []


async def test_concurrent_rejected_tokens_refresh_once(tmp_path: Path) -> None:
    adapter = OAuth()
    auth = service(tmp_path, adapter)
    await auth.provision(
        m.Credential(
            provider="chatgpt",
            kind="oauth2",
            secret="rejected",
            refresh_token="refresh",
            expires_at=time.time() + 3600,
        )
    )
    transports = await asyncio.gather(
        *(auth.transport(None, "chatgpt/X") for _ in range(4))
    )
    assert adapter.refreshes == 0
    assert all(await asyncio.gather(*(item.refresh() for item in transports)))
    assert adapter.refreshes == 1
    assert all(item.value.secret == "refreshed-access" for item in transports)


async def test_api_key_header_and_profile_selection(tmp_path: Path) -> None:
    auth = service(tmp_path, OAuth())
    await auth.provision(
        m.Credential(
            provider="anthropic", profile="work", kind="api_key", secret="private-key"
        )
    )
    transport = await auth.transport("anthropic", "model", "work")
    assert (await transport.resolve()).headers == {"x-api-key": "private-key"}
    assert not await transport.refresh()
    with pytest.raises(m.AuthenticationError):
        await auth.transport("anthropic", "model")


async def test_refresh_is_shared_across_services_and_durable(tmp_path: Path) -> None:
    adapter = OAuth()
    first, second = service(tmp_path, adapter), service(tmp_path, adapter)
    value = m.Credential(
        provider="chatgpt",
        kind="oauth2",
        secret="old",
        refresh_token="old-refresh",
        expires_at=1,
    )
    await first.provision(value)
    results = await asyncio.gather(
        *(owner.acquire(m.Profile(provider="chatgpt")) for owner in [first, second] * 6)
    )
    assert adapter.refreshes == 1
    assert all(value.secret == "refreshed-access" for value in results)
    assert (await second.store.load(value)).refresh_token == "rotated-refresh"
    transport = await second.transport(None, "chatgpt/X")
    assert (await transport.resolve()).headers[
        "Authorization"
    ] == "Bearer refreshed-access"


async def test_cancelled_refresh_finishes_rotated_token_publication(
    tmp_path: Path,
) -> None:
    adapter = OAuth()
    adapter.release.clear()
    auth = service(tmp_path, adapter)
    value = m.Credential(
        provider="chatgpt",
        kind="oauth2",
        secret="old",
        refresh_token="old-refresh",
        expires_at=1,
    )
    await auth.provision(value)
    task = asyncio.create_task(auth.acquire(value))
    await adapter.started.wait()
    task.cancel()
    adapter.release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert (await auth.store.load(value)).refresh_token == "rotated-refresh"


async def test_oauth_wait_claim_disconnect_callback_and_restart(tmp_path: Path) -> None:
    adapter = OAuth()
    auth = service(tmp_path, adapter)
    tasks = [
        asyncio.create_task(auth.transport(None, "chatgpt/X", execution_id="execution"))
        for _ in range(3)
    ]
    requirement = await pending(auth)
    claim = auth.claim(requirement.id, "host-one")
    assert auth.claim(requirement.id, "host-one") == claim
    with pytest.raises(m.AuthenticationConflict):
        auth.claim(requirement.id, "host-two")
    auth.disconnect("host-one")
    with pytest.raises(m.AuthenticationConflict):
        auth.ready(m.Attempt(id=claim.id, attempt=claim.attempt), "host-one")
    claim = auth.claim(requirement.id, "host-two")
    assert parse_qs(
        urlsplit(
            auth.ready(m.Attempt(id=claim.id, attempt=claim.attempt), "host-two")
        ).query
    )["state"] == [claim.state]
    with pytest.raises(m.AuthenticationConflict, match="state"):
        await auth.complete(
            m.Callback(
                id=claim.id, attempt=claim.attempt, state="wrong", code="private-code"
            ),
            "host-two",
        )
    await auth.complete(
        m.Callback(
            id=claim.id, attempt=claim.attempt, state=claim.state, code="private-code"
        ),
        "host-two",
    )
    async with asyncio.timeout(3):
        results = await asyncio.gather(*tasks)
    assert adapter.exchanges == 1
    assert auth.pending() == []
    assert all(
        [
            (await result.resolve()).headers["chatgpt-account-id"] == "account"
            for result in results
        ]
    )
    restored = await service(tmp_path, adapter).transport(None, "chatgpt/X")
    assert (await restored.resolve()).headers[
        "Authorization"
    ] == "Bearer private-access"


async def test_expired_claim_and_explicit_cancellation(tmp_path: Path) -> None:
    auth = service(tmp_path, OAuth())
    task = asyncio.create_task(auth.acquire(m.Profile(provider="chatgpt")))
    requirement = await pending(auth)
    claim = auth.claim(requirement.id, "host")
    claim.expires_at = 0
    replacement = auth.claim(requirement.id, "replacement")
    assert replacement.attempt != claim.attempt
    auth.cancel(requirement.id)
    with pytest.raises(m.AuthenticationCancelled):
        await task
    assert auth.pending() == []


async def test_invalid_refresh_requires_login_without_refresh_loop(
    tmp_path: Path,
) -> None:
    adapter = OAuth()
    adapter.invalid = True
    auth = service(tmp_path, adapter)
    value = m.Credential(
        provider="chatgpt",
        kind="oauth2",
        secret="old",
        refresh_token="invalid",
        expires_at=1,
    )
    await auth.provision(value)
    task = asyncio.create_task(auth.acquire(value))
    requirement = await pending(auth)
    await asyncio.sleep(1.1)
    assert adapter.refreshes == 1
    auth.cancel(requirement.id)
    with pytest.raises(m.AuthenticationCancelled):
        await task


async def test_host_helper_runs_callback_on_host(
    tmp_path: Path, unused_tcp_port: int
) -> None:
    adapter = OAuth()
    adapter.redirect_uri = f"http://127.0.0.1:{unused_tcp_port}/auth/callback"
    async with RunnerService(tmp_path) as runner:
        runner.authentication.adapters["chatgpt"] = adapter
        token = (await runner.configuration.load()).control_token
        server = RunnerServer(runner, token)
        port = await server.listen()
        task = asyncio.create_task(runner.authentication.transport(None, "chatgpt/X"))
        try:
            requirement = await pending(runner.authentication)
            async with await client.RunnerClient.tcp(port) as host:

                async def launch(url: str) -> None:
                    state = parse_qs(urlsplit(url).query)["state"][0]
                    async with aiohttp.ClientSession() as http:
                        async with http.get(
                            adapter.redirect_uri,
                            params={"code": "private-code", "state": "wrong"},
                        ) as response:
                            assert response.status == 400
                        async with http.get(
                            adapter.redirect_uri,
                            params={"code": "private-code", "state": state},
                        ) as response:
                            assert response.status == 200

                await oauth_host.authenticate(host, requirement.id, launch=launch)
                assert (await task).value.secret == "private-access"
                with pytest.raises(p.RemoteError) as error:
                    await host.request(
                        "auth.provision",
                        {
                            "provider": "openai",
                            "kind": "api_key",
                            "secret": {"private": "never-echo"},
                        },
                    )
                assert "never-echo" not in str(error.value)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await server.aclose()


WORKFLOW = sources.read("authorize.py")


async def test_pending_workflow_recovers_and_completed_activity_replays(
    tmp_path: Path,
) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/job.py").write_text(WORKFLOW)
    async with RunnerService(tmp_path) as runner:
        runner.authentication.adapters["chatgpt"] = OAuth()
        accepted = p.Accepted.model_validate(
            await runner.dispatch(
                "executions.start",
                {"reference": "job:main", "idempotency_key": "auth"},
            )
        )
        await pending(runner.authentication)
    async with RunnerService(tmp_path) as runner:
        adapter = OAuth()
        runner.authentication.adapters["chatgpt"] = adapter
        requirement = await pending(runner.authentication)
        claim = runner.authentication.claim(requirement.id, "host")
        await runner.authentication.complete(
            m.Callback(
                id=claim.id,
                attempt=claim.attempt,
                state=claim.state,
                code="private-code",
            ),
            "host",
        )
        async with asyncio.timeout(3):
            while await runner.dispatch("executions.current", {}) is not None:
                await asyncio.sleep(0.01)
        assert (
            await runner.runner.execution_state(accepted.execution_id)
        ).status == "completed"
        assert await runner.runner.resume(accepted.execution_id) == "authorized"
        assert adapter.exchanges == 1
    for path in (tmp_path / "data").rglob("*"):
        if path.is_file():
            data = path.read_bytes()
            for secret in (b"private-access", b"private-refresh", b"private-code"):
                assert secret not in data
