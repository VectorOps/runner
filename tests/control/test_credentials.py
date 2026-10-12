import asyncio
from pathlib import Path
import threading
from typing import Optional
from unittest.mock import AsyncMock

import pytest
import pydantic

from vectorops.runner.authentication import models as m
from vectorops.runner.authentication.service import AuthenticationService
from vectorops.runner.control import client, protocol, transport
from vectorops.runner.control.server import RunnerServer
from vectorops.runner.control.service import RunnerService


@pytest.mark.parametrize("tcp", [False, True])
async def test_credential_management_roundtrip(tmp_path: Path, tcp: bool) -> None:
    config = tmp_path / "global"
    async with RunnerService(tmp_path, config_dir=config) as service:
        token = (await service.configuration.load()).control_token
        server = RunnerServer(service, token)
        try:
            if tcp:
                connection = await client.RunnerClient.tcp(await server.listen(), token)
            else:
                host, worker = transport.memory_pair()
                server.attach(worker)
                connection = await client.RunnerClient.connect(host, token)
            async with connection:
                await connection.add_authentication_profile("work")
                await connection.switch_authentication_profile("work")
                assert (await connection.credentials()).credentials == []
                assert (await connection.check_credential("openai")).status == "missing"
                for provider in ("openai", "anthropic"):
                    await connection.provision_credentials(
                        m.Credential(
                            provider=provider, kind="api_key", secret="private-api-key"
                        )
                    )
                await connection.provision_credentials(
                    m.Credential(
                        provider="chatgpt",
                        kind="oauth2",
                        secret="private-access-token",
                        refresh_token="private-refresh-token",
                        expires_at=4102444800,
                        account_id="private-account",
                    )
                )
                await connection.provision_credentials(
                    m.Credential(
                        provider="openai",
                        profile="default",
                        kind="api_key",
                        secret="other-key",
                    )
                )
                first = await connection.credentials(limit=2)
                assert first.profile == "work"
                assert [item.provider for item in first.credentials] == [
                    "anthropic",
                    "chatgpt",
                ]
                assert first.next_after == "chatgpt"
                last = await connection.credentials(after=first.next_after, limit=2)
                assert [item.provider for item in last.credentials] == ["openai"]
                assert last.next_after is None
                status = await connection.check_credential("chatgpt")
                assert status.kind == "oauth2" and status.status == "present"
                assert status.has_refresh_token
                for payload in (
                    first.model_dump_json(),
                    last.model_dump_json(),
                    status.model_dump_json(),
                ):
                    for secret in (
                        "private-api-key",
                        "private-access-token",
                        "private-refresh-token",
                        "private-account",
                        "other-key",
                    ):
                        assert secret not in payload
                await connection.provision_credentials(
                    m.Credential(
                        provider="openai",
                        profile="work",
                        kind="api_key",
                        secret="replacement",
                    )
                )
                stored = await service.authentication.store.load(
                    m.Profile(provider="openai", profile="work")
                )
                assert stored is not None and stored.secret == "replacement"
                assert (await connection.delete_credential("openai")).deleted
                assert not (await connection.delete_credential("openai")).deleted
                assert (await connection.check_credential("openai")).status == "missing"
                assert (
                    await connection.check_credential("openai", "default")
                ).status == "present"
                await connection.delete_authentication_profile("work")
                assert not list((config / "credentials").glob("*.work.encrypted"))
                assert (await connection.credentials()).profile == "default"
                assert len((await connection.credentials()).credentials) == 1
                invalid_requests: list[tuple[str, dict[str, pydantic.JsonValue]]] = [
                    ("auth.credentials.list", {"limit": 501}),
                    ("auth.credentials.check", {"provider": "../private-secret"}),
                    (
                        "auth.credentials.delete",
                        {"provider": "openai", "profile": "../private-secret"},
                    ),
                ]
                for method, params in invalid_requests:
                    with pytest.raises(protocol.RemoteError) as error:
                        await connection.request(method, params)
                    assert error.value.code == "invalid_argument"
                    assert "private-secret" not in str(error.value)
                assert not service.authentication.pending()
                assert (await connection.active()).executions == []
        finally:
            await server.aclose()
    async with client.embedded(tmp_path, config_dir=config) as reopened:
        assert (await reopened.check_credential("openai")).status == "present"
        assert (await reopened.credentials()).credentials[0].provider == "openai"


@pytest.mark.parametrize(
    "kind,expires,status",
    [
        ("api_key", None, "present"),
        ("api_key", 100, "expired"),
        ("api_key", 101, "present"),
        ("oauth2", None, "unknown_expiry"),
        ("oauth2", 99, "expired"),
        ("oauth2", 100, "expired"),
        ("oauth2", 160, "expiring"),
        ("oauth2", 161, "present"),
    ],
)
async def test_checks_are_local_only(
    kind: str, expires: Optional[float], status: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    auth = AuthenticationService()
    value = m.Credential.model_validate(
        {
            "provider": "chatgpt",
            "kind": kind,
            "secret": "private",
            "expires_at": expires,
            "refresh_token": "refresh" if kind == "oauth2" else None,
        }
    )
    await auth.provision(value)
    forbidden = AsyncMock(side_effect=AssertionError("check attempted refresh"))
    monkeypatch.setattr(auth.adapters["chatgpt"], "refresh", forbidden)
    monkeypatch.setattr("vectorops.runner.authentication.store.time.time", lambda: 100)
    result = await auth.check_credential(m.CredentialQuery(provider="chatgpt"))
    assert result.status == status
    assert result.has_refresh_token == (kind == "oauth2")
    forbidden.assert_not_awaited()
    assert not auth.pending()


async def test_unreadable_entry_can_be_listed_replaced_and_deleted(
    tmp_path: Path,
) -> None:
    async with RunnerService(tmp_path) as runner:
        auth = runner.authentication
        value = m.Credential(provider="openai", kind="api_key", secret="private-key")
        await auth.provision(value)
        path = auth.store.path / "openai.default.encrypted"
        path.write_bytes(b"private-corrupt-payload")
        checked = await runner.dispatch(
            "auth.credentials.check", {"provider": "openai"}
        )
        assert isinstance(checked, dict) and checked["status"] == "unreadable"
        assert checked["kind"] is None
        assert "private-corrupt-payload" not in str(checked)
        page = await auth.credentials(m.CredentialListQuery())
        assert page.credentials[0].status == "unreadable"
        await auth.provision(value)
        assert (
            await auth.check_credential(m.CredentialQuery(provider="openai"))
        ).status == "present"
        path.write_bytes(b"corrupt-again")
        assert (
            await auth.delete_credential(m.CredentialQuery(provider="openai"))
        ).deleted


async def test_cancelled_delete_finishes_before_releasing_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    auth = AuthenticationService()
    value = m.Credential(provider="openai", kind="api_key", secret="old")
    await auth.provision(value)
    target = auth.store.path / "openai.default.encrypted"
    started, release = threading.Event(), threading.Event()
    original = Path.unlink

    def unlink(path: Path, missing_ok: bool = False) -> None:
        if path == target:
            started.set()
            if not release.wait(5):
                raise TimeoutError("delete not released")
        original(path, missing_ok=missing_ok)

    monkeypatch.setattr(Path, "unlink", unlink)
    deleting = asyncio.create_task(
        auth.delete_credential(m.CredentialQuery(provider="openai"))
    )
    try:
        assert await asyncio.to_thread(started.wait, 5)
        deleting.cancel()
        await asyncio.sleep(0)
        assert not deleting.done()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await deleting
    await auth.provision(value.model_copy(update={"secret": "new"}))
    stored = await auth.store.load(value)
    assert stored is not None and stored.secret == "new"
