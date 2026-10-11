import asyncio
import json
from pathlib import Path
import threading

import pytest

from vectorops.runner import cli, global_config
from vectorops.runner.authentication import models as m
from vectorops.runner.authentication.service import AuthenticationService
from vectorops.runner.control import client, commands, protocol


def service(path: Path) -> AuthenticationService:
    return AuthenticationService(global_config.GlobalConfiguration(path))


async def pending(auth: AuthenticationService) -> m.Requirement:
    while not auth.pending():
        await asyncio.sleep(0.01)
    return auth.pending()[0]


async def test_profile_management_persists_and_validates(tmp_path: Path) -> None:
    profiles = service(tmp_path).profiles
    assert await profiles.listing() == m.ProfileListing()
    assert (await profiles.add("work")).changed
    assert not (await profiles.add("work")).changed
    assert (await profiles.add("personal")).profiles == ["default", "personal", "work"]
    assert (await profiles.switch("work")).changed
    assert not (await profiles.switch("work")).changed
    assert (await service(tmp_path).profiles.listing()).active_profile == "work"
    assert (await profiles.delete("work")).active_profile == "default"
    assert not (await profiles.delete("work")).changed
    with pytest.raises(m.AuthenticationError, match="cannot be deleted"):
        await profiles.delete("default")
    with pytest.raises(m.AuthenticationError, match="does not exist"):
        await profiles.switch("missing")
    for name in ("", "../escape", "contains spaces", "x" * 65):
        with pytest.raises(ValueError):
            await profiles.add(name)


async def test_active_selection_explicit_override_and_inflight_pinning(
    tmp_path: Path,
) -> None:
    auth = service(tmp_path)
    await auth.provision(
        m.Credential(provider="openai", kind="api_key", secret="default-key")
    )
    original = await auth.transport("openai", "model")
    await auth.profiles.add("work")
    await auth.profiles.switch("work")
    with pytest.raises(m.AuthenticationError, match="not configured"):
        await auth.transport("openai", "model")
    await auth.provision(
        m.Credential(provider="openai", kind="api_key", secret="work-key")
    )
    selected = await auth.transport(None, "openai/model")
    assert selected.profile.profile == "work"
    assert (await selected.resolve()).headers == {"Authorization": "Bearer work-key"}
    assert (await original.resolve()).headers == {"Authorization": "Bearer default-key"}
    explicit = await auth.transport("openai", "model", "default")
    assert explicit.profile.profile == "default"
    restarted = await service(tmp_path).transport("openai", "model")
    assert restarted.value.secret == "work-key"


async def test_deletion_removes_all_provider_credentials_and_readd_is_empty(
    tmp_path: Path,
) -> None:
    auth = service(tmp_path)
    for provider in ("openai", "anthropic"):
        await auth.provision(
            m.Credential(
                provider=provider, profile="work", kind="api_key", secret="secret"
            )
        )
    assert (await auth.profiles.listing()).profiles == ["default", "work"]
    await auth.profiles.switch("work")
    await auth.profiles.delete("work")
    assert not list(auth.store.path.glob("*.work.encrypted"))
    with pytest.raises(m.AuthenticationError, match="deleted"):
        await auth.provision(
            m.Credential(
                provider="openai", profile="work", kind="api_key", secret="late-write"
            )
        )
    await auth.profiles.add("work")
    with pytest.raises(m.AuthenticationError, match="not configured"):
        await auth.transport("openai", "model", "work")


async def test_delete_waits_for_credential_publication(tmp_path: Path) -> None:
    auth = service(tmp_path)
    await auth.profiles.add("work")
    profile = m.Profile(provider="openai", profile="work")
    started, release = asyncio.Event(), asyncio.Event()

    async def writer() -> None:
        async with auth.store.locked(profile):
            started.set()
            await release.wait()
            await auth.store.save(
                m.Credential(
                    provider="openai",
                    profile="work",
                    kind="api_key",
                    secret="published",
                )
            )

    writing = asyncio.create_task(writer())
    await started.wait()
    deleting = asyncio.create_task(service(tmp_path).profiles.delete("work"))
    try:
        await asyncio.sleep(0.05)
        assert not deleting.done()
    finally:
        release.set()
        await asyncio.wait_for(asyncio.gather(writing, deleting), 3)
    assert not list(auth.store.path.glob("*.work.encrypted"))


async def test_concurrent_profile_updates_do_not_lose_names(tmp_path: Path) -> None:
    await asyncio.gather(
        *(service(tmp_path).profiles.add(f"profile{i}") for i in range(20))
    )
    assert set((await service(tmp_path).profiles.listing()).profiles) == {
        "default",
        *(f"profile{i}" for i in range(20)),
    }


async def test_failed_and_cancelled_profile_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    auth = service(tmp_path)
    await auth.profiles.add("work")
    original = global_config.atomic_write

    def fail(path: Path, data: bytes) -> None:
        raise OSError("failed")

    monkeypatch.setattr(global_config, "atomic_write", fail)
    with pytest.raises(m.CredentialStorageError):
        await auth.profiles.switch("work")
    assert (await auth.profiles.listing()).active_profile == "default"
    started = threading.Event()
    release = threading.Event()

    def blocked(path: Path, data: bytes) -> None:
        started.set()
        if not release.wait(3):
            raise TimeoutError("write not released")
        original(path, data)

    monkeypatch.setattr(global_config, "atomic_write", blocked)
    task = asyncio.create_task(auth.profiles.switch("work"))
    try:
        assert await asyncio.to_thread(started.wait, 3)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert (await service(tmp_path).profiles.listing()).active_profile == "work"


async def test_profile_rpc_and_shell(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config = tmp_path / "global"
    async with client.embedded(tmp_path, config_dir=config) as connection:
        assert await connection.authentication_profiles() == m.ProfileListing()
        for words in (
            ["auth", "profile"],
            ["auth", "profile", "add", "work"],
            ["auth", "profile", "switch", "work"],
        ):
            await commands.execute(
                connection,
                commands.parser().parse_args(words, namespace=commands.Arguments()),
            )
        assert "Active profile: work" in capsys.readouterr().out
        await connection.provision_credentials(
            m.Credential(provider="openai", kind="api_key", secret="private")
        )
        assert (
            await service(config).transport("openai", "model")
        ).value.secret == "private"
        await commands.execute(
            connection,
            commands.parser().parse_args(
                ["auth", "profile", "list", "--json"], namespace=commands.Arguments()
            ),
        )
        assert json.loads(capsys.readouterr().out) == {
            "active_profile": "work",
            "profiles": ["default", "work"],
        }
        with pytest.raises(protocol.RemoteError) as error:
            await connection.request("auth.profile.switch", {"name": "../invalid"})
        assert error.value.code == "invalid_argument"
        assert (
            await connection.delete_authentication_profile("work")
        ).active_profile == "default"


def test_local_cli_profiles(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    prefix = ["--config-dir", str(tmp_path), "auth", "profile"]
    assert cli.main([*prefix, "add", "work"]) == 0
    assert json.loads(capsys.readouterr().out)["changed"]
    assert cli.main([*prefix, "switch", "work"]) == 0
    assert json.loads(capsys.readouterr().out)["active_profile"] == "work"
    assert cli.main([*prefix, "list"]) == 0
    assert json.loads(capsys.readouterr().out)["profiles"] == ["default", "work"]
    assert cli.main([*prefix, "delete", "work"]) == 0
    assert json.loads(capsys.readouterr().out)["active_profile"] == "default"


async def test_explicit_login_stays_on_selected_profile(tmp_path: Path) -> None:
    auth = service(tmp_path)
    requirement = await auth.login("chatgpt")
    assert requirement.profile == "default"
    await auth.profiles.add("work")
    await auth.profiles.switch("work")
    assert auth.pending()[0].profile == "default"
    with pytest.raises(m.LoginRequired):
        await auth.transport(None, "chatgpt/model")
    assert len(auth.pending()) == 1


async def test_deleted_profile_rejects_login_completion(tmp_path: Path) -> None:
    auth = service(tmp_path)
    await auth.profiles.add("work")
    requirement = await auth.login("chatgpt", "work")
    claim = auth.claim(requirement.id, "host")
    await auth.profiles.delete("work")
    with pytest.raises(m.AuthenticationError, match="deleted"):
        await auth.complete(
            m.Callback(
                id=claim.id, attempt=claim.attempt, state=claim.state, code="code"
            ),
            "host",
        )
    auth.cancel(requirement.id)
    assert not auth.pending()
