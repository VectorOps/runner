import asyncio
import json
from pathlib import Path
import secrets
import stat

import pytest

from vectorops.runner import global_config as g
from vectorops.runner.authentication import models as m
from vectorops.runner.authentication.store import CredentialStore


async def test_configuration_initialization_is_private_and_stable(
    tmp_path: Path,
) -> None:
    configuration = g.GlobalConfiguration(tmp_path / "global")
    values = await asyncio.gather(*(configuration.load() for _ in range(20)))
    assert all(value == values[0] for value in values)
    assert len(values[0].encryption_key) == 64
    assert len(values[0].control_token) >= 43
    assert values[0].control_token not in repr(values[0])
    assert values[0].encryption_key not in repr(values[0])
    assert stat.S_IMODE(configuration.path.stat().st_mode) == 0o700
    assert stat.S_IMODE((configuration.path / "secrets.json").stat().st_mode) == 0o600
    assert await g.GlobalConfiguration(configuration.path).load() == values[0]


async def test_predefined_token_and_malformed_configuration(tmp_path: Path) -> None:
    configuration = g.GlobalConfiguration(tmp_path)
    initial = await configuration.load()
    token = secrets.token_urlsafe(32)
    path = tmp_path / "secrets.json"
    path.write_text(
        initial.model_copy(update={"control_token": token}).model_dump_json()
    )
    assert (await configuration.load()).control_token == token
    path.write_text(json.dumps({"encryption_key": "private-invalid-value"}))
    with pytest.raises(g.ConfigurationError) as error:
        await configuration.load()
    assert "private-invalid-value" not in str(error.value)


async def test_encrypted_store_restart_and_missing_key(tmp_path: Path) -> None:
    configuration = g.GlobalConfiguration(tmp_path)
    store = CredentialStore(configuration)
    credential = m.Credential(
        provider="openai", profile="work", kind="api_key", secret="private-api-key"
    )
    await store.provision(credential)
    path = tmp_path / "credentials" / "openai.work.encrypted"
    assert credential.secret.encode() not in path.read_bytes()
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert (
        await CredentialStore(g.GlobalConfiguration(tmp_path)).load(credential)
        == credential
    )
    (tmp_path / "secrets.json").unlink()
    with pytest.raises(g.ConfigurationError, match="missing"):
        await configuration.load()
    assert not (tmp_path / "secrets.json").exists()


async def test_corrupt_store_and_wrong_key_are_not_missing_credentials(
    tmp_path: Path,
) -> None:
    configuration = g.GlobalConfiguration(tmp_path)
    store = CredentialStore(configuration)
    credential = m.Credential(provider="openai", kind="api_key", secret="private-key")
    await store.provision(credential)
    original = await configuration.load()
    (tmp_path / "secrets.json").write_text(
        original.model_copy(
            update={"encryption_key": secrets.token_hex(32)}
        ).model_dump_json()
    )
    with pytest.raises(m.CredentialStorageError, match="decrypt"):
        await store.load(credential)
    (tmp_path / "credentials" / "openai.default.encrypted").write_bytes(b"broken")
    with pytest.raises(m.CredentialStorageError, match="decrypt"):
        await store.load(credential)


async def test_separate_profiles_do_not_overwrite_each_other(tmp_path: Path) -> None:
    store = CredentialStore(g.GlobalConfiguration(tmp_path))
    values = [
        m.Credential(
            provider="openai", profile=str(index), kind="api_key", secret=f"key-{index}"
        )
        for index in range(10)
    ]
    await asyncio.gather(*(store.provision(value) for value in values))
    assert await asyncio.gather(*(store.load(value) for value in values)) == values


async def test_atomic_write_failure_preserves_previous_secret(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = CredentialStore(g.GlobalConfiguration(tmp_path))
    value = m.Credential(provider="openai", kind="api_key", secret="old")
    await store.provision(value)

    def fail(*args: object) -> None:
        raise OSError("private filesystem detail")

    monkeypatch.setattr(g.os, "replace", fail)
    with pytest.raises(m.CredentialStorageError, match="persist"):
        await store.provision(value.model_copy(update={"secret": "new"}))
    assert await store.load(value) == value
