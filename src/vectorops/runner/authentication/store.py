"""Encrypted credential files with per-profile interprocess serialization."""

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
import secrets
from typing import AsyncIterator, Optional

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from vectorops.runner import global_config as g
from vectorops.runner.authentication import models as m


class CredentialStore:
    def __init__(self, configuration: g.GlobalConfiguration) -> None:
        self.configuration = configuration
        self.path = configuration.path / "credentials"

    @asynccontextmanager
    async def locked(self, profile: m.Profile) -> AsyncIterator[None]:
        await self.configuration.load()
        await asyncio.to_thread(g.private_directory, self.path)
        async with g.async_file_lock(self.path / (profile.key + ".lock")):
            yield

    async def load(self, profile: m.Profile) -> Optional[m.Credential]:
        key = await self.configuration.load()
        path = self.path / (profile.key + ".encrypted")
        try:
            data = await asyncio.to_thread(path.read_bytes)
        except FileNotFoundError:
            return None
        except OSError:
            raise m.CredentialStorageError("cannot read credential store") from None
        try:
            plaintext = AESGCM(bytes.fromhex(key.encryption_key)).decrypt(
                data[:12], data[12:], profile.key.encode()
            )
            value = m.Credential.model_validate_json(plaintext)
            if value.key != profile.key:
                raise ValueError("credential identity mismatch")
            return value
        except Exception:
            raise m.CredentialStorageError("cannot decrypt credential store") from None

    async def save(self, value: m.Credential) -> None:
        """Call while holding the profile lock; finish publication before cancellation."""
        key = await self.configuration.load()
        nonce = secrets.token_bytes(12)
        data = nonce + AESGCM(bytes.fromhex(key.encryption_key)).encrypt(
            nonce, value.model_dump_json().encode(), value.key.encode()
        )
        task = asyncio.create_task(
            asyncio.to_thread(
                g.atomic_write, self.path / (value.key + ".encrypted"), data
            )
        )
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise
        except Exception:
            raise m.CredentialStorageError("cannot persist credential store") from None

    async def provision(self, value: m.Credential) -> None:
        async with self.locked(value):
            await self.save(value)
