"""Encrypted credential files with per-profile interprocess serialization."""

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
import secrets
from typing import AsyncIterator, Optional

import pydantic
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from vectorops.runner import global_config as g
from vectorops.runner.authentication import models as m
from vectorops.runner.authentication.profiles import ProfileStore


class CredentialStore:
    def __init__(self, configuration: g.GlobalConfiguration) -> None:
        self.configuration = configuration
        self.path = configuration.path / "credentials"
        self.profiles = ProfileStore(configuration)

    @asynccontextmanager
    async def locked(self, profile: m.Profile) -> AsyncIterator[None]:
        await self.configuration.load()
        await asyncio.to_thread(g.private_directory, self.path)
        async with (
            self.profiles.locked(profile.profile),
            g.async_file_lock(self.path / (profile.key + ".lock")),
        ):
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
            await self.profiles.ensure(value.profile)
            await self.save(value)

    async def publish(
        self, profile: m.Profile, issue: Callable[[], Awaitable[m.Credential]]
    ) -> m.Credential:
        """Finish issuance and publication before cancellation releases the caller's lock."""

        async def update() -> m.Credential:
            value = await issue()
            if value.key != profile.key:
                raise m.AuthenticationError("credential identity mismatch")
            await self.save(value)
            return value

        task = asyncio.create_task(update())
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise

    async def bindings(self, binding: m.Profile) -> list[m.Profile]:
        """Read non-secret profile bindings while holding the binding lock."""
        try:
            data = await asyncio.to_thread(
                (self.path / (binding.key + ".bindings")).read_bytes
            )
            return pydantic.TypeAdapter(list[m.Profile]).validate_json(data)
        except FileNotFoundError:
            return []
        except Exception:
            raise m.CredentialStorageError("cannot read credential bindings") from None

    async def remember(self, binding: m.Profile, profile: m.Profile) -> None:
        async with self.locked(binding):
            profiles = await self.bindings(binding)
            if any(value.key == profile.key for value in profiles):
                return
            profiles.append(profile)
            data = pydantic.TypeAdapter(list[m.Profile]).dump_json(profiles)
            task = asyncio.create_task(
                asyncio.to_thread(
                    g.atomic_write, self.path / (binding.key + ".bindings"), data
                )
            )
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                await task
                raise
            except Exception:
                raise m.CredentialStorageError(
                    "cannot persist credential bindings"
                ) from None

    async def delete(self, profile: m.Profile) -> None:
        """Remove a local credential under its profile lock, without remote revocation."""
        async with self.locked(profile):
            await asyncio.to_thread(
                (self.path / (profile.key + ".encrypted")).unlink, missing_ok=True
            )
