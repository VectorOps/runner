"""Encrypted credential files with per-profile interprocess serialization."""

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
import secrets
import time
from typing import AsyncIterator, Optional

import pydantic
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from vectorops.runner import global_config as g
from vectorops.runner.authentication import models as m
from vectorops.runner.authentication.profiles import ProfileStore
from vectorops.runner.runner.resources import finish_cleanup


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

    async def check(self, profile: m.Profile) -> m.CredentialStatus:
        """Inspect one stored credential without refreshing or contacting its provider."""
        async with self.locked(profile):
            try:
                value = await self.load(profile)
            except m.CredentialStorageError:
                return m.CredentialStatus(
                    provider=profile.provider,
                    profile=profile.profile,
                    status="unreadable",
                )
            if value is None:
                return m.CredentialStatus(
                    provider=profile.provider, profile=profile.profile, status="missing"
                )
            return m.CredentialStatus.from_credential(value, time.time())

    async def listing(
        self, query: m.CredentialListQuery, profile: str
    ) -> m.CredentialPage:
        """Page stored identities, decrypting only the selected page for safe metadata."""

        def scan() -> list[m.Profile]:
            references = []
            try:
                for path in self.path.glob(f"*.{profile}.encrypted"):
                    provider, _, name = path.stem.partition(".")
                    try:
                        reference = m.Profile(provider=provider, profile=name)
                    except pydantic.ValidationError:
                        continue
                    if reference.profile == profile and (
                        query.after is None or reference.provider > query.after
                    ):
                        references.append(reference)
            except OSError:
                raise m.CredentialStorageError("cannot list credential store") from None
            return sorted(references, key=lambda item: item.provider)[: query.limit + 1]

        async with self.profiles.locked(profile):
            references = await asyncio.to_thread(scan)
            credentials = [
                await self.check(reference) for reference in references[: query.limit]
            ]
            return m.CredentialPage(
                profile=profile,
                credentials=credentials,
                next_after=(
                    references[query.limit - 1].provider
                    if len(references) > query.limit
                    else None
                ),
            )

    async def delete(self, profile: m.Profile) -> bool:
        """Remove a local credential under its profile lock, without remote revocation."""

        def remove() -> bool:
            try:
                (self.path / (profile.key + ".encrypted")).unlink()
                return True
            except FileNotFoundError:
                return False
            except OSError:
                raise m.CredentialStorageError(
                    "cannot delete credential store entry"
                ) from None

        async with self.locked(profile):
            return await finish_cleanup(asyncio.create_task(asyncio.to_thread(remove)))
