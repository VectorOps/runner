"""Global profile selection and deletion, independent of provider authentication."""

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Optional

import pydantic

from vectorops.runner import global_config as g
from vectorops.runner.authentication import models as m
from vectorops.runner.runner.resources import finish_cleanup


class _Catalog(m.ProfileListing):
    deleted: list[m.ProfileName] = pydantic.Field(default_factory=list)

    @pydantic.model_validator(mode="after")
    def valid_deleted(self) -> "_Catalog":
        if set(self.deleted).intersection(self.profiles):
            raise ValueError("deleted profiles cannot be active")
        return self


class ProfileStore:
    def __init__(self, configuration: g.GlobalConfiguration) -> None:
        self.configuration = configuration
        self.path = configuration.path / "profiles.json"

    def _load(self) -> _Catalog:
        try:
            return _Catalog.model_validate_json(self.path.read_bytes())
        except FileNotFoundError:
            return _Catalog()
        except Exception:
            raise m.CredentialStorageError(
                "cannot read authentication profiles"
            ) from None

    def _save(self, catalog: _Catalog) -> None:
        try:
            g.atomic_write(self.path, catalog.model_dump_json().encode())
        except Exception:
            raise m.CredentialStorageError(
                "cannot persist authentication profiles"
            ) from None

    async def _update(self, change: Callable[[_Catalog], bool]) -> m.ProfileResult:
        await self.configuration.load()

        def update() -> m.ProfileResult:
            with g.file_lock(self.path.with_suffix(".lock")):
                catalog = self._load()
                changed = change(catalog)
                if changed:
                    self._save(catalog)
                return m.ProfileResult(
                    active_profile=catalog.active_profile,
                    profiles=["default", *sorted(set(catalog.profiles) - {"default"})],
                    changed=changed,
                )

        return await finish_cleanup(asyncio.create_task(asyncio.to_thread(update)))

    async def listing(self) -> m.ProfileListing:
        result = await self._update(lambda catalog: False)
        return m.ProfileListing(
            active_profile=result.active_profile, profiles=result.profiles
        )

    async def resolve(self, name: Optional[str]) -> str:
        listing = await self.listing()
        if name is None:
            return listing.active_profile
        name = m.ProfileQuery(name=name).name
        if name not in listing.profiles:
            raise m.AuthenticationError(
                "authentication profile does not exist; add it first"
            )
        return name

    @asynccontextmanager
    async def locked(self, name: str) -> AsyncIterator[None]:
        name = m.ProfileQuery(name=name).name
        await self.configuration.load()
        async with g.async_file_lock(
            self.configuration.path / f".profile-{name}.lock", shared=True
        ):
            catalog = await asyncio.to_thread(self._load)
            if name in catalog.deleted:
                raise m.AuthenticationError("authentication profile was deleted")
            yield

    async def ensure(self, name: str) -> None:
        """Register provisioned credentials without resurrecting a deleted profile."""
        name = m.ProfileQuery(name=name).name

        def change(catalog: _Catalog) -> bool:
            if name in catalog.deleted:
                raise m.AuthenticationError(
                    "authentication profile was deleted; add it first"
                )
            if name in catalog.profiles:
                return False
            catalog.profiles.append(name)
            return True

        await self._update(change)

    def _purge(self, name: str) -> None:
        directory = self.configuration.path / "credentials"
        for suffix in ("encrypted", "bindings"):
            for path in directory.glob(f"*.{name}.{suffix}"):
                path.unlink(missing_ok=True)

    async def add(self, name: str) -> m.ProfileResult:
        name = m.ProfileQuery(name=name).name
        await self.configuration.load()
        async with g.async_file_lock(self.configuration.path / f".profile-{name}.lock"):

            def change(catalog: _Catalog) -> bool:
                if name in catalog.profiles:
                    return False
                if name in catalog.deleted:
                    self._purge(name)
                    catalog.deleted.remove(name)
                catalog.profiles.append(name)
                return True

            return await self._update(change)

    async def switch(self, name: str) -> m.ProfileResult:
        name = m.ProfileQuery(name=name).name

        def change(catalog: _Catalog) -> bool:
            if name not in catalog.profiles:
                raise m.AuthenticationError(
                    "authentication profile does not exist; add it first"
                )
            if name == catalog.active_profile:
                return False
            catalog.active_profile = name
            return True

        return await self._update(change)

    async def delete(self, name: str) -> m.ProfileResult:
        """Disable the profile before removing files so failed cleanup cannot expose secrets."""
        name = m.ProfileQuery(name=name).name
        if name == "default":
            raise m.AuthenticationError(
                "default authentication profile cannot be deleted"
            )
        await self.configuration.load()
        async with g.async_file_lock(self.configuration.path / f".profile-{name}.lock"):

            def change(catalog: _Catalog) -> bool:
                if name not in catalog.profiles:
                    return False
                catalog.profiles.remove(name)
                catalog.deleted.append(name)
                if catalog.active_profile == name:
                    catalog.active_profile = "default"
                return True

            async def remove() -> m.ProfileResult:
                result = await self._update(change)
                if name in (await asyncio.to_thread(self._load)).deleted:
                    try:
                        await asyncio.to_thread(self._purge, name)
                    except OSError:
                        raise m.CredentialStorageError(
                            "cannot remove profile credentials"
                        ) from None
                return result

            return await finish_cleanup(asyncio.create_task(remove()))
