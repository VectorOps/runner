"""User-owned runner secrets and private, crash-safe filesystem operations."""

import asyncio
from contextlib import asynccontextmanager, contextmanager
import fcntl
import os
from pathlib import Path
import secrets
import tempfile
from typing import AsyncIterator, Iterator, Optional

import pydantic


class ConfigurationError(RuntimeError):
    pass


class Secrets(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", hide_input_in_errors=True)

    version: int = pydantic.Field(default=1, ge=1, le=1)
    encryption_key: str = pydantic.Field(pattern=r"^[0-9a-f]{64}$", repr=False)
    control_token: str = pydantic.Field(
        min_length=43, max_length=4096, pattern=r"^[A-Za-z0-9_-]+$", repr=False
    )


def directory(path: Optional[Path] = None) -> Path:
    if path is not None:
        return path.expanduser().absolute()
    override = os.environ.get("VOPS_RUNNER_CONFIG_DIR")
    return (
        Path(override).expanduser().absolute()
        if override
        else Path.home() / ".config" / "vops-runner"
    )


def private_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path, 0o700)


def atomic_write(path: Path, data: bytes) -> None:
    """Publish a complete private file and sync both its contents and directory."""
    descriptor, temporary = tempfile.mkstemp(prefix=".write-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(data)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        Path(temporary).unlink(missing_ok=True)


@contextmanager
def file_lock(path: Path) -> Iterator[None]:
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        os.close(descriptor)


@asynccontextmanager
async def async_file_lock(path: Path, *, shared: bool = False) -> AsyncIterator[None]:
    opening = asyncio.create_task(
        asyncio.to_thread(os.open, path, os.O_CREAT | os.O_RDWR, 0o600)
    )
    try:
        descriptor = await asyncio.shield(opening)
    except asyncio.CancelledError:
        os.close(await opening)
        raise
    try:
        while True:
            try:
                mode = fcntl.LOCK_SH if shared else fcntl.LOCK_EX
                fcntl.flock(descriptor, mode | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                await asyncio.sleep(0.05)
        yield
    finally:
        os.close(descriptor)


class GlobalConfiguration:
    def __init__(self, path: Optional[Path] = None) -> None:
        self.path = directory(path)

    async def load(self) -> Secrets:
        return await asyncio.to_thread(self._load)

    def _load(self) -> Secrets:
        try:
            private_directory(self.path)
            with file_lock(self.path / ".configuration.lock"):
                path = self.path / "secrets.json"
                if path.exists():
                    os.chmod(path, 0o600)
                    return Secrets.model_validate_json(path.read_bytes())
                credentials = self.path / "credentials"
                if credentials.exists() and any(credentials.glob("*.encrypted")):
                    raise ConfigurationError("runner encryption key is missing")
                value = Secrets(
                    encryption_key=secrets.token_hex(32),
                    control_token=secrets.token_urlsafe(32),
                )
                atomic_write(path, value.model_dump_json().encode())
                return value
        except ConfigurationError:
            raise
        except Exception:
            raise ConfigurationError("cannot load runner global secrets") from None
