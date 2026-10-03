"""Read harness-owned bearer tokens without persisting or displaying them."""

import asyncio
from pathlib import Path
import sys
import typing

from vectorops.runner.control.auth import validate_token


def _decode(data: bytes) -> str:
    if data.endswith(b"\n"):
        data = data[:-1]
        if data.endswith(b"\r"):
            data = data[:-1]
    try:
        token = data.decode("ascii")
    except UnicodeDecodeError:
        raise ValueError("token must be URL-safe ASCII text") from None
    return validate_token(token)


async def read_token(
    *, token_stdin: bool = False, token_file: typing.Optional[Path] = None
) -> str:
    """Read one bounded stdin line or an entire bounded token file exactly once."""
    if token_stdin == (token_file is not None):
        raise ValueError("specify exactly one of --token-stdin or --token-file")

    def read() -> str:
        if token_file is not None:
            with token_file.open("rb") as stream:
                return _decode(stream.read(4099))
        if sys.stdin.isatty():
            raise ValueError(
                "--token-stdin requires a pipe or redirected file, not a terminal"
            )
        return _decode(sys.stdin.buffer.readline(4099))

    return await asyncio.to_thread(read)
