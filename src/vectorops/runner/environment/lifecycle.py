"""Translate environment cleanup reports into an awaitable resource finalizer."""

from .errors import EnvironmentError, ErrorCode
from .protocol import TargetEnvironment


async def close_environment(environment: TargetEnvironment) -> None:
    results = await environment.close()
    unconfirmed = [
        result.process_id for result in results if not result.termination_confirmed
    ]
    if unconfirmed:
        raise EnvironmentError(
            ErrorCode.IO,
            "process termination was not confirmed: " + ", ".join(unconfirmed),
            may_have_occurred=True,
        )
