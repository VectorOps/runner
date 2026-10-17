"""Translate environment cleanup reports into an awaitable resource finalizer."""

from .errors import EnvironmentError, ErrorCode
from .processes.manager import ProcessManager


async def close_processes(processes: ProcessManager) -> None:
    results = await processes.close()
    unconfirmed = [
        result.process_id for result in results if not result.termination_confirmed
    ]
    if unconfirmed:
        raise EnvironmentError(
            ErrorCode.IO,
            "process termination was not confirmed: " + ", ".join(unconfirmed),
            may_have_occurred=True,
        )
