import asyncio
import os
from pathlib import Path
import signal
import sys

import pytest

from tests.fixtures import sources
from vectorops.runner.environment.configuration import LocalEnvironmentConfig
from vectorops.runner.environment.local import LocalEnvironment
from vectorops.runner.environment.processes import models as m

pytestmark = pytest.mark.skipif(
    os.name != "posix", reason="process groups require POSIX"
)


@pytest.mark.parametrize("kill_tree", [True, False])
async def test_parent_termination_selects_process_or_group(
    tmp_path: Path, kill_tree: bool
) -> None:
    environment = await LocalEnvironment.open(
        LocalEnvironmentConfig(
            base_project_path=str(tmp_path),
            processes=m.ProcessConfig(
                termination_grace_seconds=0.05, io_timeout_seconds=0.1
            ),
        )
    )
    heartbeat = tmp_path / "heartbeat"
    child_pid = None
    try:
        result = await environment.exec(
            m.ExecRequest(
                command=m.ArgvCommand(
                    argv=[
                        sys.executable,
                        "-u",
                        str(sources.path("process.py")),
                        "parent",
                        str(heartbeat),
                    ]
                ),
                kill_tree=kill_tree,
                yield_timeout_ms=0,
            )
        )
        data = b""
        async with asyncio.timeout(5):
            while b"\n" not in data:
                result = await environment.poll(
                    m.PollRequest(
                        process_id=result.process_id,
                        cursor=result.cursor,
                        wait_timeout_ms=100,
                    )
                )
                data += b"".join(chunk.data for chunk in result.chunks)
        child_pid = int(data.strip())
        report = await environment.kill(m.SessionRequest(process_id=result.process_id))
        assert report.scope == ("group" if kill_tree else "process")
        assert report.termination_confirmed
        if not kill_tree:
            os.kill(child_pid, 0)
        await asyncio.sleep(0.05)
        before = heartbeat.read_text()
        await asyncio.sleep(0.1)
        assert (heartbeat.read_text() == before) is kill_tree
    finally:
        if child_pid is not None:
            try:
                os.kill(child_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        await environment.close()
