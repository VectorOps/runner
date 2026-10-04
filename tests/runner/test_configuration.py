import asyncio
from pathlib import Path

from vectorops.runner.control import client, protocol as p
from vectorops.runner.runner import mailbox
from vectorops.runner.runner.workflow import WorkflowRunner


async def test_completed_children_replay_old_settings(workspace: Path) -> None:
    config = workspace / "config.toml"
    config.write_text('[components.llm]\nmodel = "first"\n')
    runner = WorkflowRunner(workspace)
    result = await runner.run("configuration:children")
    config.write_text('[components.llm]\nmodel = "second"\n')
    assert await WorkflowRunner(workspace).resume() == result
    updated = await WorkflowRunner(workspace).run("configuration:main")
    assert isinstance(updated, dict) and updated["model"] == "second"


async def test_resume_replays_old_read_then_reads_fresh_settings(
    workspace: Path,
) -> None:
    path = workspace / "config.toml"
    path.write_text('[components.llm]\nmodel = "first"\n')
    async with client.embedded(workspace) as host:
        accepted = await host.start(
            p.Start(
                reference="configuration:changing",
                idempotency_key="config",
            )
        )
        async with asyncio.timeout(5):
            while True:
                try:
                    pending = await host.pending_inputs(
                        mailbox.RequestQuery(execution_id=accepted.execution_id)
                    )
                except p.RemoteError as exc:
                    if exc.code != "not_found":
                        raise
                    await asyncio.sleep(0.01)
                    continue
                if pending.requests:
                    break
                await asyncio.sleep(0.01)
    path.write_text('[components.llm]\nmodel = "second"\n')
    async with client.embedded(workspace) as host:
        await host.answer_input(
            mailbox.Answer(
                execution_id=accepted.execution_id,
                request_id=pending.requests[0].request_id,
                response="yes",
                idempotency_key="answer",
            )
        )
        async with asyncio.timeout(5):
            while await host.current() is not None:
                await asyncio.sleep(0.01)
    assert await WorkflowRunner(workspace).resume(accepted.execution_id) == [
        "first",
        "second",
    ]
