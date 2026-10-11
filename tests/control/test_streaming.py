import asyncio
from pathlib import Path

from tests.fixtures import sources
from vectorops.runner import models
from vectorops.runner.control import client, protocol as p
from vectorops.runner.runner import history_query as h


async def test_stream_revisions_match_persisted_snapshots(tmp_path: Path) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/stream.py").write_text(sources.read("stream.py"))
    async with client.embedded(tmp_path) as connection:
        await connection.subscribe()
        accepted = await connection.start(
            p.Start(reference="stream:main", idempotency_key="stream")
        )
        steps: dict[str, list[models.WorkflowStep]] = {}
        async with asyncio.timeout(5):
            while True:
                event = (await connection.next_event()).event
                if event["kind"] == "execution.completed":
                    break
                assert event["kind"] != "execution.failed", event
                if "step" in event:
                    step = models.WorkflowStep.model_validate(event["step"])
                    steps.setdefault(step.id, []).append(step)
        page = await connection.history(
            h.HistoryQuery(execution_id=accepted.execution_id)
        )
        assert page.items
        for item in page.items:
            assert item.step == steps[item.step.id][-1]
            revisions = [step.revision for step in steps[item.step.id]]
            assert revisions == list(range(1, len(revisions) + 1))
        assert any(len(updates) >= 4 for updates in steps.values())


async def test_resume_and_filters(tmp_path: Path) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/job.py").write_text(sources.read("answer.py"))
    async with client.embedded(tmp_path) as connection:
        subscription = await connection.subscribe(
            p.Subscribe(kinds=["execution.completed"])
        )
        accepted = await connection.start(
            p.Start(reference="job:main", idempotency_key="start")
        )
        async with asyncio.timeout(5):
            assert (await connection.next_event()).event[
                "kind"
            ] == "execution.completed"
        resumed = await connection.resume(
            p.Resume(execution_id=accepted.execution_id, idempotency_key="resume")
        )
        assert resumed.execution_id == accepted.execution_id
        async with asyncio.timeout(5):
            assert (await connection.next_event()).event[
                "kind"
            ] == "execution.completed"
        await connection.unsubscribe(subscription)


async def test_shutdown_cancels_active_execution(tmp_path: Path) -> None:
    (tmp_path / "workflows").mkdir()
    (tmp_path / "workflows/wait.py").write_text(sources.read("wait.py"))
    async with asyncio.timeout(5):
        async with client.embedded(tmp_path) as connection:
            await connection.start(
                p.Start(reference="wait:main", idempotency_key="start")
            )
