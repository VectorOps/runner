import asyncio
from collections.abc import Mapping
from pathlib import Path
from typing import Optional
from unittest.mock import AsyncMock, patch

import pytest

from tests.runner.test_usage import generation, seed
from vectorops.runner import models
from vectorops.runner.runner import events, sql
from vectorops.runner.runner.cached_history import CachedHistory
from vectorops.runner.runner.session import ExecutionSession
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner.runner.usage_history import UsageHistory
from vectorops.runner.runner.usage_store import UsageStore
from vectorops.runner.runner.workspace_database import WorkspaceDatabase


async def unused_child(
    configuration: models.Workflow,
    *,
    continue_from_step_id: Optional[str] = None,
    child_heads: Optional[Mapping[str, str]] = None,
) -> models.WorkflowStep:
    raise AssertionError("unexpected child")


@pytest.mark.parametrize(
    "identifier", ["plain", "quoted'\";--", "unicode-\u03bb", "null\x00byte"]
)
async def test_stream_replacement_uses_bound_static_sql(
    tmp_path: Path, identifier: str
) -> None:
    async with WorkspaceDatabase(tmp_path / "workspace") as workspace:
        usage = UsageStore(workspace)
        async with SQLiteHistory(
            tmp_path / "execution/history.sqlite3", usage_store=usage
        ) as database:
            await seed(database)
            await database.save_workflow(
                models.Workflow(id=identifier, definition_ref="test")
            )
            step = generation(identifier, identifier, streaming=True)
            await database.append_step(step)
            changed = generation(identifier, identifier, streaming=True, outgoing=99)
            with patch.object(
                sql, "compile", side_effect=AssertionError("hot query compilation")
            ):
                await database.update_step(changed)
                assert await database.get_step(identifier) == changed
                revision, total = await database.usage_snapshot()
                await usage.record("quoted'\u03bb", revision, total)
                await database._invoke(
                    lambda connection: UsageHistory.ancestors(connection, identifier)
                )
                await database.update_steps([])
            assert (await usage.read()).totals.output_tokens == 198


async def test_session_replacements_are_isolated_without_deepcopy(
    tmp_path: Path,
) -> None:
    database = SQLiteHistory(tmp_path / "history.sqlite3")
    await seed(database)
    initial = generation("stream", streaming=True)
    initial = initial.model_copy(
        update={
            "operation": models.StepOperation(
                kind="llm.generate", name="test", input={"request": ["original"]}
            )
        }
    )
    captured: list[events.StepEvent] = []
    async with CachedHistory(database, write_through=True) as cache:
        await cache.append_step(initial)
        session = ExecutionSession(
            cache,
            await database.get_workflow("root"),
            unused_child,
            continue_from_step_id=None,
            child_heads={},
            event_sink=captured.append,
        )
        incoming = initial.model_copy(
            update={"outcome": models.StepStreaming(state={"text": ["first"]})}
        )
        with patch.object(
            models.WorkflowStep,
            "__deepcopy__",
            side_effect=AssertionError("hot snapshot deepcopy"),
        ):
            await session.update(incoming)
            await session.update(
                initial.model_copy(
                    update={"outcome": models.StepStreaming(state={"text": ["second"]})}
                )
            )
        assert [event.step.revision for event in captured] == [2, 3]
        assert captured[0].step.outcome == models.StepStreaming(
            state={"text": ["first"]}
        )
        assert isinstance(incoming.operation, models.StepOperation)
        incoming.operation.input["request"] = ["caller mutation"]
        assert isinstance(incoming.outcome, models.StepStreaming)
        assert isinstance(incoming.outcome.state, dict)
        incoming.outcome.state["text"] = ["caller mutation"]
        assert isinstance(captured[1].step.operation, models.StepOperation)
        captured[1].step.operation.input["request"] = ["consumer mutation"]
        captured[1].step.deliveries.append("consumer mutation")
        returned = await cache.get_step("stream")
        assert isinstance(returned.operation, models.StepOperation)
        assert returned.operation.input == {"request": ["original"]}
        assert returned.deliveries == []
        returned.operation.input.clear()
        assert await cache.get_step("stream") == await database.get_step("stream")


async def test_failed_and_concurrent_session_replacements(tmp_path: Path) -> None:
    database = SQLiteHistory(tmp_path / "history.sqlite3")
    await seed(database)
    initial = generation("stream", streaming=True)
    captured: list[events.StepEvent] = []
    async with CachedHistory(database, write_through=True) as cache:
        await cache.append_step(initial)
        session = ExecutionSession(
            cache,
            await database.get_workflow("root"),
            unused_child,
            continue_from_step_id=None,
            child_heads={},
            event_sink=captured.append,
        )
        with patch.object(
            database,
            "update_step",
            new=AsyncMock(side_effect=RuntimeError("write failed")),
        ):
            with pytest.raises(RuntimeError, match="write failed"):
                await session.update(generation("stream", streaming=True, outgoing=55))
        assert await cache.get_step("stream") == initial
        assert await database.get_step("stream") == initial
        assert captured == []
        await asyncio.gather(
            *(
                session.update(
                    generation("stream", streaming=True, outgoing=index + 20)
                )
                for index in range(8)
            )
        )
        assert [event.step.revision for event in captured] == list(range(2, 10))
        assert await cache.get_step("stream") == await database.get_step("stream")
