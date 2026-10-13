import asyncio
from collections.abc import Iterator
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock, patch

import connect
import pydantic
import pytest

from tests.runner.test_mailbox import answer, store
from vectorops.runner import models
from vectorops.runner.api import chat_input, user_input
from vectorops.runner.authentication.service import AuthenticationService
from vectorops.runner.runner import mailbox
from vectorops.runner.runner.cached_history import CachedHistory
from vectorops.runner.runner.llm import RunnerLLM
from vectorops.runner.runner.naming import WorkflowNaming
from vectorops.runner.runner.project import RunnerProject
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner.runner.workflow import WorkflowRunner


@pytest.fixture(autouse=True)
def cleanup_patches() -> Iterator[None]:
    yield
    patch.stopall()


def content(text: str) -> pydantic.JsonValue:
    return {"content": [{"type": "text", "text": text}]}


async def naming(path: Path) -> WorkflowNaming:
    project = await RunnerProject.open(path)
    llm = RunnerLLM(project.configuration, AuthenticationService())
    patch.object(
        llm,
        "generate",
        new=AsyncMock(
            return_value=connect.AssistantMessage(
                content=[connect.TextBlock(text='  "Fix   failing tests"  ')],
                finish_reason="stop",
            )
        ),
    ).start()
    return WorkflowNaming(llm)


async def test_name_survives_restart_without_steps_and_preserves_configuration(
    tmp_path: Path,
) -> None:
    database = await store(tmp_path / "history.sqlite3")
    original = await database.get_workflow("workflow")
    await database.register_input(
        "request",
        user_input.input_spec(chat_input.UserMessageRequest(prompt="Message?")),
    )
    await database.answer_input(answer(content("Fix my failing tests")))
    owner = await naming(tmp_path)
    async with CachedHistory(database, cache_workflows=False) as cache:
        assert (await cache.get_workflow("workflow")).name is None
        await asyncio.gather(
            *(owner.ensure_name(database, "workflow") for _ in range(3))
        )
        assert (await cache.get_workflow("workflow")).name == "Fix failing tests"
        await cache.save_workflow(original)
    reopened = SQLiteHistory(database.path)
    assert (await reopened.get_workflow("workflow")).name == "Fix failing tests"
    cast(AsyncMock, owner.llm.generate).assert_awaited_once()
    await (await naming(tmp_path)).ensure_name(reopened, "workflow")
    assert (await reopened.inspect_execution()).stats.steps == 1
    assert (await reopened.execution_summary()).name == "Fix failing tests"
    with pytest.raises(ValueError, match="configuration conflict"):
        await reopened.save_workflow(
            original.model_copy(update={"input": {"changed": True}})
        )
    assert (
        await reopened.set_workflow_name("workflow", "Replacement")
    ).name == "Fix failing tests"


async def test_only_first_typed_prompt_is_used_after_failure(tmp_path: Path) -> None:
    database = await store(tmp_path / "history.sqlite3")
    spec = user_input.input_spec(chat_input.UserMessageRequest(prompt="Message?"))
    await database.register_input("request", spec)
    await database.answer_input(answer(content("Original topic")))
    owner = await naming(tmp_path)
    cast(AsyncMock, owner.llm.generate).side_effect = RuntimeError("offline")
    await owner.ensure_name(database, "workflow")
    assert (await database.get_workflow("workflow")).name is None
    await database.update_step(
        (await database.get_step("request")).model_copy(
            update={"outcome": models.StepCompleted()}
        )
    )
    await database.append_step(
        models.WorkflowStep(
            id="second",
            workflow_id="workflow",
            previous_step_id="request",
            operation=models.StepOperation(kind=mailbox.KIND, name="request_input"),
            outcome=models.StepPending(),
        )
    )
    await database.register_input("second", spec)
    await database.answer_input(
        mailbox.Answer(
            execution_id="execution",
            request_id="second",
            response=content("Different topic"),
            idempotency_key="second",
        )
    )
    cast(AsyncMock, owner.llm.generate).side_effect = None
    cast(AsyncMock, owner.llm.generate).return_value = connect.AssistantMessage(
        content=[connect.TextBlock(text="Original topic")], finish_reason="stop"
    )
    await owner.ensure_name(database, "workflow")
    message = cast(AsyncMock, owner.llm.generate).call_args.args[0]
    assert message.content[0].text == "Original topic"


async def test_non_chat_inputs_do_not_generate_names(tmp_path: Path) -> None:
    database = await store(tmp_path / "history.sqlite3")
    await database.register_input("request", mailbox.InputSpec(channel="approval"))
    await database.answer_input(answer())
    owner = await naming(tmp_path)
    await owner.ensure_name(database, "workflow")
    cast(AsyncMock, owner.llm.generate).assert_not_awaited()


async def test_runner_names_after_accepting_input(tmp_path: Path) -> None:
    project = await RunnerProject.open(tmp_path)
    path = project.data_path / "executions" / "execution" / "history.sqlite3"
    database = await store(path)
    await database.register_input(
        "request",
        user_input.input_spec(chat_input.UserMessageRequest(prompt="Message?")),
    )
    runner = WorkflowRunner(tmp_path)
    runner._naming = await naming(tmp_path)
    accepted = await runner.answer_input(answer(content("Help me fix tests")))
    assert accepted.status == "answered"
    assert (await database.get_workflow("workflow")).name == "Fix failing tests"


@pytest.mark.parametrize("name", ["", "x" * 121])
def test_invalid_workflow_name(name: str) -> None:
    with pytest.raises(ValueError):
        models.Workflow(id="workflow", definition_ref="test:workflow", name=name)


@pytest.mark.parametrize("text", ["", " \n\t", '""'])
async def test_empty_titles_are_not_persisted(tmp_path: Path, text: str) -> None:
    database = await store(tmp_path / "history.sqlite3")
    await database.register_input(
        "request",
        user_input.input_spec(chat_input.UserMessageRequest(prompt="Message?")),
    )
    await database.answer_input(answer(content("Hello")))
    owner = await naming(tmp_path)
    cast(AsyncMock, owner.llm.generate).return_value = connect.AssistantMessage(
        content=[connect.TextBlock(text=text)], finish_reason="stop"
    )
    await owner.ensure_name(database, "workflow")
    assert (await database.get_workflow("workflow")).name is None


async def test_naming_cancellation_propagates_and_can_recover(tmp_path: Path) -> None:
    database = await store(tmp_path / "history.sqlite3")
    await database.register_input(
        "request",
        user_input.input_spec(chat_input.UserMessageRequest(prompt="Message?")),
    )
    await database.answer_input(answer(content("Hello")))
    owner = await naming(tmp_path)
    cast(AsyncMock, owner.llm.generate).side_effect = asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError):
        await owner.ensure_name(database, "workflow")
    assert (await database.get_workflow("workflow")).name is None
    reopened = SQLiteHistory(database.path)
    await (await naming(tmp_path)).ensure_name(reopened, "workflow")
    assert (await reopened.get_workflow("workflow")).name == "Fix failing tests"
