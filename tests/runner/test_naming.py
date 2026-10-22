import asyncio
from collections.abc import Iterator
from pathlib import Path
from typing import Optional, cast
from unittest.mock import AsyncMock, patch

import connect
import pydantic
import pytest

from tests.control.test_client_commands import pending, settled
from tests.fixtures.user_input import reply
from tests.runner.test_mailbox import answer, store
from vectorops.runner import models
from vectorops.runner.api import chat_input, user_input
from vectorops.runner.authentication.service import AuthenticationService
from vectorops.runner.control import client, protocol
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


@pytest.mark.parametrize("workflow_name", [None, "Customer workflow", "Legacy title"])
async def test_description_survives_restart_without_steps_and_preserves_name(
    tmp_path: Path,
    workflow_name: Optional[str],
) -> None:
    database = await store(tmp_path / "history.sqlite3")
    if workflow_name is not None:
        await database.set_workflow_name("workflow", workflow_name)
    original = await database.get_workflow("workflow")
    await database.register_input(
        "request",
        user_input.input_spec(chat_input.UserMessageRequest(prompt="Message?")),
    )
    await database.answer_input(answer(content("Fix my failing tests")))
    owner = await naming(tmp_path)
    async with CachedHistory(database, cache_workflows=False) as cache:
        assert (await cache.get_workflow("workflow")).description is None
        await asyncio.gather(
            *(owner.ensure_name(database, "workflow") for _ in range(3))
        )
        assert (await cache.get_workflow("workflow")).description == "Fix failing tests"
        await cache.save_workflow(original)
    reopened = SQLiteHistory(database.path)
    saved = await reopened.get_workflow("workflow")
    assert saved.description == "Fix failing tests"
    assert saved.name == workflow_name
    cast(AsyncMock, owner.llm.generate).assert_awaited_once()
    arguments = cast(AsyncMock, owner.llm.generate).call_args.kwargs
    assert arguments["max_output_tokens"] is None
    assert "at most 8 words" in arguments["system_prompt"]
    await (await naming(tmp_path)).ensure_name(reopened, "workflow")
    assert (await reopened.inspect_execution()).stats.steps == 1
    summary = await reopened.execution_summary()
    assert summary.description == "Fix failing tests"
    assert summary.name == workflow_name
    with pytest.raises(ValueError, match="configuration conflict"):
        await reopened.save_workflow(
            original.model_copy(update={"input": {"changed": True}})
        )
    assert (
        await reopened.set_workflow_description("workflow", "Replacement")
    ).description == "Fix failing tests"


async def test_only_first_typed_prompt_is_used_after_failure(tmp_path: Path) -> None:
    database = await store(tmp_path / "history.sqlite3")
    spec = user_input.input_spec(chat_input.UserMessageRequest(prompt="Message?"))
    await database.register_input("request", spec)
    await database.answer_input(answer(content("Original topic")))
    owner = await naming(tmp_path)
    cast(AsyncMock, owner.llm.generate).side_effect = RuntimeError("offline")
    await owner.ensure_name(database, "workflow")
    assert (await database.get_workflow("workflow")).description is None
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
    generate = cast(AsyncMock, runner._naming.llm.generate)
    response = generate.return_value
    started = asyncio.Event()
    release = asyncio.Event()

    async def delayed(*args: object, **kwargs: object) -> connect.AssistantMessage:
        started.set()
        await release.wait()
        return cast(connect.AssistantMessage, response)

    generate.side_effect = delayed
    try:
        accepted = await asyncio.wait_for(
            runner.answer_input(answer(content("Help me fix tests"))), 1
        )
        assert accepted.status == "answered"
        await asyncio.wait_for(started.wait(), 1)
        assert (await database.get_workflow("workflow")).description is None
        release.set()
        async with asyncio.timeout(5):
            while (await database.get_workflow("workflow")).description is None:
                await asyncio.sleep(0.01)
        assert (
            await database.get_workflow("workflow")
        ).description == "Fix failing tests"
    finally:
        await runner.aclose()


@pytest.mark.parametrize("failure", [RuntimeError("offline"), asyncio.CancelledError()])
async def test_naming_failure_does_not_cancel_workflow(
    workspace: Path, failure: BaseException
) -> None:
    (workspace / "workflows/chat.py").write_text(
        "from vectorops.runner.api import llm\n"
        "async def main(ctx):\n"
        "    await llm.request_user_message()\n"
        "    await llm.request_user_message()\n"
        "    return 'completed'\n"
    )
    started = asyncio.Event()
    release = asyncio.Event()
    finished = asyncio.Event()

    async def fail(*args: object, **kwargs: object) -> connect.AssistantMessage:
        started.set()
        await release.wait()
        finished.set()
        raise failure

    with patch.object(RunnerLLM, "generate", side_effect=fail):
        async with client.embedded(workspace) as connection:
            accepted = await connection.start(
                protocol.Start(reference="chat:main", idempotency_key="start")
            )
            request = await pending(connection, accepted.execution_id)
            await asyncio.wait_for(reply(connection, request, "Hello"), 1)
            await asyncio.wait_for(started.wait(), 1)
            next_request = await pending(connection, accepted.execution_id)
            release.set()
            await asyncio.wait_for(finished.wait(), 1)
            await asyncio.wait_for(reply(connection, next_request, "Continue"), 1)
            await settled(connection, accepted.execution_id)
            assert (
                await connection.execution_summary(accepted.execution_id)
            ).status == "completed"


async def test_resume_does_not_wait_for_description_and_preserves_definition_name(
    workspace: Path,
) -> None:
    (workspace / "workflows/chat.py").write_text(
        "from vectorops.runner.api import composition, llm\n"
        "@composition.workflow(name='Customer workflow')\n"
        "async def main(ctx):\n"
        "    await llm.request_user_message()\n"
        "    return 'completed'\n"
    )
    async with client.embedded(workspace) as connection:
        accepted = await connection.start(
            protocol.Start(reference="chat:main", idempotency_key="start")
        )
        request = await pending(connection, accepted.execution_id)
    database = SQLiteHistory(
        workspace / "data/executions" / accepted.execution_id / "history.sqlite3"
    )
    await database.answer_input(
        mailbox.Answer(
            execution_id=accepted.execution_id,
            request_id=request.request_id,
            response=content("Hello"),
            idempotency_key="answer",
        )
    )
    started = asyncio.Event()
    release = asyncio.Event()

    async def delayed(*args: object, **kwargs: object) -> connect.AssistantMessage:
        started.set()
        await release.wait()
        return connect.AssistantMessage(
            content=[connect.TextBlock(text="Background title")], finish_reason="stop"
        )

    runner = WorkflowRunner(workspace)
    try:
        with patch.object(RunnerLLM, "generate", side_effect=delayed):
            assert (
                await asyncio.wait_for(runner.resume(accepted.execution_id), 1)
                == "completed"
            )
            await asyncio.wait_for(started.wait(), 1)
            assert (
                await database.get_workflow(request.workflow_id)
            ).description is None
            release.set()
            async with asyncio.timeout(5):
                while (
                    await database.get_workflow(request.workflow_id)
                ).description is None:
                    await asyncio.sleep(0.01)
            assert (
                await database.get_workflow(request.workflow_id)
            ).description == "Background title"
            assert (
                await database.get_workflow(request.workflow_id)
            ).name == "Customer workflow"
    finally:
        await runner.aclose()


@pytest.mark.parametrize("delete", [False, True])
async def test_shutdown_and_deletion_cancel_background_naming(
    workspace: Path, delete: bool
) -> None:
    (workspace / "workflows/chat.py").write_text(
        "from vectorops.runner.api import llm\n"
        "async def main(ctx):\n"
        "    while True:\n"
        "        await llm.request_user_message()\n"
    )
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def delayed(*args: object, **kwargs: object) -> connect.AssistantMessage:
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()
        raise AssertionError("naming should be cancelled")

    with patch.object(RunnerLLM, "generate", side_effect=delayed):
        async with client.embedded(workspace) as connection:
            accepted = await connection.start(
                protocol.Start(reference="chat:main", idempotency_key="start")
            )
            request = await pending(connection, accepted.execution_id)
            await asyncio.wait_for(reply(connection, request, "Hello"), 1)
            await asyncio.wait_for(started.wait(), 1)
            await pending(connection, accepted.execution_id)
            if delete:
                async with asyncio.timeout(5):
                    while (
                        await connection.execution(accepted.execution_id)
                    ).status != "paused":
                        await asyncio.sleep(0.01)
                assert (
                    await connection.delete_execution(accepted.execution_id)
                ).deleted
                assert cancelled.is_set()
                assert not (
                    workspace / "data/executions" / accepted.execution_id
                ).exists()
        assert cancelled.is_set()


def test_invalid_workflow_name() -> None:
    with pytest.raises(ValueError):
        models.Workflow(id="workflow", definition_ref="test:workflow", name="")


@pytest.mark.parametrize("description", ["", "x" * 121])
def test_invalid_execution_description(description: str) -> None:
    with pytest.raises(ValueError):
        models.Workflow(
            id="workflow", definition_ref="test:workflow", description=description
        )


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
    assert (await database.get_workflow("workflow")).description is None


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
    assert (await database.get_workflow("workflow")).description is None
    reopened = SQLiteHistory(database.path)
    await (await naming(tmp_path)).ensure_name(reopened, "workflow")
    assert (await reopened.get_workflow("workflow")).description == "Fix failing tests"
