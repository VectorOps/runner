from pathlib import Path
import typing

import pydantic
import pytest

from tests.fixtures.history import MemoryHistory
from vectorops.runner import models
from vectorops.runner.api import composition as c
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.errors import ReplayMismatchError
from vectorops.runner.runner.events import StepEvent
from vectorops.runner.runner.project import RunnerProject
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner.runner.workflow import WorkflowRunner


@pytest.mark.parametrize("version", [0, -1, True, "1", 1.5])
def test_contract_versions_reject_invalid_values(version: object) -> None:
    value = typing.cast(int, version)
    with pytest.raises(pydantic.ValidationError):
        models.StepOperation(kind="vocode.llm.invoke", name="invoke", version=value)
    with pytest.raises(pydantic.ValidationError):
        models.Workflow(id="w", definition_ref="file:workflow", version=value)


def test_old_records_default_to_version_one() -> None:
    assert (
        models.StepOperation.model_validate({"kind": "activity", "name": "x"}).version
        == 1
    )
    workflow = models.Workflow.model_validate(
        {"id": "w", "definition_ref": "file:workflow"}
    )
    assert (workflow.kind, workflow.version) == ("workflow", 1)


def test_decorator_contracts_are_independent_of_display_names() -> None:
    async def invoke(ctx: ActivityExecutionContext) -> int:
        return 1

    invoke.__qualname__ = "invoke"
    definition = c.activity(name="display", namespace="customer.acme", version=2)(
        invoke
    )
    assert definition.kind == f"customer.acme.{__name__}.invoke"
    assert (definition.name, definition.version) == ("display", 2)
    assert c.activity(kind="vocode.llm.invoke", version=3)(invoke).version == 3
    with pytest.raises(ValueError, match="mutually exclusive"):
        c.activity(kind="x.y", namespace="customer.acme")(invoke)
    with pytest.raises(ValueError, match="namespace"):
        c.activity(namespace="customer..acme")(invoke)
    with pytest.raises(pydantic.ValidationError):
        c.activity(version=0)(invoke)


SOURCE = """from vectorops.runner.api import composition as c

@c.activity(namespace="customer.acme", version=2)
async def prepare(ctx, text):
    return text

@c.streamable_activity(kind="vocode.llm.invoke", version=3)
async def invoke(ctx, text):
    yield c.delta({"text": "partial"})
    yield c.delta({"text": text})
    yield c.final({"text": text})

@c.workflow(namespace="customer.acme", version=4)
async def turn(ctx, text):
    return await invoke(await prepare(text))

@c.workflow(kind="customer.acme.chat", version=2)
async def workflow(ctx):
    return await turn("hello")
"""


async def test_nested_contracts_persist_and_stream_without_extra_content_steps(
    workspace: Path, history: MemoryHistory
) -> None:
    directory = workspace / "workflows" / "support"
    directory.mkdir()
    (directory / "chat.py").write_text(SOURCE)
    events: list[StepEvent] = []
    runner = WorkflowRunner(workspace, history, event_sink=events.append)
    assert await runner.run("support/chat", workflow_id="root") == {"text": "hello"}
    assert (history.workflows["root"].kind, history.workflows["root"].version) == (
        "customer.acme.chat",
        2,
    )
    child = next(w for w in history.workflows.values() if w.id != "root")
    assert (child.kind, child.version) == ("customer.acme.support.chat.turn", 4)
    invocation = [e for e in events if e.step.operation.kind == "vocode.llm.invoke"]
    assert len({e.step.id for e in invocation}) == 1
    assert all(
        isinstance(e.step.operation, models.StepOperation)
        and e.step.operation.version == 3
        for e in invocation
    )
    assert [
        e.step.outcome.state
        for e in invocation
        if isinstance(e.step.outcome, models.StepStreaming)
    ] == [None, {"text": "partial"}, {"text": "hello"}, {"text": "hello"}]
    assert await runner.run(
        "support/chat", continue_from_step_id=history.heads["root"]
    ) == {"text": "hello"}
    assert any(
        s.operation.kind == "customer.acme.support.chat.prepare"
        for s in history.steps.values()
    )


async def test_contracts_survive_sqlite_restart(workspace: Path) -> None:
    (workspace / "workflows" / "contracts.py").write_text(SOURCE)
    runner = WorkflowRunner(workspace)
    assert await runner.run("contracts") == {"text": "hello"}
    assert runner.last_execution_id is not None
    project = await RunnerProject.open(workspace)
    database = SQLiteHistory(
        project.data_path / "executions" / runner.last_execution_id / "history.sqlite3"
    )
    record = await runner.execution_state(runner.last_execution_id)
    root = await database.get_workflow(record.root_workflow_id)
    assert (root.kind, root.version) == ("customer.acme.chat", 2)
    assert await WorkflowRunner(workspace).resume(runner.last_execution_id) == {
        "text": "hello"
    }


@pytest.mark.parametrize("declaration", ["activity", "stream", "workflow", "child"])
async def test_replay_rejects_changed_contract_versions(
    workspace: Path, history: MemoryHistory, declaration: str
) -> None:
    path = workspace / "workflows" / "contracts.py"
    path.write_text(SOURCE)
    runner = WorkflowRunner(workspace, history)
    await runner.run("contracts", workflow_id="root")
    replacements = {
        "activity": (
            'namespace="customer.acme", version=2',
            'namespace="customer.acme", version=5',
        ),
        "stream": (
            'kind="vocode.llm.invoke", version=3',
            'kind="vocode.llm.invoke", version=5',
        ),
        "workflow": (
            'kind="customer.acme.chat", version=2',
            'kind="customer.acme.chat", version=5',
        ),
        "child": (
            'namespace="customer.acme", version=4',
            'namespace="customer.acme", version=5',
        ),
    }
    path.write_text(SOURCE.replace(*replacements[declaration]) + "\n")
    with pytest.raises(ReplayMismatchError):
        if declaration in ("activity", "stream"):
            child = next(w for w in history.workflows.values() if w.id != "root")
            await runner.run(
                "contracts.py", "turn", continue_from_step_id=history.heads[child.id]
            )
        else:
            await runner.run("contracts", continue_from_step_id=history.heads["root"])


async def test_manual_spawn_resolves_decorated_contract_and_replays(
    workspace: Path, history: MemoryHistory
) -> None:
    path = workspace / "workflows" / "contracts.py"
    path.write_text(
        SOURCE
        + """
async def manual(ctx):
    handle = await ctx.spawn_workflow("contracts.py", "turn", input={"text": "hello"})
    return (await ctx.join_workflows([handle]))[0].result
"""
    )
    runner = WorkflowRunner(workspace, history)
    assert await runner.run("contracts", "manual", workflow_id="root") == {
        "text": "hello"
    }
    child = next(w for w in history.workflows.values() if w.id != "root")
    assert (child.kind, child.version) == ("customer.acme.contracts.turn", 4)
    assert await runner.run(
        "contracts", "manual", continue_from_step_id=history.heads["root"]
    ) == {"text": "hello"}
