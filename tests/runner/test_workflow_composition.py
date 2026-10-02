import asyncio
from pathlib import Path
import typing

import pytest

from tests.fixtures.history import MemoryHistory
from tests.fixtures.probe import ExecutionProbe
from tests.fixtures.library_workflows import child as library_child
from vectorops.runner.api import composition as c
from vectorops.runner.runner.context import WorkflowExecutionContext
from vectorops.runner.runner.workflow import WorkflowRunner


async def test_direct_await_records_spawn_join_and_returns_result(
    runner: WorkflowRunner, history: MemoryHistory, execution_probe: ExecutionProbe
) -> None:
    assert await runner.run("composed", workflow_id="root") == 6
    root = history.path(history.heads["root"])
    assert [step.operation.kind for step in root] == [
        "workflow.spawn",
        "workflow.join",
        "workflow.resume",
        "workflow.complete",
    ]
    child = next(
        workflow for workflow in history.workflows.values() if workflow.id != "root"
    )
    assert child.definition_ref == "composed.py:child"
    assert child.input == {"value": 3, "block": False}
    appended = list(history.appended)
    assert await runner.run("composed", continue_from_step_id=root[-1].id) == 6
    assert history.appended == appended
    assert execution_probe.calls == [3]


async def test_standard_gather_runs_children_concurrently(
    runner: WorkflowRunner, history: MemoryHistory, execution_probe: ExecutionProbe
) -> None:
    task = asyncio.create_task(runner.run("composed", "parallel"))
    assert {
        await asyncio.wait_for(execution_probe.started.get(), 2),
        await asyncio.wait_for(execution_probe.started.get(), 2),
    } == {1, 2}
    assert execution_probe.peak_active == 2
    execution_probe.gate.set()
    assert await asyncio.wait_for(task, 2) == [4, 2]
    root = next(
        workflow
        for workflow in history.workflows.values()
        if workflow.definition_ref == "composed:parallel"
    )
    appended = list(history.appended)
    assert await runner.run(
        "composed", "parallel", continue_from_step_id=history.heads[root.id]
    ) == [4, 2]
    assert history.appended == appended
    assert sorted(execution_probe.calls) == [1, 2]


async def test_imported_library_child_has_resolvable_reference(
    runner: WorkflowRunner, history: MemoryHistory
) -> None:
    assert await runner.run("composed", "library", workflow_id="root") == 12
    child = next(
        workflow for workflow in history.workflows.values() if workflow.id != "root"
    )
    assert child.definition_ref == "python:tests.fixtures.library_workflows:child"
    assert library_child.name == "library-multiply"


async def test_imported_library_can_be_root(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    runner = WorkflowRunner(workspace)
    assert (
        await runner.run(
            "python:tests.fixtures.library_workflows", "child", input={"value": 4}
        )
        == 12
    )
    assert await WorkflowRunner(workspace).resume() == 12
    assert execution_probe.calls == [4]


async def test_decorated_workspace_root_survives_restart(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    assert await WorkflowRunner(workspace).run("composed") == 6
    assert await WorkflowRunner(workspace).resume() == 6
    assert execution_probe.calls == [3]


async def test_child_failure_can_be_caught(
    runner: WorkflowRunner, history: MemoryHistory
) -> None:
    assert (
        await runner.run("composed", "caught", workflow_id="root") == "negative child"
    )
    assert (
        await runner.run(
            "composed", "caught", continue_from_step_id=history.heads["root"]
        )
        == "negative child"
    )


async def test_undecorated_helper_stays_in_parent_scope(
    runner: WorkflowRunner, history: MemoryHistory
) -> None:
    assert await runner.run("composed", "inline", workflow_id="root") == 8
    assert len(history.workflows) == 1
    assert all(
        step.operation.kind != "workflow.spawn" for step in history.steps.values()
    )


async def test_child_call_requires_workflow_context() -> None:
    with pytest.raises(RuntimeError, match="active workflow"):
        await library_child()


def test_persistent_workflow_rejects_closures_and_invalid_signatures() -> None:
    async def local(ctx: WorkflowExecutionContext) -> int:
        return 1

    with pytest.raises(ValueError, match="module-level"):
        c.workflow()(local)

    def sync(ctx: WorkflowExecutionContext) -> int:
        return 1

    with pytest.raises(TypeError, match="async"):
        c.workflow()(typing.cast(typing.Any, sync))


def test_workflow_metadata_and_input_signature_validation() -> None:
    async def module_function(ctx: WorkflowExecutionContext, value: int = 2) -> int:
        return value

    module_function.__qualname__ = "module_function"
    definition = c.workflow()(module_function)
    assert definition.name.endswith(".module_function")
    assert c.workflow(name="custom")(module_function).name == "custom"
    with pytest.raises(ValueError, match="name"):
        c.workflow(name="")(module_function)

    async def variadic(ctx: WorkflowExecutionContext, *values: int) -> int:
        return len(values)

    variadic.__qualname__ = "variadic"
    with pytest.raises(TypeError, match="named parameters"):
        c.workflow()(variadic)


async def test_decorated_child_stream_fork_reuses_child_outcome(
    runner: WorkflowRunner, history: MemoryHistory, execution_probe: ExecutionProbe
) -> None:
    assert await runner.run("composed", "streamed", workflow_id="root") == 7
    path = history.path(history.heads["root"])
    assert (
        await runner.run("composed", "streamed", continue_from_step_id=path[0].id) == 7
    )
    fork = history.path(history.heads["root"])
    assert fork[0] == path[0]
    assert fork[1].id != path[1].id
    assert history.steps[path[-1].id] == path[-1]
