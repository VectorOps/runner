import asyncio
import os
from pathlib import Path

import pytest

from tests.fixtures import resource_workflows as workflows
from tests.fixtures.history import MemoryHistory
from vectorops.runner.control.service import RunnerService
from vectorops.runner.runner.errors import ActivityError
from vectorops.runner.runner.resources import ExecutionResources
from vectorops.runner.runner.workflow import WorkflowRunner


async def test_cleanup_is_lifo_idempotent_and_continues_after_failure() -> None:
    resources = ExecutionResources()
    order: list[str] = []

    async def first() -> None:
        order.append("first")

    async def broken() -> None:
        order.append("broken")
        raise ValueError("cleanup failed")

    resources.register("first", first)
    resources.register("broken", broken)
    with pytest.raises(ExceptionGroup):
        await resources.close()
    assert order == ["broken", "first"]
    with pytest.raises(ExceptionGroup):
        await resources.close()
    assert order == ["broken", "first"]
    with pytest.raises(RuntimeError):
        resources.register("late", first)


async def test_cleanup_preserves_original_error() -> None:
    async def broken() -> None:
        raise ValueError("cleanup")

    with pytest.raises(RuntimeError, match="original"):
        async with ExecutionResources() as resources:
            resources.register("broken", broken)
            raise RuntimeError("original")


async def test_cleanup_waits_through_repeated_cancellation() -> None:
    resources = ExecutionResources()
    started = asyncio.Event()
    release = asyncio.Event()
    finished = asyncio.Event()

    async def cleanup() -> None:
        started.set()
        await release.wait()
        finished.set()

    resources.register("blocking", cleanup)
    task = asyncio.create_task(resources.close())
    await started.wait()
    for _ in range(3):
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert finished.is_set()
    await resources.close()


@pytest.fixture
def configured_workspace(
    workspace: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Path:
    target = tmp_path / "target"
    target.mkdir()
    (workspace / "config.toml").write_text(
        f'[environment]\nkind = "local"\nbase_project_path = "{target}"\n'
    )
    monkeypatch.setattr(workflows, "probe", workflows.Probe())
    return workspace


@pytest.mark.skipif(os.name != "posix", reason="local environment requires POSIX")
@pytest.mark.parametrize("mode", ["success", "failure", "child", "wait", "cancel"])
async def test_root_owns_environment_until_shutdown(
    configured_workspace: Path, mode: str
) -> None:
    runner = WorkflowRunner(configured_workspace, MemoryHistory())
    task = asyncio.create_task(
        runner.run(
            "python:tests.fixtures.resource_workflows:workflow",
            input={"mode": "wait" if mode == "cancel" else mode},
        )
    )
    pid = await asyncio.wait_for(workflows.probe.started.get(), 5)
    if mode in ("wait", "cancel"):
        await asyncio.wait_for(workflows.probe.waiting.wait(), 5)
        if mode == "cancel":
            task.cancel()
        else:
            runner.stop()
            runner.stop()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)
        assert workflows.probe.events == ["workflow.stopped", "resource.closed"]
    elif mode == "failure":
        with pytest.raises(ActivityError):
            await asyncio.wait_for(task, 5)
    else:
        assert await asyncio.wait_for(task, 5) == "done"
    assert "resource.closed" in workflows.probe.events
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)
    environment = workflows.probe.contexts[0].environment
    assert environment is not None
    assert await environment.processes.list_processes() == []


@pytest.mark.skipif(os.name != "posix", reason="local environment requires POSIX")
@pytest.mark.parametrize("shutdown", [False, True])
async def test_service_stop_closes_processes_before_terminal_event(
    configured_workspace: Path, tmp_path: Path, shutdown: bool
) -> None:
    async with RunnerService(
        configured_workspace, config_dir=tmp_path / "global"
    ) as service:
        stopped = asyncio.Event()
        terminal = "execution.suspended" if shutdown else "execution.cancelled"
        service.listen(
            lambda event: (stopped.set() if event.get("kind") == terminal else None)
        )
        await service.dispatch(
            "executions.start",
            {
                "reference": "python:tests.fixtures.resource_workflows:workflow",
                "input": {"mode": "wait"},
                "idempotency_key": "process-cleanup",
            },
        )
        pid = await asyncio.wait_for(workflows.probe.started.get(), 5)
        await asyncio.wait_for(workflows.probe.waiting.wait(), 5)
        identifier = service.runner.last_execution_id
        assert identifier is not None
        if shutdown:
            await asyncio.wait_for(service.aclose(), 5)
        else:
            await service.dispatch("executions.stop", {"execution_id": identifier})
        await asyncio.wait_for(stopped.wait(), 5)
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)
        environment = workflows.probe.contexts[0].environment
        assert environment is not None
        assert await environment.processes.list_processes() == []


@pytest.mark.skipif(os.name != "posix", reason="local environment requires POSIX")
async def test_concurrent_roots_have_separate_attachments(
    configured_workspace: Path,
) -> None:
    runner = WorkflowRunner(configured_workspace, MemoryHistory())
    tasks = [
        asyncio.create_task(
            runner.run(
                "python:tests.fixtures.resource_workflows:workflow",
                input={"mode": "wait"},
                workflow_id=f"root-{index}",
            )
        )
        for index in range(2)
    ]
    pids = [await asyncio.wait_for(workflows.probe.started.get(), 5) for _ in tasks]
    try:
        contexts = workflows.probe.contexts
        assert contexts[0].environment is not contexts[1].environment
        assert contexts[0].resources is not contexts[1].resources
        tasks[0].cancel()
        with pytest.raises(asyncio.CancelledError):
            await tasks[0]
        assert not tasks[1].done()
        environment = next(
            ctx.environment for ctx in contexts if ctx.workflow_id == "root-1"
        )
        assert environment is not None and await environment.processes.list_processes()
    finally:
        runner.stop()
        await asyncio.gather(*tasks, return_exceptions=True)
    for pid in pids:
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)


@pytest.mark.skipif(os.name != "posix", reason="local environment requires POSIX")
async def test_persistent_execution_closes_environment(
    configured_workspace: Path,
) -> None:
    runner = WorkflowRunner(configured_workspace)
    assert (
        await runner.run("python:tests.fixtures.resource_workflows:workflow") == "done"
    )
    assert runner.last_execution_id is not None
    assert (
        await runner.execution_state(runner.last_execution_id)
    ).status == "completed"
    pid = workflows.probe.started.get_nowait()
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


async def test_root_registry_with_default_environment_and_repeated_cancellation(
    workspace: Path,
) -> None:
    from vectorops.runner import models
    from vectorops.runner.runner.activity_context import ActivityExecutionContext
    from vectorops.runner.runner.context import WorkflowExecutionContext
    from vectorops.runner.runner.executor import WorkflowCallable, WorkflowExecutor
    from unittest.mock import patch

    cleaning = asyncio.Event()
    release = asyncio.Event()
    finished = asyncio.Event()

    async def activity(ctx: ActivityExecutionContext) -> None:
        assert ctx.environment is not None and ctx.resources is not None

        async def cleanup() -> None:
            cleaning.set()
            await release.wait()
            finished.set()

        ctx.resources.register("other resource", cleanup)

    async def workflow(ctx: WorkflowExecutionContext) -> None:
        await ctx.run_activity(
            models.StepOperation(kind="activity", name="resource"), activity
        )

    async def definition(self: WorkflowExecutor, reference: str) -> WorkflowCallable:
        return workflow

    with patch.object(WorkflowExecutor, "_definition", definition):
        runner = WorkflowRunner(workspace, MemoryHistory())
        task = asyncio.create_task(runner.run("resources:workflow"))
        await asyncio.wait_for(cleaning.wait(), 5)
        for _ in range(3):
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)
        assert finished.is_set()
