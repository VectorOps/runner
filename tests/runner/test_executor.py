from pathlib import Path
import sys

import pytest

from tests.fixtures.history import MemoryHistory
from vectorops.runner import models
from vectorops.runner.runner.executor import WorkflowExecutor
from vectorops.runner.runner.project import RunnerProject


@pytest.mark.parametrize("name", ["activities", "activities.py"])
async def test_resolve_async_callable(
    workspace: Path, history: MemoryHistory, name: str
) -> None:
    executor = WorkflowExecutor(await RunnerProject.open(workspace), history)
    before = set(sys.modules)
    assert callable(await executor.load(name, "workflow"))
    assert not {
        name
        for name in set(sys.modules) - before
        if name.startswith("_runner_workflow_")
    }


@pytest.mark.parametrize(
    "name,callable_name,error",
    [
        ("missing", "workflow", FileNotFoundError),
        ("activities", "missing", TypeError),
        ("activities", "synchronous", TypeError),
        ("activities", "bad.name", ValueError),
        ("../outside", "workflow", ValueError),
        ("activities.txt", "workflow", ValueError),
        ("", "workflow", ValueError),
    ],
)
async def test_invalid_targets(
    workspace: Path,
    history: MemoryHistory,
    name: str,
    callable_name: str,
    error: type[Exception],
) -> None:
    executor = WorkflowExecutor(await RunnerProject.open(workspace), history)
    with pytest.raises(error):
        await executor.load(name, callable_name)


async def test_absolute_and_symlink_targets(
    workspace: Path, tmp_path: Path, history: MemoryHistory
) -> None:
    executor = WorkflowExecutor(await RunnerProject.open(workspace), history)
    with pytest.raises(ValueError):
        await executor.load(str(workspace / "workflows/activities.py"), "workflow")
    outside = workspace.parent / f"{workspace.name}-outside.py"
    outside.write_text("async def workflow(ctx): return 1")
    (workspace / "workflows/link.py").symlink_to(outside)
    with pytest.raises(ValueError):
        await executor.load("link", "workflow")


async def test_import_exception_cleans_module(
    workspace: Path, history: MemoryHistory
) -> None:
    (workspace / "workflows/broken.py").write_text(
        'raise RuntimeError("import failed")'
    )
    executor = WorkflowExecutor(await RunnerProject.open(workspace), history)
    before = set(sys.modules)
    with pytest.raises(RuntimeError, match="import failed"):
        await executor.load("broken", "workflow")
    assert not {
        name
        for name in set(sys.modules) - before
        if name.startswith("_runner_workflow_")
    }


async def test_invalid_definition_reference(
    workspace: Path, history: MemoryHistory
) -> None:
    executor = WorkflowExecutor(await RunnerProject.open(workspace), history)
    with pytest.raises(ValueError, match="file and callable"):
        await executor.execute(models.Workflow(id="w", definition_ref="invalid"))
