from pathlib import Path
import sys
from typing import Optional

import pytest

from tests.fixtures import sources
from tests.fixtures.history import MemoryHistory
from vectorops.runner import models
from vectorops.runner.presentation import resolve_display
from vectorops.runner.runner.executor import WorkflowExecutor
from vectorops.runner.runner.project import RunnerProject


@pytest.mark.parametrize(
    "name", ["activities", "python:tests.fixtures.workflows.activities"]
)
async def test_resolve_async_callable(
    workspace: Path, history: MemoryHistory, name: str
) -> None:
    executor = WorkflowExecutor(await RunnerProject.open(workspace), history)
    before = set(sys.modules)
    assert callable(await executor.load(f"{name}:workflow"))
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
        await executor.load(f"{name}:{callable_name}")


async def test_absolute_and_symlink_targets(
    workspace: Path, tmp_path: Path, history: MemoryHistory
) -> None:
    executor = WorkflowExecutor(await RunnerProject.open(workspace), history)
    with pytest.raises(ValueError):
        await executor.load(f"{workspace / 'workflows/activities'}:workflow")
    outside = workspace.parent / f"{workspace.name}-outside.py"
    outside.write_text(sources.read("workflow_one.py"))
    (workspace / "workflows/link.py").symlink_to(outside)
    with pytest.raises(ValueError):
        await executor.load("link:workflow")


async def test_import_exception_cleans_module(
    workspace: Path, history: MemoryHistory
) -> None:
    (workspace / "workflows/broken.py").write_text(sources.read("import_failure.py"))
    executor = WorkflowExecutor(await RunnerProject.open(workspace), history)
    before = set(sys.modules)
    with pytest.raises(RuntimeError, match="import failed"):
        await executor.load("broken:workflow")
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


@pytest.mark.parametrize(
    "declared_name", [None, "Customer workflow", "Long name " * 20]
)
@pytest.mark.parametrize("execution_name", [None, "Explicit name"])
async def test_configure_defaults_execution_name_from_definition(
    workspace: Path,
    history: MemoryHistory,
    declared_name: Optional[str],
    execution_name: Optional[str],
) -> None:
    (workspace / "workflows/named.py").write_text(
        "from vectorops.runner.api import composition\n"
        f"@composition.workflow(name={declared_name!r})\n"
        "async def main(ctx):\n"
        "    return 'done'\n"
    )
    project = await RunnerProject.open(workspace)
    descriptor = await project.resolver.describe("named:main")
    original = models.Workflow(
        id="workflow", definition_ref="named:main", name=execution_name
    )
    configured = await WorkflowExecutor(project, history).configure(original)
    expected = execution_name if execution_name is not None else descriptor.name
    assert configured.name == expected
    assert original.name == execution_name
    assert configured.description is None
    assert resolve_display(configured).label == " ".join(expected.split())[:40]


async def test_undecorated_workflow_keeps_name_optional(
    workspace: Path, history: MemoryHistory
) -> None:
    executor = WorkflowExecutor(await RunnerProject.open(workspace), history)
    configured = await executor.configure(
        models.Workflow(id="workflow", definition_ref="activities:workflow")
    )
    assert configured.name is None
    assert resolve_display(configured).label == "activities:workflow"
