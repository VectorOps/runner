import asyncio
from pathlib import Path
from typing import Optional
from unittest.mock import patch

import pytest

from tests.fixtures import worktree_cases as cases
from vectorops.runner.environment.local import LocalEnvironment
from vectorops.runner.runner.project import RunnerProject
from vectorops.runner.runner.workflow import WorkflowRunner
from vectorops.runner.worktrees import models as m


@pytest.mark.parametrize(
    "root,child,grandchild",
    [
        (None, None, None),
        ("A", None, None),
        ("A", "A", None),
        (None, "B", None),
        ("A", "B", None),
        ("A", "B", "C"),
        ("A", "B", "A"),
        ("A", None, "C"),
    ],
)
async def test_child_target_matrix(
    worktree_project: RunnerProject,
    worktree_probe: cases.Probe,
    repository: Path,
    root: Optional[str],
    child: Optional[str],
    grandchild: Optional[str],
) -> None:
    manager = worktree_project.worktrees
    records = {
        name: await manager.create(m.Create(name=name), key=name)
        for name in sorted(
            {name for name in (root, child, grandchild) if name is not None}
        )
    }
    selected_child = child or root
    selected_grandchild = grandchild or selected_child
    paths = {name: record.path for name, record in records.items()}
    paths["default"] = str(repository)
    expected = {
        "parent-before": paths[root or "default"],
        "child-before": paths[selected_child or "default"],
        "leaf": paths[selected_grandchild or "default"],
        "child-after": paths[selected_child or "default"],
        "parent-after": paths[root or "default"],
    }
    runner = WorkflowRunner(worktree_project.root, project=worktree_project)
    with patch.object(LocalEnvironment, "open", wraps=LocalEnvironment.open) as opened:
        result = await runner.run(
            cases.REFERENCE + "matrix",
            input={
                "child": str(records[child].id) if child else None,
                "grandchild": str(records[grandchild].id) if grandchild else None,
            },
            worktree_id=records[root].id if root else None,
        )
    assert result == expected
    assert opened.await_count == 1 + (selected_child != root) + (
        selected_grandchild != selected_child
    )
    contexts = worktree_probe.contexts
    assert contexts["parent-before"].environment is contexts["parent-after"].environment
    assert contexts["child-before"].environment is contexts["child-after"].environment
    for parent, descendant, shared in (
        ("parent-before", "child-before", selected_child == root),
        ("child-before", "leaf", selected_grandchild == selected_child),
    ):
        assert (
            contexts[parent].environment is contexts[descendant].environment
        ) is shared
        assert (contexts[parent].resources is contexts[descendant].resources) is shared
    for record in records.values():
        assert await manager.store.uses(record.id) == []
        assert (await manager.get(m.Get(worktree_id=record.id))).status == "ready"


async def test_no_override_does_not_resolve_or_open_another_target(
    worktree_project: RunnerProject, worktree_probe: cases.Probe
) -> None:
    with (
        patch.object(
            worktree_project.worktrees,
            "resolve",
            side_effect=AssertionError("unexpected worktree resolution"),
        ),
        patch.object(LocalEnvironment, "open", wraps=LocalEnvironment.open) as opened,
    ):
        await WorkflowRunner(worktree_project.root, project=worktree_project).run(
            cases.REFERENCE + "matrix"
        )
    assert opened.await_count == 1
    assert (
        len({id(context.environment) for context in worktree_probe.contexts.values()})
        == 1
    )
    assert (
        len({id(context.resources) for context in worktree_probe.contexts.values()})
        == 1
    )


@pytest.mark.parametrize("shared", [False, True])
async def test_parallel_sibling_targets_survive_independent_completion(
    worktree_project: RunnerProject,
    worktree_probe: cases.Probe,
    repository: Path,
    shared: bool,
) -> None:
    manager = worktree_project.worktrees
    first = await manager.create(m.Create(name="first"), key="first")
    second = (
        first if shared else await manager.create(m.Create(name="second"), key="second")
    )
    worktree_probe.gates = {label: asyncio.Event() for label in ("child-0", "child-1")}
    runner = WorkflowRunner(worktree_project.root, project=worktree_project)
    task = asyncio.create_task(
        runner.run(
            cases.REFERENCE + "siblings",
            input={"worktrees": [str(first.id), str(second.id)]},
        )
    )
    try:
        started = {
            await asyncio.wait_for(worktree_probe.started.get(), 5) for _ in range(2)
        }
        assert started == {"child-0", "child-1"}
        contexts = worktree_probe.contexts
        assert contexts["child-0"].environment is not contexts["child-1"].environment
        for label, record in (("child-0", first), ("child-1", second)):
            assert await cases.cwd(contexts[label]) == record.path
        worktree_probe.gates["child-0"].set()
        async with asyncio.timeout(5):
            while len(await manager.store.uses(first.id)) != (1 if shared else 0):
                await asyncio.sleep(0.01)
        assert not task.done()
        assert await cases.cwd(contexts["child-1"]) == second.path
        assert await cases.cwd(contexts["parent-before"]) == str(repository)
        with pytest.raises(m.WorktreeError, match="in use"):
            await manager.delete(m.Delete(worktree_id=second.id, force=True))
        if not shared:
            assert (
                await manager.delete(m.Delete(worktree_id=first.id))
            ).status == "deleted"
        worktree_probe.gates["child-1"].set()
        assert await asyncio.wait_for(task, 5) == [first.path, second.path]
        assert (
            worktree_probe.contexts["parent-before"].environment
            is worktree_probe.contexts["parent-after"].environment
        )
        assert await manager.store.uses(second.id) == []
    finally:
        if not task.done():
            task.cancel()
        for gate in worktree_probe.gates.values():
            gate.set()
        await asyncio.gather(task, return_exceptions=True)
