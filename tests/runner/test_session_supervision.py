import asyncio
from pathlib import Path

import pytest

from tests.fixtures.history import MemoryHistory
from tests.fixtures.probe import ExecutionProbe
from vectorops.runner import models
from vectorops.runner.runner.workflow import WorkflowRunner


class JoinWriteFailure(MemoryHistory):
    async def update_step(self, step: models.WorkflowStep) -> None:
        if step.operation.kind == "workflow.join":
            raise OSError("join write failed")
        await super().update_step(step)


async def test_deferred_history_failure_is_fatal_not_a_child_exception(
    workspace: Path, execution_probe: ExecutionProbe
) -> None:
    history = JoinWriteFailure()
    before = set(asyncio.all_tasks())
    with pytest.raises(OSError, match="join write"):
        await asyncio.wait_for(
            WorkflowRunner(workspace, history).run("composed:collected"), 2
        )
    assert not set(asyncio.all_tasks()) - before
    assert any(step.outcome.kind == "waiting" for step in history.steps.values())
    assert any(
        step.operation.kind == "workflow.complete"
        and step.outcome.kind == "failed"
        and step.workflow_id
        == next(
            workflow.id
            for workflow in history.workflows.values()
            if workflow.definition_ref == "composed:collected"
        )
        for step in history.steps.values()
    )


@pytest.mark.parametrize(
    "callable_name,expected",
    [("cancelled_wait", "wait cancelled"), ("abandoned_call", "parent completed")],
)
async def test_parent_exit_cleans_cancelled_and_abandoned_child_calls(
    runner: WorkflowRunner,
    execution_probe: ExecutionProbe,
    callable_name: str,
    expected: str,
) -> None:
    before = set(asyncio.all_tasks())
    assert (
        await asyncio.wait_for(runner.run(f"composed:{callable_name}"), 2) == expected
    )
    assert execution_probe.active == 0
    assert not set(asyncio.all_tasks()) - before
