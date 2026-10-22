from pathlib import Path
from typing import Optional

import pytest

from tests.fixtures import probe
from tests.fixtures.probe import ExecutionProbe
from vectorops.runner import models
from vectorops.runner.runner import branching, execution_state, mailbox
from vectorops.runner.runner.events import StepEvent
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner.runner.workflow import WorkflowRunner


def selection_history(
    outcomes: list[Optional[models.ChildOutcome]],
    *,
    selected: Optional[int] = 0,
    candidates: Optional[list[int]] = None,
) -> tuple[dict[str, models.WorkflowStep], execution_state.ExecutionRecord]:
    steps: dict[str, models.WorkflowStep] = {}
    heads: dict[str, str] = {}
    previous = None
    for index, outcome in enumerate(outcomes):
        child = f"child-{index}"
        spawn = f"spawn-{index}"
        steps[spawn] = models.WorkflowStep(
            id=spawn,
            workflow_id="root",
            previous_step_id=previous,
            operation=models.WorkflowSpawnOperation(child_workflow_id=child),
            outcome=models.StepCompleted(),
        )
        previous = spawn
        activity = f"activity-{index}"
        steps[activity] = models.WorkflowStep(
            id=activity,
            workflow_id=child,
            operation=models.StepOperation(kind="activity", name="work"),
            outcome=(
                models.StepCompleted()
                if outcome is not None and outcome.kind == "completed"
                else models.StepPending()
            ),
            deferred_delivery=True,
            deliveries=(
                [activity]
                if outcome is not None and outcome.kind == "completed"
                else []
            ),
        )
        heads[child] = activity
        if outcome is not None:
            completion = f"complete-{index}"
            steps[completion] = models.WorkflowStep(
                id=completion,
                workflow_id=child,
                previous_step_id=activity,
                operation=models.WorkflowCompleteOperation(),
                outcome=outcome,
            )
            heads[child] = completion
    steps["selection"] = models.WorkflowStep(
        id="selection",
        workflow_id="root",
        previous_step_id=previous,
        operation=models.WorkflowNextCompletedOperation(
            spawn_step_ids=[
                f"spawn-{index}"
                for index in (
                    range(len(outcomes)) if candidates is None else candidates
                )
            ]
        ),
        outcome=(
            models.StepWaiting()
            if selected is None
            else models.StepJoined(
                child_completions=[
                    models.ChildWorkflowCompletion(
                        spawn_step_id=f"spawn-{selected}",
                        completion_step_id=f"complete-{selected}",
                    )
                ]
            )
        ),
        deferred_delivery=True,
    )
    heads["root"] = "selection"
    record = execution_state.ExecutionRecord(
        id="execution",
        root_workflow_id="root",
        checkpoint=execution_state.ExecutionCheckpoint(heads=heads),
    )
    return steps, record


def make_plan(
    steps: dict[str, models.WorkflowStep], execution: execution_state.ExecutionRecord
) -> branching.Plan:
    def no_input(identifier: str) -> mailbox.StoredInputRequest:
        raise AssertionError("no mailbox boundary")

    return branching.plan(
        branching.Operation(
            request=branching.Branch(
                execution_id=execution.id,
                step_id="selection",
                idempotency_key="restart",
                action=branching.Restart(),
            )
        ),
        execution,
        steps.__getitem__,
        no_input,
    )


def test_restart_single_completed_selection_is_exclusive() -> None:
    steps, record = selection_history([models.StepCompleted(result=1)])
    plan = make_plan(steps, record)
    assert plan.heads == {"root": "spawn-0", "child-0": "complete-0"}
    assert plan.retained_step_ids == {"spawn-0", "activity-0", "complete-0"}
    assert "selection" not in plan.retained_step_ids


@pytest.mark.parametrize("selected", [None, 0, 1])
@pytest.mark.parametrize(
    "first",
    [
        models.StepCompleted(),
        models.StepFailed(error=models.StepError(type="Error", message="failed")),
        models.StepCancelled(),
    ],
)
@pytest.mark.parametrize(
    "second",
    [
        models.StepCompleted(),
        models.StepFailed(error=models.StepError(type="Error", message="failed")),
        models.StepCancelled(),
    ],
)
def test_terminal_candidates_are_retained_without_joining_all(
    selected: Optional[int],
    first: models.ChildOutcome,
    second: models.ChildOutcome,
) -> None:
    steps, record = selection_history([first, second], selected=selected)
    plan = make_plan(steps, record)
    assert plan.heads == {
        "root": "spawn-1",
        "child-0": "complete-0",
        "child-1": "complete-1",
    }
    assert plan.retained_step_ids == set(steps) - {"selection"}
    assert steps["selection"].outcome.kind == (
        "waiting" if selected is None else "joined"
    )
    if selected is not None:
        outcome = steps["selection"].outcome
        assert outcome.kind == "joined" and len(outcome.child_completions) == 1


@pytest.mark.parametrize(
    "outcomes,selected",
    [
        ([models.StepCompleted(), None], 0),
        ([None, models.StepCompleted()], 1),
        ([None], None),
    ],
)
def test_unfinished_candidates_require_restart_at_spawn(
    outcomes: list[Optional[models.ChildOutcome]], selected: Optional[int]
) -> None:
    steps, record = selection_history(outcomes, selected=selected)
    with pytest.raises(branching.BranchConflict, match="unfinished child"):
        make_plan(steps, record)


def test_unrelated_outstanding_child_is_not_adopted() -> None:
    steps, record = selection_history(
        [models.StepCompleted(), models.StepCompleted()], candidates=[0]
    )
    with pytest.raises(branching.BranchConflict, match="outstanding child's spawn"):
        make_plan(steps, record)


def test_selection_rejects_foreign_completion_reference() -> None:
    steps, record = selection_history([models.StepCompleted()])
    steps["complete-0"] = steps["complete-0"].model_copy(
        update={"workflow_id": "other"}
    )
    with pytest.raises(branching.BranchConflict):
        make_plan(steps, record)


def test_recorded_winner_keeps_exact_completion_not_later_head() -> None:
    steps, record = selection_history([models.StepCompleted(), models.StepCompleted()])
    steps["later"] = models.WorkflowStep(
        id="later",
        workflow_id="child-0",
        operation=models.StepOperation(kind="activity", name="later"),
        outcome=models.StepPending(),
        deferred_delivery=True,
    )
    record = record.model_copy(
        update={
            "checkpoint": record.checkpoint.model_copy(
                update={"heads": {**record.checkpoint.heads, "child-0": "later"}}
            )
        }
    )
    plan = make_plan(steps, record)
    assert plan.heads["child-0"] == "complete-0"
    assert "later" not in plan.retained_step_ids


def test_candidate_without_checkpoint_is_rejected() -> None:
    steps, record = selection_history([models.StepCompleted(), None])
    record = record.model_copy(
        update={
            "checkpoint": record.checkpoint.model_copy(
                update={
                    "heads": {
                        key: value
                        for key, value in record.checkpoint.heads.items()
                        if key != "child-1"
                    }
                }
            )
        }
    )
    with pytest.raises(branching.BranchConflict, match="unfinished child"):
        make_plan(steps, record)


def test_earlier_selection_is_replayed_while_target_selection_is_restarted() -> None:
    steps, record = selection_history([models.StepCompleted(), models.StepCompleted()])
    earlier = steps["selection"].model_copy(
        update={"id": "earlier", "deliveries": ["earlier"]}
    )
    steps["earlier"] = earlier
    steps["selection"] = models.WorkflowStep(
        id="selection",
        workflow_id="root",
        previous_step_id="earlier",
        operation=models.WorkflowNextCompletedOperation(spawn_step_ids=["spawn-1"]),
        outcome=models.StepJoined(
            child_completions=[
                models.ChildWorkflowCompletion(
                    spawn_step_id="spawn-1", completion_step_id="complete-1"
                )
            ]
        ),
        deferred_delivery=True,
    )
    plan = make_plan(steps, record)
    assert plan.heads == {
        "root": "earlier",
        "child-0": "complete-0",
        "child-1": "complete-1",
    }
    assert "earlier" in plan.retained_step_ids
    assert "selection" not in plan.retained_step_ids


@pytest.mark.parametrize("count", [1, 2])
async def test_manual_selection_restart_reuses_children(
    workspace: Path, execution_probe: ExecutionProbe, count: int
) -> None:
    source = """
from vectorops.runner.api import composition as c
from tests.fixtures import probe

@c.activity()
async def work(ctx, value: int) -> int:
    probe.current.calls.append(value)
    return value

@c.workflow()
async def child(ctx, value: int) -> int:
    return await work(value)

@c.activity()
async def ready(ctx):
    await probe.current.gate.wait()

async def main(ctx, count: int):
    handles = []
    for value in range(count):
        handle = await ctx.spawn_workflow("selection_branch:child", input={"value": value})
        handles.append(handle)
    await ready()
    results = []
    while handles:
        completion = await ctx.next_completed(handles)
        handles.remove(completion.handle)
        results.append(completion.outcome.result)
    return results
"""
    (workspace / "workflows/selection_branch.py").write_text(source)

    completions: set[str] = set()

    def committed(event: StepEvent) -> None:
        if event.step.operation.kind == "workflow.complete":
            completions.add(event.step.workflow_id)
            if len(completions) == count:
                execution_probe.gate.set()

    runner = WorkflowRunner(workspace, event_sink=committed)
    assert await runner.run("selection_branch:main", input={"count": count}) == list(
        range(count)
    )
    identifier = runner.last_execution_id
    assert identifier is not None
    database = SQLiteHistory(
        workspace / "data/executions" / identifier / "history.sqlite3"
    )
    record = await database.get_execution()
    original = [
        step
        async for step in database.replay_path(
            record.root_workflow_id, record.checkpoint.heads[record.root_workflow_id]
        )
    ]
    target = next(
        step for step in original if step.operation.kind == "workflow.next_completed"
    )
    calls = list(probe.current.calls)
    await runner.accept_branch(
        branching.Branch(
            execution_id=identifier,
            step_id=target.id,
            idempotency_key="restart",
            action=branching.Restart(),
        )
    )
    assert await runner.resume(identifier) == list(range(count))
    assert probe.current.calls == calls
    updated = await database.get_execution()
    assert {
        key: value
        for key, value in updated.checkpoint.heads.items()
        if key != record.root_workflow_id
    } == {
        key: value
        for key, value in record.checkpoint.heads.items()
        if key != record.root_workflow_id
    }
    current = [
        step
        async for step in database.replay_path(
            record.root_workflow_id, updated.checkpoint.heads[record.root_workflow_id]
        )
    ]
    replacement = next(
        step for step in current if step.operation.kind == "workflow.next_completed"
    )
    assert replacement.id != target.id
    assert replacement.outcome == target.outcome
    assert await database.get_step(target.id) == target
