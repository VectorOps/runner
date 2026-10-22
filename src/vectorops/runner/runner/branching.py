"""Exclusive replay boundaries and durable branch operation contracts."""

from collections.abc import Callable
import typing
from uuid import uuid4

import pydantic

from vectorops.runner import models
from vectorops.runner.runner import execution_state, mailbox


class Model(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class Restart(Model):
    kind: typing.Literal["restart"] = "restart"


class InsertInput(Model):
    kind: typing.Literal["insert_input"] = "insert_input"
    response: pydantic.JsonValue


Action: typing.TypeAlias = typing.Annotated[
    typing.Union[Restart, InsertInput], pydantic.Field(discriminator="kind")
]


class Branch(Model):
    execution_id: str = pydantic.Field(pattern=r"^[A-Za-z0-9_-]+$", max_length=128)
    step_id: str = pydantic.Field(min_length=1, max_length=128)
    idempotency_key: str = pydantic.Field(min_length=1, max_length=256)
    action: Action


class Accepted(Model):
    execution_id: str
    operation_id: str
    status: typing.Literal["accepted"] = "accepted"


class Operation(Model):
    id: str = pydantic.Field(default_factory=lambda: uuid4().hex)
    request: Branch
    status: typing.Literal["accepted", "applied", "failed"] = "accepted"
    input_step_id: typing.Optional[str] = None
    error: typing.Optional[str] = None

    def accepted(self) -> Accepted:
        return Accepted(execution_id=self.request.execution_id, operation_id=self.id)


class Plan(Model):
    heads: dict[str, str]
    retained_step_ids: set[str]
    input_step: typing.Optional[models.WorkflowStep] = None
    input_request: typing.Optional[mailbox.StoredInputRequest] = None


class BranchConflict(ValueError):
    pass


def plan(
    operation: Operation,
    execution: execution_state.ExecutionRecord,
    get_step: Callable[[str], models.WorkflowStep],
    get_input: Callable[[str], mailbox.StoredInputRequest],
) -> Plan:
    """Select a closed root prefix, retaining only reachable completed children."""
    request = operation.request
    if request.execution_id != execution.id:
        raise BranchConflict("execution identity mismatch")
    target = get_step(request.step_id)
    if target.workflow_id != execution.root_workflow_id:
        raise BranchConflict("branch targets must belong to the root workflow")
    if target.operation.kind == "workflow.resume":
        raise BranchConflict("restart the invocation, not its delivery marker")
    retained: set[str] = set()
    heads: dict[str, str] = {}

    def path(workflow_id: str, head: typing.Optional[str]) -> list[models.WorkflowStep]:
        steps: list[models.WorkflowStep] = []
        seen: set[str] = set()
        while head is not None:
            if head in seen:
                raise BranchConflict("cycle in branch prefix")
            seen.add(head)
            step = get_step(head)
            if step.workflow_id != workflow_id:
                raise BranchConflict("branch prefix crosses workflows")
            steps.append(step)
            head = step.previous_step_id
        return list(reversed(steps))

    def retain(
        workflow_id: str,
        head: typing.Optional[str],
        join_target: typing.Optional[models.WorkflowStep] = None,
        *,
        resume: bool = False,
    ) -> None:
        if workflow_id in heads:
            if heads[workflow_id] != head:
                raise BranchConflict("conflicting child completion heads")
            return
        steps = path(workflow_id, head)
        if head is not None:
            heads[workflow_id] = head
        spawns: dict[str, str] = {}
        joined: dict[str, str] = {}
        outstanding: set[str] = set()
        for step in steps:
            retained.add(step.id)
            if not resume and step.outcome.kind in ("pending", "streaming", "waiting"):
                raise BranchConflict("branch prefix contains unfinished work")
            if step.deferred_delivery:
                outstanding.add(step.id)
            outstanding.difference_update(step.deliveries)
            if step.operation.kind == "workflow.resume":
                delivery = typing.cast(models.WorkflowResumeOperation, step.operation)
                outstanding.discard(delivery.step_id)
            if step.operation.kind == "workflow.spawn":
                spawn = typing.cast(models.WorkflowSpawnOperation, step.operation)
                spawns[step.id] = spawn.child_workflow_id
            if step.outcome.kind == "joined":
                for child in step.outcome.child_completions:
                    joined[child.spawn_step_id] = child.completion_step_id
        if outstanding and not resume:
            raise BranchConflict("branch prefix contains undelivered work")
        unjoined = set(spawns) - set(joined)
        if resume:
            for spawn_id in unjoined:
                child_id = spawns[spawn_id]
                retain(child_id, execution.checkpoint.heads.get(child_id), resume=True)
            unjoined = set()
        if unjoined:
            if join_target is None or join_target.operation.kind != "workflow.join":
                raise BranchConflict("restart at the outstanding child's spawn")
            join = typing.cast(models.WorkflowJoinOperation, join_target.operation)
            if not unjoined.issubset(join.spawn_step_ids):
                raise BranchConflict("restart at the outstanding child's spawn")
            recorded = (
                {
                    item.spawn_step_id: item.completion_step_id
                    for item in join_target.outcome.child_completions
                }
                if join_target.outcome.kind == "joined"
                else {}
            )
            for spawn_id in unjoined:
                child_id = spawns[spawn_id]
                completion_id = recorded.get(
                    spawn_id
                ) or execution.checkpoint.heads.get(child_id)
                if completion_id is None:
                    raise BranchConflict("restart the unfinished child at its spawn")
                completion = get_step(completion_id)
                if (
                    completion.workflow_id != child_id
                    or completion.operation.kind != "workflow.complete"
                    or completion.outcome.kind != "completed"
                ):
                    raise BranchConflict(
                        "restart the failed or unfinished child at its spawn"
                    )
                joined[spawn_id] = completion_id
        for spawn_id, completion_id in joined.items():
            if spawn_id not in spawns:
                raise BranchConflict("join references a spawn outside the prefix")
            completion = get_step(completion_id)
            if completion.operation.kind != "workflow.complete":
                raise BranchConflict("join does not reference a child completion")
            retain(
                spawns[spawn_id],
                completion_id,
                resume=resume or completion.outcome.kind == "cancelled",
            )

    retain(
        target.workflow_id,
        target.previous_step_id,
        target,
        resume=target.resume_step_id is not None and request.action.kind == "restart",
    )
    result = Plan(heads=heads, retained_step_ids=retained)
    if request.action.kind == "insert_input":
        if target.operation.kind != mailbox.KIND:
            raise BranchConflict(
                "input insertion requires a recorded root mailbox.input boundary"
            )
        spec = get_input(target.id).specification()
        spec.validate_response(request.action.response)
        identifier = operation.input_step_id
        assert identifier is not None
        now = execution_state.timestamp()
        step = models.WorkflowStep(
            id=identifier,
            workflow_id=target.workflow_id,
            previous_step_id=target.previous_step_id,
            operation=target.operation,
            outcome=models.StepCompleted(result=request.action.response),
            created_at=now,
            completed_at=now,
            deferred_delivery=True,
            deliveries=[identifier],
        )
        answered = mailbox.StoredInputRequest(
            **spec.model_dump(),
            request_id=identifier,
            execution_id=execution.id,
            workflow_id=target.workflow_id,
            status="answered",
            response=request.action.response,
            idempotency_key=request.idempotency_key,
            created_at=now,
            answered_at=now,
        )
        return result.model_copy(
            update={
                "heads": {**heads, target.workflow_id: identifier},
                "input_step": step,
                "input_request": answered,
            }
        )
    return result
