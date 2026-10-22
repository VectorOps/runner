"""Runtime context passed to workflow entry points."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
import typing
from uuid import UUID

import pydantic

from vectorops.runner import models, presentation as presentation_models
from vectorops.runner.references import WorkflowReference
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.errors import ActivityError
from vectorops.runner.runner import inbox
from vectorops.runner.runner.project import RunnerProject
from vectorops.runner.runner.streaming import StreamItem


@dataclass(frozen=True)
class ActivityReconstruction:
    """Run orchestration for pending calls and selected recorded outcomes."""

    invoke: Callable[
        [ActivityExecutionContext, models.WorkflowStep], Awaitable[pydantic.JsonValue]
    ]
    replay_when: typing.Optional[Callable[[models.WorkflowStep], bool]] = None

    def applies(self, step: models.WorkflowStep) -> bool:
        return self.replay_when is None or self.replay_when(step)


ActivityInvocation: typing.TypeAlias = typing.Union[
    Callable[[ActivityExecutionContext], Awaitable[pydantic.JsonValue]],
    ActivityReconstruction,
]


@dataclass(frozen=True)
class ActivityRequest:
    kind: typing.ClassVar[typing.Literal["activity"]] = "activity"
    operation: models.StepOperation
    invoke: ActivityInvocation


@dataclass(frozen=True)
class StreamingActivityRequest:
    kind: typing.ClassVar[typing.Literal["streaming"]] = "streaming"
    operation: models.StepOperation
    invoke: Callable[[ActivityExecutionContext], AsyncGenerator[StreamItem, None]]
    input_factory: typing.Optional[Callable[[], dict[str, pydantic.JsonValue]]] = None


@dataclass(frozen=True)
class SpawnRequest:
    kind: typing.ClassVar[typing.Literal["spawn"]] = "spawn"
    reference: str
    input: dict[str, pydantic.JsonValue]
    contract_kind: typing.Optional[str] = None
    version: typing.Optional[int] = None
    description: typing.Optional[str] = None
    worktree_id: typing.Optional[UUID] = None
    presentation: typing.Optional[presentation_models.WorkflowPresentationOverride] = (
        None
    )


@dataclass(frozen=True)
class JoinRequest:
    kind: typing.ClassVar[typing.Literal["join"]] = "join"
    operation: models.WorkflowJoinOperation


@dataclass(frozen=True)
class CancelRequest:
    kind: typing.ClassVar[typing.Literal["cancel"]] = "cancel"
    operation: models.WorkflowCancelOperation


@dataclass(frozen=True)
class InvokeChildRequest:
    kind: typing.ClassVar[typing.Literal["invoke_child"]] = "invoke_child"
    spawn: SpawnRequest


@dataclass(frozen=True)
class ChildWorkflowHandle:
    parent_workflow_id: str
    workflow_id: str
    spawn_step_id: str


OperationRequest: typing.TypeAlias = typing.Union[
    ActivityRequest,
    StreamingActivityRequest,
    SpawnRequest,
    JoinRequest,
    CancelRequest,
    InvokeChildRequest,
]


@dataclass(frozen=True)
class ActivityResponse:
    kind: typing.ClassVar[typing.Literal["activity"]] = "activity"
    outcome: models.StepCompleted | models.StepFailed


@dataclass(frozen=True)
class SpawnResponse:
    kind: typing.ClassVar[typing.Literal["spawn"]] = "spawn"
    handle: ChildWorkflowHandle


@dataclass(frozen=True)
class JoinResponse:
    kind: typing.ClassVar[typing.Literal["join"]] = "join"
    outcomes: list[models.ChildOutcome]
    error: typing.Optional[models.StepError] = None


OperationResponse: typing.TypeAlias = typing.Union[
    ActivityResponse, SpawnResponse, JoinResponse
]


class OperationSubmitter(typing.Protocol):
    async def submit(self, request: OperationRequest) -> OperationResponse: ...


@dataclass(frozen=True)
class WorkflowExecutionContext:
    project: RunnerProject
    workflow: models.Workflow
    submitter: OperationSubmitter
    inboxes: typing.Optional[inbox.Registry] = None

    @property
    def version(self) -> int:
        """Return the durable execution version, unchanged by code upgrades on replay."""
        return self.workflow.version

    @property
    def data_path(self) -> Path:
        return self.project.data_path

    async def run_activity(
        self,
        operation: models.StepOperation,
        invoke: ActivityInvocation,
    ) -> pydantic.JsonValue:
        response = await self.submitter.submit(ActivityRequest(operation, invoke))
        if response.kind != "activity":
            raise RuntimeError("executor returned an invalid activity response")
        if response.outcome.kind == "failed":
            raise ActivityError(response.outcome.error)
        return response.outcome.result

    async def spawn_workflow(
        self,
        reference: str,
        *,
        input: typing.Optional[dict[str, pydantic.JsonValue]] = None,
        kind: typing.Optional[str] = None,
        version: typing.Optional[int] = None,
        description: typing.Optional[str] = None,
        worktree_id: typing.Optional[UUID] = None,
        presentation: typing.Optional[
            presentation_models.WorkflowPresentationOverride
        ] = None,
    ) -> ChildWorkflowHandle:
        response = await self.submitter.submit(
            SpawnRequest(
                str(WorkflowReference.parse(reference)),
                {} if input is None else input,
                kind,
                version,
                description,
                worktree_id,
                presentation,
            )
        )
        if response.kind != "spawn":
            raise RuntimeError("executor returned an invalid spawn response")
        return response.handle

    async def run_streaming_activity(
        self,
        operation: models.StepOperation,
        invoke: Callable[[ActivityExecutionContext], AsyncGenerator[StreamItem, None]],
        *,
        input_factory: typing.Optional[
            Callable[[], dict[str, pydantic.JsonValue]]
        ] = None,
    ) -> pydantic.JsonValue:
        response = await self.submitter.submit(
            StreamingActivityRequest(operation, invoke, input_factory)
        )
        if response.kind != "activity":
            raise RuntimeError("executor returned an invalid activity response")
        if response.outcome.kind == "failed":
            raise ActivityError(response.outcome.error)
        return response.outcome.result

    async def join_workflows(
        self,
        handles: list[ChildWorkflowHandle],
        *,
        failure_policy: typing.Literal["propagate", "collect"] = "propagate",
    ) -> list[models.ChildOutcome]:
        if any(handle.parent_workflow_id != self.workflow.id for handle in handles):
            raise ValueError("child handles must belong to this workflow")
        response = await self.submitter.submit(
            JoinRequest(
                models.WorkflowJoinOperation(
                    spawn_step_ids=[handle.spawn_step_id for handle in handles],
                    failure_policy=failure_policy,
                )
            )
        )
        if response.kind != "join":
            raise RuntimeError("executor returned an invalid join response")
        if response.error is not None:
            raise ActivityError(response.error)
        return response.outcomes

    async def cancel_workflows(
        self,
        handles: list[ChildWorkflowHandle],
        *,
        reason: typing.Optional[str] = None,
    ) -> list[models.ChildOutcome]:
        """Persist cancellation intent and wait for selected child subtrees to settle."""
        if any(handle.parent_workflow_id != self.workflow.id for handle in handles):
            raise ValueError("child handles must belong to this workflow")
        response = await self.submitter.submit(
            CancelRequest(
                models.WorkflowCancelOperation(
                    spawn_step_ids=[handle.spawn_step_id for handle in handles],
                    reason=reason,
                )
            )
        )
        if response.kind != "join":
            raise RuntimeError("executor returned an invalid cancellation response")
        if response.error is not None:
            raise ActivityError(response.error)
        return response.outcomes
