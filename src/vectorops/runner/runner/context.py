"""Runtime context passed to workflow entry points."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
import typing

import pydantic

from vectorops.runner import models
from vectorops.runner.runner.errors import ActivityError
from vectorops.runner.runner.project import RunnerProject
from vectorops.runner.runner.streaming import StreamItem


@dataclass(frozen=True)
class ActivityRequest:
    kind: typing.ClassVar[typing.Literal["activity"]] = "activity"
    operation: models.StepOperation
    invoke: Callable[[], Awaitable[pydantic.JsonValue]]


@dataclass(frozen=True)
class StreamingActivityRequest:
    kind: typing.ClassVar[typing.Literal["streaming"]] = "streaming"
    operation: models.StepOperation
    invoke: Callable[[], AsyncGenerator[StreamItem, None]]


@dataclass(frozen=True)
class SpawnRequest:
    kind: typing.ClassVar[typing.Literal["spawn"]] = "spawn"
    workflow_name: str
    callable_name: str
    input: dict[str, pydantic.JsonValue]


@dataclass(frozen=True)
class JoinRequest:
    kind: typing.ClassVar[typing.Literal["join"]] = "join"
    operation: models.WorkflowJoinOperation


@dataclass(frozen=True)
class ChildWorkflowHandle:
    parent_workflow_id: str
    workflow_id: str
    spawn_step_id: str


OperationRequest: typing.TypeAlias = typing.Union[
    ActivityRequest, StreamingActivityRequest, SpawnRequest, JoinRequest
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
    outcomes: list[models.StepCompleted | models.StepFailed]
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

    @property
    def data_path(self) -> Path:
        return self.project.data_path

    async def run_activity(
        self,
        operation: models.StepOperation,
        invoke: Callable[[], Awaitable[pydantic.JsonValue]],
    ) -> pydantic.JsonValue:
        response = await self.submitter.submit(ActivityRequest(operation, invoke))
        if response.kind != "activity":
            raise RuntimeError("executor returned an invalid activity response")
        if response.outcome.kind == "failed":
            raise ActivityError(response.outcome.error)
        return response.outcome.result

    async def spawn_workflow(
        self,
        workflow_name: str,
        callable_name: str = "workflow",
        *,
        input: typing.Optional[dict[str, pydantic.JsonValue]] = None,
    ) -> ChildWorkflowHandle:
        response = await self.submitter.submit(
            SpawnRequest(workflow_name, callable_name, {} if input is None else input)
        )
        if response.kind != "spawn":
            raise RuntimeError("executor returned an invalid spawn response")
        return response.handle

    async def run_streaming_activity(
        self,
        operation: models.StepOperation,
        invoke: Callable[[], AsyncGenerator[StreamItem, None]],
    ) -> pydantic.JsonValue:
        response = await self.submitter.submit(
            StreamingActivityRequest(operation, invoke)
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
    ) -> list[models.StepCompleted | models.StepFailed]:
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
