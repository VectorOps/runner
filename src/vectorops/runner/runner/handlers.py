"""Typed operation handlers behind a checked request-dispatch boundary."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
import typing
from uuid import uuid4

import pydantic

from vectorops.runner import models
from vectorops.runner.runner import context
from vectorops.runner.runner.errors import ReplayMismatchError
from vectorops.runner.runner.history import WorkflowHistory


class HandlerRuntime(typing.Protocol):
    history: WorkflowHistory
    workflow: models.Workflow

    async def recorded(
        self, operation: typing.Optional[models.WorkflowOperation]
    ) -> typing.Optional[models.WorkflowStep]: ...

    async def append(
        self, operation: models.WorkflowOperation, outcome: models.StepOutcome
    ) -> models.WorkflowStep: ...

    def start_child(
        self, workflow: models.Workflow
    ) -> asyncio.Task[models.WorkflowStep]: ...

    def failure(self, exc: Exception) -> models.StepFailed: ...


R = typing.TypeVar("R", bound=context.OperationRequest, contravariant=True)


class OperationHandler(typing.Protocol[R]):
    async def execute(self, request: R) -> context.OperationResponse: ...


class HandlerRegistry:
    def __init__(self) -> None:
        self._handlers: dict[
            str,
            Callable[[context.OperationRequest], Awaitable[context.OperationResponse]],
        ] = {}

    def register(self, request_type: type[R], handler: OperationHandler[R]) -> None:
        """Narrow requests once at the heterogeneous registry boundary."""
        kind = request_type.kind
        if kind in self._handlers:
            raise ValueError(f"handler already registered for {kind}")

        async def dispatch(
            request: context.OperationRequest,
        ) -> context.OperationResponse:
            if not isinstance(request, request_type):
                raise TypeError(f"invalid request type for {kind}")
            return await handler.execute(request)

        self._handlers[kind] = dispatch

    async def dispatch(
        self, request: context.OperationRequest
    ) -> context.OperationResponse:
        return await self._handlers[request.kind](request)


class ActivityHandler:
    def __init__(self, runtime: HandlerRuntime) -> None:
        self.runtime = runtime

    async def execute(
        self, request: context.ActivityRequest
    ) -> context.ActivityResponse:
        step = await self.runtime.recorded(request.operation)
        if step is None:
            try:
                outcome: models.StepOutcome = models.StepCompleted(
                    result=await request.invoke()
                )
            except Exception as exc:
                outcome = self.runtime.failure(exc)
            step = await self.runtime.append(request.operation, outcome)
        if step.outcome.kind == "joined":
            raise ReplayMismatchError("activity has an invalid outcome")
        return context.ActivityResponse(step.outcome)


class SpawnHandler:
    def __init__(self, runtime: HandlerRuntime) -> None:
        self.runtime = runtime

    async def execute(self, request: context.SpawnRequest) -> context.SpawnResponse:
        step = await self.runtime.recorded(None)
        definition = f"{request.workflow_name}:{request.callable_name}"
        if step is not None:
            operation = require_spawn(step)
            child = await self.runtime.history.get_workflow(operation.child_workflow_id)
            if child.definition_ref != definition or child.input != request.input:
                raise ReplayMismatchError("child configuration differs from replay")
        else:
            child = models.Workflow(
                id=uuid4().hex, definition_ref=definition, input=request.input
            )
            await self.runtime.history.save_workflow(child)
            step = await self.runtime.append(
                models.WorkflowSpawnOperation(child_workflow_id=child.id),
                models.StepCompleted(),
            )
            self.runtime.start_child(child)
        return context.SpawnResponse(
            context.ChildWorkflowHandle(self.runtime.workflow.id, child.id, step.id)
        )


def require_spawn(step: models.WorkflowStep) -> models.WorkflowSpawnOperation:
    """Validate a referenced spawn using its type owner's schema."""
    try:
        operation = models.WorkflowSpawnOperation.model_validate(
            step.operation.model_dump(mode="json")
        )
    except pydantic.ValidationError as exc:
        raise ReplayMismatchError("expected a workflow spawn operation") from exc
    if step.outcome.kind != "completed":
        raise ReplayMismatchError("expected a successful workflow spawn")
    return operation


class JoinHandler:
    def __init__(self, runtime: HandlerRuntime) -> None:
        self.runtime = runtime

    async def child_for_spawn(self, spawn_id: str) -> models.Workflow:
        spawn = await self.runtime.history.get_step(spawn_id)
        if spawn.workflow_id != self.runtime.workflow.id:
            raise ReplayMismatchError("join references another workflow's spawn")
        operation = require_spawn(spawn)
        return await self.runtime.history.get_workflow(operation.child_workflow_id)

    async def execute(self, request: context.JoinRequest) -> context.JoinResponse:
        operation = request.operation
        step = await self.runtime.recorded(operation)
        if step is None:
            children = [
                await self.child_for_spawn(spawn_id)
                for spawn_id in operation.spawn_step_ids
            ]
            completions = await asyncio.gather(
                *(self.runtime.start_child(child) for child in children)
            )
            references = [
                models.ChildWorkflowCompletion(
                    spawn_step_id=spawn_id, completion_step_id=completion.id
                )
                for spawn_id, completion in zip(
                    operation.spawn_step_ids, completions, strict=True
                )
            ]
            step = await self.runtime.append(
                operation, models.StepJoined(child_completions=references)
            )
        if step.outcome.kind == "failed":
            return context.JoinResponse([], error=step.outcome.error)
        if step.outcome.kind != "joined":
            raise ReplayMismatchError("join has an invalid outcome")
        outcomes: list[models.StepCompleted | models.StepFailed] = []
        for reference in step.outcome.child_completions:
            child = await self.child_for_spawn(reference.spawn_step_id)
            completion = await self.runtime.history.get_step(
                reference.completion_step_id
            )
            if completion.workflow_id != child.id:
                raise ReplayMismatchError("join references another child's completion")
            try:
                models.WorkflowCompleteOperation.model_validate(
                    completion.operation.model_dump(mode="json")
                )
            except pydantic.ValidationError as exc:
                raise ReplayMismatchError("expected a workflow completion") from exc
            if completion.outcome.kind == "joined":
                raise ReplayMismatchError("join references an invalid child completion")
            outcomes.append(completion.outcome)
        if operation.failure_policy == "propagate":
            for outcome in outcomes:
                if outcome.kind == "failed":
                    return context.JoinResponse(outcomes, error=outcome.error)
        return context.JoinResponse(outcomes)
