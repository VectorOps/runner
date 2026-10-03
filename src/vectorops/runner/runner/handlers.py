"""Typed operation handlers behind a checked request-dispatch boundary."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import aclosing
from dataclasses import dataclass
from functools import partial
import typing
from uuid import uuid4

import pydantic

from vectorops.runner import models
from vectorops.runner import global_config
from vectorops.runner.authentication.models import CredentialStorageError
from vectorops.runner.runner import context
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner import streaming
from vectorops.runner.runner.errors import ReplayMismatchError
from vectorops.runner.runner.history import WorkflowHistory


class HandlerRuntime(typing.Protocol):
    history: WorkflowHistory
    workflow: models.Workflow

    async def prepare_workflow(self, workflow: models.Workflow) -> models.Workflow: ...

    async def recorded(
        self, operation: typing.Optional[models.WorkflowOperation]
    ) -> typing.Optional[models.WorkflowStep]: ...

    async def append(
        self, operation: models.WorkflowOperation, outcome: models.StepOutcome
    ) -> models.WorkflowStep: ...

    async def update(self, step: models.WorkflowStep) -> None: ...

    def start_child(
        self, workflow: models.Workflow
    ) -> asyncio.Task[models.WorkflowStep]: ...

    def failure(self, exc: Exception) -> models.StepFailed: ...

    def activity_context(
        self, step: models.WorkflowStep
    ) -> ActivityExecutionContext: ...


R = typing.TypeVar("R", bound=context.OperationRequest, contravariant=True)


@dataclass(frozen=True)
class DeferredResponse:
    step: models.WorkflowStep
    invoke: Callable[[], Awaitable[context.OperationResponse]]


async def prepare_response(
    runtime: HandlerRuntime,
    operation: models.WorkflowOperation,
    initial: models.StepOutcome,
    resolve: Callable[[models.WorkflowStep], Awaitable[context.OperationResponse]],
) -> DeferredResponse:
    """Reserve or replay the invocation before creating its deferred execution."""
    step = await runtime.recorded(operation)
    if step is None:
        step = await runtime.append(operation, initial)
    return DeferredResponse(step, partial(resolve, step))


HandlerResponse: typing.TypeAlias = typing.Union[
    context.OperationResponse, DeferredResponse
]


class OperationHandler(typing.Protocol[R]):
    async def execute(self, request: R) -> HandlerResponse: ...


class HandlerRegistry:
    def __init__(self) -> None:
        self._handlers: dict[
            str,
            Callable[[context.OperationRequest], Awaitable[HandlerResponse]],
        ] = {}

    def register(self, request_type: type[R], handler: OperationHandler[R]) -> None:
        """Narrow requests once at the heterogeneous registry boundary."""
        kind = request_type.kind
        if kind in self._handlers:
            raise ValueError(f"handler already registered for {kind}")

        async def dispatch(
            request: context.OperationRequest,
        ) -> HandlerResponse:
            if not isinstance(request, request_type):
                raise TypeError(f"invalid request type for {kind}")
            return await handler.execute(request)

        self._handlers[kind] = dispatch

    async def dispatch(self, request: context.OperationRequest) -> HandlerResponse:
        return await self._handlers[request.kind](request)


class ActivityHandler:
    def __init__(self, runtime: HandlerRuntime) -> None:
        self.runtime = runtime

    async def execute(self, request: context.ActivityRequest) -> DeferredResponse:
        return await prepare_response(
            self.runtime,
            request.operation,
            models.StepPending(),
            partial(self._resolve, request),
        )

    async def _resolve(
        self, request: context.ActivityRequest, step: models.WorkflowStep
    ) -> context.ActivityResponse:
        if step.outcome.kind == "pending":
            try:
                outcome: models.StepOutcome = models.StepCompleted(
                    result=await request.invoke(self.runtime.activity_context(step))
                )
            except (CredentialStorageError, global_config.ConfigurationError):
                raise
            except Exception as exc:
                outcome = self.runtime.failure(exc)
            step = step.model_copy(update={"outcome": outcome})
            await self.runtime.update(step)
        if step.outcome.kind not in ("completed", "failed"):
            raise ReplayMismatchError("activity has an invalid outcome")
        return context.ActivityResponse(step.outcome)


class StreamingActivityHandler:
    def __init__(self, runtime: HandlerRuntime) -> None:
        self.runtime = runtime

    async def execute(
        self, request: context.StreamingActivityRequest
    ) -> DeferredResponse:
        return await prepare_response(
            self.runtime,
            request.operation,
            models.StepStreaming(),
            partial(self._resolve, request),
        )

    async def _resolve(
        self, request: context.StreamingActivityRequest, step: models.WorkflowStep
    ) -> context.ActivityResponse:
        """Replace one activity snapshot per yield and finalize only after exhaustion."""
        if step.outcome.kind in ("completed", "failed"):
            return context.ActivityResponse(step.outcome)
        if step.outcome.kind != "streaming":
            raise ReplayMismatchError("streaming activity has an invalid outcome")
        final_result: pydantic.JsonValue = None
        final_seen = False
        stream = request.invoke(self.runtime.activity_context(step))
        async with aclosing(stream):
            while True:
                try:
                    item = await anext(stream)
                    if not isinstance(
                        item, (streaming.ActivityDelta, streaming.ActivityFinal)
                    ):
                        raise TypeError("streamable activity yielded unsupported item")
                    if final_seen:
                        raise ValueError("streamable activity yielded after final()")
                    if item.kind == "delta":
                        snapshot = models.StepStreaming(state=item.value)
                    elif item.kind == "final":
                        final_result = models.StepCompleted(result=item.value).result
                        final_seen = True
                        snapshot = models.StepStreaming(state=final_result)
                    else:
                        raise TypeError("streamable activity yielded unsupported item")
                except StopAsyncIteration:
                    if final_seen:
                        break
                    outcome = self.runtime.failure(
                        ValueError("streamable activity completed without final()")
                    )
                    await self.runtime.update(
                        step.model_copy(update={"outcome": outcome})
                    )
                    return context.ActivityResponse(outcome)
                except (CredentialStorageError, global_config.ConfigurationError):
                    raise
                except Exception as exc:
                    outcome = self.runtime.failure(exc)
                    await self.runtime.update(
                        step.model_copy(update={"outcome": outcome})
                    )
                    return context.ActivityResponse(outcome)
                step = step.model_copy(update={"outcome": snapshot})
                await self.runtime.update(step)
        completed = models.StepCompleted(result=final_result)
        await self.runtime.update(step.model_copy(update={"outcome": completed}))
        return context.ActivityResponse(completed)


class SpawnHandler:
    def __init__(self, runtime: HandlerRuntime) -> None:
        self.runtime = runtime

    async def execute(self, request: context.SpawnRequest) -> context.SpawnResponse:
        step = await self.runtime.recorded(None)
        definition = f"{request.workflow_name}:{request.callable_name}"
        if step is not None:
            operation = require_spawn(step)
            child = await self.runtime.history.get_workflow(operation.child_workflow_id)
            if (
                child.definition_ref != definition
                or child.input != request.input
                or (
                    request.contract_kind is not None
                    and child.kind != request.contract_kind
                )
                or (request.version is not None and child.version != request.version)
            ):
                raise ReplayMismatchError("child configuration differs from replay")
        else:
            child = models.Workflow(
                id=uuid4().hex,
                definition_ref=definition,
                input=request.input,
                kind=(
                    "workflow"
                    if request.contract_kind is None
                    else request.contract_kind
                ),
                version=1 if request.version is None else request.version,
            )
            child = await self.runtime.prepare_workflow(child)
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

    async def execute(self, request: context.JoinRequest) -> DeferredResponse:
        return await prepare_response(
            self.runtime,
            request.operation,
            models.StepWaiting(),
            partial(self._resolve, request.operation),
        )

    async def _resolve(
        self, operation: models.WorkflowJoinOperation, step: models.WorkflowStep
    ) -> context.JoinResponse:
        if step.outcome.kind == "waiting":
            children = [
                await self.child_for_spawn(spawn_id)
                for spawn_id in operation.spawn_step_ids
            ]
            completions = await asyncio.gather(
                *(asyncio.shield(self.runtime.start_child(child)) for child in children)
            )
            references = [
                models.ChildWorkflowCompletion(
                    spawn_step_id=spawn_id, completion_step_id=completion.id
                )
                for spawn_id, completion in zip(
                    operation.spawn_step_ids, completions, strict=True
                )
            ]
            step = step.model_copy(
                update={"outcome": models.StepJoined(child_completions=references)}
            )
            await self.runtime.update(step)
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
            if completion.outcome.kind not in ("completed", "failed"):
                raise ReplayMismatchError("join references an invalid child completion")
            outcomes.append(completion.outcome)
        if operation.failure_policy == "propagate":
            for outcome in outcomes:
                if outcome.kind == "failed":
                    return context.JoinResponse(outcomes, error=outcome.error)
        return context.JoinResponse(outcomes)
