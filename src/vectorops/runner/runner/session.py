"""Executor-owned operation dispatch, streamed replay, and child coordination."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Awaitable, Callable
from collections.abc import Mapping
from contextlib import aclosing
from dataclasses import dataclass
import typing
from uuid import uuid4

import pydantic

from vectorops.runner import models
from vectorops.runner.runner import context
from vectorops.runner.runner import handlers
from vectorops.runner.runner.checkpoint import CheckpointReporter
from vectorops.runner.runner.errors import ActivityError, ReplayMismatchError
from vectorops.runner.runner.history import WorkflowHistory


class ChildExecutor(typing.Protocol):
    async def __call__(
        self,
        configuration: models.Workflow,
        *,
        continue_from_step_id: typing.Optional[str] = None,
        child_heads: typing.Optional[Mapping[str, str]] = None,
    ) -> models.WorkflowStep: ...


@dataclass
class _PendingRequest:
    request: context.OperationRequest
    response: asyncio.Future[context.OperationResponse]


class ExecutionSession:
    def __init__(
        self,
        history: WorkflowHistory,
        workflow: models.Workflow,
        execute_child: ChildExecutor,
        *,
        continue_from_step_id: typing.Optional[str],
        child_heads: Mapping[str, str],
        reporter: typing.Optional[CheckpointReporter] = None,
    ) -> None:
        self.history = history
        self.workflow = workflow
        self.execute_child = execute_child
        self.child_heads = child_heads
        self.replay_head = continue_from_step_id
        self.head: typing.Optional[str] = None
        self.queue: asyncio.Queue[_PendingRequest] = asyncio.Queue(maxsize=1)
        self.busy = False
        self.closed = False
        self.children: dict[str, asyncio.Task[models.WorkflowStep]] = {}
        self.replay: typing.Optional[AsyncGenerator[models.WorkflowStep, None]] = None
        self.replaying = continue_from_step_id is not None
        self.reporter = reporter
        self.handlers = handlers.HandlerRegistry()
        self.handlers.register(context.ActivityRequest, handlers.ActivityHandler(self))
        self.handlers.register(
            context.StreamingActivityRequest, handlers.StreamingActivityHandler(self)
        )
        self.handlers.register(context.SpawnRequest, handlers.SpawnHandler(self))
        self.handlers.register(context.JoinRequest, handlers.JoinHandler(self))

    async def submit(
        self, request: context.OperationRequest
    ) -> context.OperationResponse:
        if self.closed:
            raise RuntimeError("workflow session is closed")
        if self.busy:
            raise RuntimeError("parallel operations in one workflow are not supported")
        self.busy = True
        response: asyncio.Future[context.OperationResponse] = (
            asyncio.get_running_loop().create_future()
        )
        try:
            await self.queue.put(_PendingRequest(request, response))
            return await response
        finally:
            self.busy = False

    async def _path(self) -> AsyncGenerator[models.WorkflowStep, None]:
        if self.replay_head is not None:
            async with aclosing(
                self.history.replay_path(self.workflow.id, self.replay_head)
            ) as path:
                async for step in path:
                    yield step

    async def run(
        self, invoke: Callable[[], Awaitable[pydantic.JsonValue]]
    ) -> models.WorkflowStep:
        """Drive workflow requests while treating dispatch failures as fatal."""
        await self.history.save_workflow(self.workflow)
        replay = self._path()
        self.replay = replay

        async def run_workflow() -> pydantic.JsonValue:
            return await invoke()

        task = asyncio.create_task(run_workflow())
        pending: typing.Optional[_PendingRequest] = None
        getter: typing.Optional[asyncio.Task[_PendingRequest]] = None
        try:
            while not task.done():
                getter = asyncio.create_task(self.queue.get())
                await asyncio.wait((task, getter), return_when=asyncio.FIRST_COMPLETED)
                if getter.done():
                    pending = getter.result()
                    response = await self.handlers.dispatch(pending.request)
                    if not pending.response.done():
                        pending.response.set_result(response)
                    pending = None
                else:
                    getter.cancel()
                    await asyncio.gather(getter, return_exceptions=True)
                getter = None
            try:
                result = task.result()
                outcome: models.StepOutcome = models.StepCompleted(result=result)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                outcome = self.failure(exc)
            recorded = await self.recorded(models.WorkflowCompleteOperation())
            if recorded is not None:
                if recorded.outcome != outcome:
                    raise ReplayMismatchError("workflow completion differs from replay")
                completion = recorded
            else:
                completion = await self.append(
                    models.WorkflowCompleteOperation(), outcome
                )
            if await self.recorded(None) is not None:
                raise ReplayMismatchError("workflow returned before consuming history")
            return completion
        finally:
            self.closed = True
            task.cancel()
            if getter is not None:
                getter.cancel()
            if pending is not None:
                pending.response.cancel()
            while not self.queue.empty():
                self.queue.get_nowait().response.cancel()
            for child in self.children.values():
                child.cancel()
            tasks = [task, *self.children.values()]
            if getter is not None:
                await asyncio.gather(getter, return_exceptions=True)
            await asyncio.gather(*tasks, return_exceptions=True)
            await replay.aclose()

    @staticmethod
    def failure(exc: Exception) -> models.StepFailed:
        error = (
            exc.error
            if isinstance(exc, ActivityError)
            else models.StepError(type=type(exc).__name__, message=str(exc))
        )
        return models.StepFailed(error=error)

    async def recorded(
        self, operation: typing.Optional[models.WorkflowOperation]
    ) -> typing.Optional[models.WorkflowStep]:
        if not self.replaying or self.replay is None:
            return None
        step = await anext(self.replay, None)
        if step is None:
            self.replaying = False
            if self.head != self.replay_head:
                raise ReplayMismatchError("replay path did not reach the selected head")
            return None
        if step.workflow_id != self.workflow.id or step.previous_step_id != self.head:
            raise ReplayMismatchError("invalid workflow or predecessor in replay path")
        if operation is not None and step.operation != operation:
            raise ReplayMismatchError("operation differs from recorded history")
        if step.outcome.kind == "streaming" and step.id != self.replay_head:
            raise ReplayMismatchError(
                "unfinished streaming activity must be the replay head"
            )
        self.head = step.id
        if self.reporter is not None:
            self.reporter.record(self.workflow.id, step.id)
        return step

    async def append(
        self, operation: models.WorkflowOperation, outcome: models.StepOutcome
    ) -> models.WorkflowStep:
        step = models.WorkflowStep(
            id=uuid4().hex,
            workflow_id=self.workflow.id,
            previous_step_id=self.head,
            operation=operation,
            outcome=outcome,
        )
        await self.history.append_step(step)
        self.head = step.id
        if self.reporter is not None:
            self.reporter.record(self.workflow.id, step.id)
        return step

    def start_child(
        self, workflow: models.Workflow
    ) -> asyncio.Task[models.WorkflowStep]:
        task = self.children.get(workflow.id)
        if task is None:
            task = asyncio.create_task(
                self.execute_child(
                    workflow,
                    continue_from_step_id=self.child_heads.get(workflow.id),
                    child_heads=self.child_heads,
                )
            )
            self.children[workflow.id] = task
        return task

    async def update(self, step: models.WorkflowStep) -> None:
        if step.id != self.head or step.workflow_id != self.workflow.id:
            raise ValueError("only the active streaming step can be updated")
        await self.history.update_step(step)
        if self.reporter is not None:
            self.reporter.record(self.workflow.id, step.id)
