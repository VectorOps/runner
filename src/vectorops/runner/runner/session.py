"""Executor-owned operation dispatch, streamed replay, and child coordination."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Awaitable, Callable
from collections.abc import Mapping
from contextlib import aclosing
from dataclasses import dataclass
import typing
import logging
from uuid import uuid4

import pydantic

from vectorops.runner import models
from vectorops.runner.runner import context
from vectorops.runner.runner import handlers
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.checkpoint import CheckpointReporter
from vectorops.runner.runner.errors import ActivityError, ReplayMismatchError
from vectorops.runner.runner.history import WorkflowHistory
from vectorops.runner.runner.events import StepEvent, StepEventSink


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
    task: typing.Optional[asyncio.Task[None]] = None

    def cancel_wait(self, response: asyncio.Future[context.OperationResponse]) -> None:
        if response.cancelled() and self.task is not None:
            self.task.cancel()

    def deliver(self, response: context.OperationResponse) -> None:
        if not self.response.done():
            self.response.set_result(response)


@dataclass
class _Resolution:
    pending: _PendingRequest
    deferred: handlers.DeferredResponse
    result: asyncio.Future[context.OperationResponse]


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
        execution_id: typing.Optional[str] = None,
        event_sink: typing.Optional[StepEventSink] = None,
    ) -> None:
        self.history = history
        self.workflow = workflow
        self.execute_child = execute_child
        self.child_heads = child_heads
        self.replay_head = continue_from_step_id
        self.head: typing.Optional[str] = None
        self.queue: asyncio.Queue[typing.Union[_PendingRequest, _Resolution]] = (
            asyncio.Queue()
        )
        self.resolutions: dict[str, _Resolution] = {}
        self.buffered: typing.Optional[models.WorkflowStep] = None
        self.ordered_replay = False
        self.live = asyncio.Event()
        if continue_from_step_id is None:
            self.live.set()
        self.waiters: set[asyncio.Task[None]] = set()
        self.closed = False
        self.children: dict[str, asyncio.Task[models.WorkflowStep]] = {}
        self.replay: typing.Optional[AsyncGenerator[models.WorkflowStep, None]] = None
        self.replaying = continue_from_step_id is not None
        self.reporter = reporter
        self.execution_id = execution_id
        self.event_sink = event_sink
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
        pending = _PendingRequest(request, asyncio.get_running_loop().create_future())
        pending.response.add_done_callback(pending.cancel_wait)
        self.queue.put_nowait(pending)
        return await pending.response

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
        fatal: asyncio.Future[Exception] = asyncio.get_running_loop().create_future()
        dispatcher = asyncio.create_task(self._dispatch(fatal))
        try:
            await asyncio.wait((task, fatal), return_when=asyncio.FIRST_COMPLETED)
            if fatal.done():
                raise fatal.result()
            self.closed = True
            await self._stop_operations(dispatcher)
            if fatal.done():
                raise fatal.result()
            return await self._complete(task)
        finally:
            self.closed = True
            task.cancel()
            await self._stop_operations(dispatcher)
            await asyncio.gather(task, return_exceptions=True)
            fatal.cancel()
            await replay.aclose()

    async def _dispatch(self, fatal: asyncio.Future[Exception]) -> None:
        """Serialize history writes and replay deliveries, never operation execution."""
        pending: typing.Optional[_PendingRequest] = None
        try:
            while True:
                if self.ordered_replay:
                    step = await self._peek()
                    if step is not None and isinstance(
                        step.operation, models.WorkflowResumeOperation
                    ):
                        resolution = self.resolutions.get(step.operation.step_id)
                        if resolution is None:
                            raise ReplayMismatchError(
                                "resume references an inactive invocation"
                            )
                        if resolution.deferred.step.outcome.kind in (
                            "pending",
                            "streaming",
                            "waiting",
                        ):
                            raise ReplayMismatchError(
                                "resume references an unfinished invocation"
                            )
                        await self.recorded(step.operation)
                        await self._deliver(resolution, replay=True)
                        continue
                item = await self.queue.get()
                if isinstance(item, _Resolution):
                    if not self.replaying:
                        await self._deliver(item)
                    continue
                pending = item
                if pending.response.cancelled():
                    pending = None
                    continue
                response = await self.handlers.dispatch(pending.request)
                if pending.response.cancelled():
                    pending = None
                    continue
                if isinstance(response, handlers.DeferredResponse):
                    resolution = _Resolution(
                        pending, response, asyncio.get_running_loop().create_future()
                    )
                    self.resolutions[response.step.id] = resolution
                    waiter = asyncio.create_task(self._resolve(resolution, fatal))
                    pending.task = waiter
                    self.waiters.add(waiter)
                    waiter.add_done_callback(self.waiters.discard)
                    if not response.step.deferred_delivery:
                        await self._deliver(resolution, replay=True)
                else:
                    pending.deliver(response)
                pending = None
        except Exception as exc:
            if not fatal.done():
                fatal.set_result(exc)
        finally:
            if pending is not None:
                pending.response.cancel()

    async def _resolve(
        self,
        resolution: _Resolution,
        fatal: asyncio.Future[Exception],
    ) -> None:
        try:
            if (
                resolution.deferred.step.deferred_delivery
                and resolution.deferred.step.outcome.kind
                in ("pending", "streaming", "waiting")
            ):
                await self.live.wait()
            response = await resolution.deferred.invoke()
            resolution.result.set_result(response)
            self.queue.put_nowait(resolution)
        except Exception as exc:
            if not fatal.done():
                fatal.set_result(exc)
        finally:
            if not resolution.result.done():
                resolution.result.cancel()
                resolution.pending.response.cancel()

    async def _deliver(self, resolution: _Resolution, *, replay: bool = False) -> None:
        """Persist delivery before waking the caller, independently of completion time."""
        step_id = resolution.deferred.step.id
        if step_id not in self.resolutions:
            return
        if resolution.pending.response.cancelled():
            self.resolutions.pop(step_id)
            return
        response = await asyncio.shield(resolution.result)
        if not replay and resolution.deferred.step.deferred_delivery:
            await self.append(
                models.WorkflowResumeOperation(step_id=step_id), models.StepCompleted()
            )
        resolution.pending.deliver(response)
        self.resolutions.pop(step_id)
        await asyncio.sleep(0)

    async def _stop_operations(self, dispatcher: asyncio.Task[None]) -> None:
        dispatcher.cancel()
        await asyncio.gather(dispatcher, return_exceptions=True)
        tasks = [*self.waiters, *self.children.values()]
        for task in tasks:
            task.cancel()
        for resolution in self.resolutions.values():
            resolution.pending.response.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        while not self.queue.empty():
            item = self.queue.get_nowait()
            if isinstance(item, _PendingRequest):
                item.response.cancel()
        self.waiters.clear()
        self.resolutions.clear()

    async def _complete(
        self, task: asyncio.Task[pydantic.JsonValue]
    ) -> models.WorkflowStep:
        """Validate replay completion or append the workflow's terminal outcome."""
        try:
            outcome: models.StepOutcome = models.StepCompleted(result=task.result())
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
            completion = await self.append(models.WorkflowCompleteOperation(), outcome)
        if await self.recorded(None) is not None:
            raise ReplayMismatchError("workflow returned before consuming history")
        return completion

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
        step = await self._peek()
        self.buffered = None
        if step is None:
            return None
        if operation is not None and step.operation != operation:
            raise ReplayMismatchError("operation differs from recorded history")
        self.ordered_replay = self.ordered_replay or step.deferred_delivery
        self.head = step.id
        if self.reporter is not None:
            self.reporter.record(self.workflow.id, step.id)
        return step

    async def _peek(self) -> typing.Optional[models.WorkflowStep]:
        if self.buffered is not None:
            return self.buffered
        if not self.replaying or self.replay is None:
            return None
        step = await anext(self.replay, None)
        if step is None:
            self.replaying = False
            if self.head != self.replay_head:
                raise ReplayMismatchError("replay path did not reach the selected head")
            self.live.set()
            for resolution in self.resolutions.values():
                if resolution.result.done():
                    self.queue.put_nowait(resolution)
            return None
        if step.workflow_id != self.workflow.id or step.previous_step_id != self.head:
            raise ReplayMismatchError("invalid workflow or predecessor in replay path")
        if (
            not step.deferred_delivery
            and step.outcome.kind in ("streaming", "pending")
            and step.id != self.replay_head
        ):
            raise ReplayMismatchError("unfinished activity must be the replay head")
        self.buffered = step
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
            deferred_delivery=isinstance(
                operation, (models.StepOperation, models.WorkflowJoinOperation)
            ),
        )
        await self.history.append_step(step)
        self._emit(step, created=True)
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
        if step.workflow_id != self.workflow.id:
            raise ValueError("step belongs to another workflow")
        await self.history.update_step(step)
        self._emit(step)
        if self.reporter is not None:
            if self.head is not None:
                self.reporter.record(self.workflow.id, self.head)

    def activity_context(self, step: models.WorkflowStep) -> ActivityExecutionContext:
        return ActivityExecutionContext(self.execution_id, self.workflow.id, step.id)

    def _emit(self, step: models.WorkflowStep, *, created: bool = False) -> None:
        if self.event_sink is None:
            return
        kind: typing.Literal[
            "step.created", "step.updated", "step.completed", "step.failed"
        ]
        if step.outcome.kind == "completed":
            kind = "step.completed"
        elif step.outcome.kind == "failed":
            kind = "step.failed"
        else:
            kind = "step.created" if created else "step.updated"
        try:
            self.event_sink(
                StepEvent(
                    kind=kind,
                    execution_id=self.execution_id,
                    step=step.model_copy(deep=True),
                )
            )
        except Exception:
            logging.getLogger(__name__).exception("Step event sink failed")
