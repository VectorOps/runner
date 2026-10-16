"""Executor-owned operation dispatch, streamed replay, and child coordination."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncGenerator, Awaitable, Callable
from collections.abc import Mapping
from contextlib import aclosing
from contextvars import ContextVar
from dataclasses import dataclass
from functools import partial
import typing
import logging
from uuid import uuid4

import pydantic

from vectorops.runner import models
from vectorops.runner import diagnostics
from vectorops.runner.authentication.service import AuthenticationService
from vectorops.runner.environment.protocol import TargetEnvironment
from vectorops.runner.mcp.runtime import MCPRuntime
from vectorops.runner.runner import context
from vectorops.runner.runner import handlers
from vectorops.runner.runner.mailbox import Mailbox
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.checkpoint import CheckpointReporter
from vectorops.runner.runner.errors import ActivityError, ReplayMismatchError
from vectorops.runner.runner.execution_state import timestamp
from vectorops.runner.runner.history import WorkflowHistory
from vectorops.runner.runner.events import StepEvent, StepEventSink
from vectorops.runner.runner.resources import CleanupRegistry, finish_cleanup
from vectorops.runner.runner.scheduler import Scheduler


class ChildExecutor(typing.Protocol):
    async def __call__(
        self,
        configuration: models.Workflow,
        *,
        continue_from_step_id: typing.Optional[str] = None,
        child_heads: typing.Optional[Mapping[str, str]] = None,
    ) -> models.WorkflowStep: ...


@dataclass
class _Invocation:
    request: context.OperationRequest
    response: asyncio.Future[context.OperationResponse]
    step: typing.Optional[models.WorkflowStep] = None
    task: typing.Optional[asyncio.Task[context.OperationResponse]] = None
    delivery_queued: bool = False
    parent_step_id: typing.Optional[str] = None
    reconstruct: bool = False

    def cancel_wait(self, response: asyncio.Future[context.OperationResponse]) -> None:
        if response.cancelled() and self.task is not None:
            self.task.cancel()

    def deliver(self, response: context.OperationResponse) -> None:
        if not self.response.done():
            self.response.set_result(response)


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
        configure_workflow: typing.Optional[
            Callable[[models.Workflow], Awaitable[models.Workflow]]
        ] = None,
        mailbox: typing.Optional[Mailbox] = None,
        authentication: typing.Optional[AuthenticationService] = None,
        environment: typing.Optional[TargetEnvironment] = None,
        resources: typing.Optional[CleanupRegistry] = None,
        mcp: typing.Optional[MCPRuntime] = None,
        scheduler: typing.Optional[Scheduler] = None,
    ) -> None:
        self.history = history
        self.workflow = workflow
        self.execute_child = execute_child
        self.child_heads = child_heads
        self.replay_head = continue_from_step_id
        self.head: typing.Optional[str] = None
        self.durable_head = continue_from_step_id
        self.queue: asyncio.Queue[typing.Union[_Invocation, str]] = asyncio.Queue()
        self.invocations: dict[str, _Invocation] = {}
        self.buffered: typing.Optional[models.WorkflowStep] = None
        self.ordered_replay = False
        self.deliveries: deque[str] = deque()
        self._history_lock = asyncio.Lock()
        self.live = asyncio.Event()
        if continue_from_step_id is None:
            self.live.set()
        self.closed = False
        self.children: dict[str, asyncio.Task[models.WorkflowStep]] = {}
        self.replay: typing.Optional[AsyncGenerator[models.WorkflowStep, None]] = None
        self.replaying = continue_from_step_id is not None
        self.reporter = reporter
        self.execution_id = execution_id
        self.event_sink = event_sink
        self.configure_workflow = configure_workflow
        self.mailbox = mailbox
        self.pause = None if mailbox is None else mailbox.pause
        self._dispatching = False
        self._parent_step: ContextVar[typing.Optional[str]] = ContextVar(
            "workflow_activity_parent", default=None
        )
        self.authentication = authentication
        self.environment = environment
        self.resources = resources
        self.mcp = mcp
        self.scheduler = scheduler if scheduler is not None else Scheduler()
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
        pending = _Invocation(
            request,
            asyncio.get_running_loop().create_future(),
            parent_step_id=self._parent_step.get(),
        )
        pending.response.add_done_callback(pending.cancel_wait)
        self.queue.put_nowait(pending)
        if self.pause is not None:
            await self.pause.running()
        return await pending.response

    def blocked_inputs(self) -> typing.Optional[set[str]]:
        """Report mailbox dependencies only after replay and deliveries settle."""
        if (
            self.closed
            or self.replaying
            or self._dispatching
            or not self.queue.empty()
            or self.deliveries
            or not self.invocations
            or self.pause is None
        ):
            return None
        requests: set[str] = set()
        for identifier, invocation in self.invocations.items():
            if identifier in self.pause.requests:
                requests.add(identifier)
                continue
            if invocation.reconstruct:
                if not any(
                    child.parent_step_id == identifier
                    for child in self.invocations.values()
                ):
                    return None
                continue
            if (
                invocation.task is None
                or invocation.task.done()
                or invocation.response.done()
            ):
                return None
            if identifier in self.pause.requests:
                requests.add(identifier)
            elif invocation.parent_step_id in self.pause.requests:
                assert invocation.parent_step_id is not None
                requests.add(invocation.parent_step_id)
            elif isinstance(invocation.request, context.JoinRequest):
                if not self.children or any(
                    not task.done() and child not in self.pause.sessions
                    for child, task in self.children.items()
                ):
                    return None
            else:
                return None
        if any(
            not task.done() and child not in self.pause.sessions
            for child, task in self.children.items()
        ):
            return None
        return requests

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
        try:
            await self.history.save_workflow(self.workflow)
        except Exception as exc:
            await self.fail(exc)
            raise
        replay = self._path()
        self.replay = replay

        async def run_workflow() -> pydantic.JsonValue:
            return await invoke()

        task = asyncio.create_task(run_workflow())
        fatal: asyncio.Future[BaseException] = (
            asyncio.get_running_loop().create_future()
        )
        dispatcher = asyncio.create_task(self._dispatch(fatal))
        if self.pause is not None:
            self.pause.sessions[self.workflow.id] = self.blocked_inputs
        try:
            try:
                await asyncio.wait((task, fatal), return_when=asyncio.FIRST_COMPLETED)
            finally:
                self.closed = True
                task.cancel()

                async def teardown() -> None:
                    await self._stop_operations(dispatcher)
                    await asyncio.gather(task, return_exceptions=True)

                await finish_cleanup(asyncio.create_task(teardown()))
            if fatal.done():
                raise fatal.result()
            return await self._complete(task)
        except Exception as exc:
            await self.fail(exc)
            raise
        finally:
            if self.pause is not None:
                self.pause.sessions.pop(self.workflow.id, None)
            fatal.cancel()
            await replay.aclose()

    async def _dispatch(self, fatal: asyncio.Future[BaseException]) -> None:
        """Serialize history writes and replay deliveries, never operation execution."""
        pending: typing.Optional[_Invocation] = None
        try:
            while True:
                if self.deliveries:
                    await self._replay_delivery(self.deliveries.popleft())
                    continue
                if self.ordered_replay:
                    step = await self._peek()
                    self._start_live()
                    if step is not None and isinstance(
                        step.operation, models.WorkflowResumeOperation
                    ):
                        await self.recorded(step.operation)
                        await self._replay_delivery(step.operation.step_id)
                        continue
                self._dispatching = False
                if self.pause is not None:
                    await self.pause.refresh()
                item = await self.queue.get()
                self._dispatching = True
                if self.pause is not None:
                    await self.pause.running()
                if isinstance(item, str):
                    if not self.replaying:
                        invocation = self.invocations.get(item)
                        if invocation is not None:
                            await self._deliver(invocation)
                    continue
                pending = item
                if pending.response.cancelled():
                    pending = None
                    continue
                response = await self.handlers.dispatch(pending.request)
                self._start_live()
                if pending.response.cancelled():
                    pending = None
                    continue
                if isinstance(response, handlers.DeferredResponse):
                    pending.step = response.step
                    pending.reconstruct = response.reconstruct
                    self.invocations[response.step.id] = pending
                    pending.task = asyncio.create_task(self._resolve(response))
                    pending.task.add_done_callback(
                        partial(self._resolved, pending, fatal)
                    )
                    if not response.step.deferred_delivery:
                        await self._deliver(pending, replay=True)
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
        self, deferred: handlers.DeferredResponse
    ) -> context.OperationResponse:
        if (
            not deferred.reconstruct
            and deferred.step.deferred_delivery
            and deferred.step.outcome.kind
            in (
                "pending",
                "streaming",
                "waiting",
            )
        ):
            await self.live.wait()
        with diagnostics.bind(step_id=deferred.step.id):
            token = self._parent_step.set(
                deferred.step.id if deferred.reconstruct else self._parent_step.get()
            )
            try:
                return await deferred.invoke()
            finally:
                self._parent_step.reset(token)

    async def _replay_delivery(self, step_id: str) -> None:
        invocation = self.invocations.get(step_id)
        if invocation is None or invocation.step is None:
            raise ReplayMismatchError("delivery references an inactive invocation")
        if invocation.step.outcome.kind in ("pending", "streaming", "waiting"):
            raise ReplayMismatchError("delivery references an unfinished invocation")
        await self._deliver(invocation, replay=True)

    def _resolved(
        self,
        invocation: _Invocation,
        fatal: asyncio.Future[BaseException],
        task: asyncio.Task[context.OperationResponse],
    ) -> None:
        if task.cancelled():
            invocation.response.cancel()
        elif (exc := task.exception()) is not None:
            if not fatal.done():
                fatal.set_result(exc)
            invocation.response.cancel()
        else:
            self._queue_delivery(invocation)

    def _queue_delivery(self, invocation: _Invocation) -> None:
        task, step = invocation.task, invocation.step
        if (
            not self.closed
            and self.live.is_set()
            and not invocation.delivery_queued
            and task is not None
            and task.done()
            and step is not None
            and step.id in self.invocations
        ):
            invocation.delivery_queued = True
            self.queue.put_nowait(step.id)

    def _start_live(self) -> None:
        """Release unfinished work and enqueue completed calls once replay is exhausted."""
        if self.replaying or self.live.is_set():
            return
        self.live.set()
        for invocation in self.invocations.values():
            self._queue_delivery(invocation)

    async def _deliver(self, invocation: _Invocation, *, replay: bool = False) -> None:
        """Persist delivery before waking the caller, independently of completion time."""
        step, task = invocation.step, invocation.task
        assert step is not None and task is not None
        if invocation.response.cancelled():
            if task.done():
                self.invocations.pop(step.id)
            return
        response = await asyncio.shield(task)
        if not replay and step.deferred_delivery:
            assert self.head is not None
            async with self._history_lock:
                updated = await self.history.record_delivery(self.head, step.id)
                self._emit(updated)
                if self.reporter is not None:
                    self.reporter.record(self.workflow.id, self.head)
        invocation.deliver(response)
        self.invocations.pop(step.id)
        await asyncio.sleep(0)

    async def _stop_operations(self, dispatcher: asyncio.Task[None]) -> None:
        dispatcher.cancel()
        await asyncio.gather(dispatcher, return_exceptions=True)
        operations = [
            invocation.task
            for invocation in self.invocations.values()
            if invocation.task is not None
        ]
        tasks = [*operations, *self.children.values()]
        for task in tasks:
            task.cancel()
        for invocation in self.invocations.values():
            invocation.response.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        while not self.queue.empty():
            item = self.queue.get_nowait()
            if isinstance(item, _Invocation):
                item.response.cancel()
        self.invocations.clear()

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

    async def fail(self, exc: Exception) -> models.WorkflowStep:
        """Record terminal failure after the durable head, never the replay cursor."""
        outcome = self.failure(exc)
        self.head = self.durable_head
        try:
            return await self.append(
                models.WorkflowCompleteOperation(),
                outcome,
                resume_step_id=self.durable_head,
            )
        except Exception as persistence_error:
            diagnostics.log(
                "workflow.completion_failed",
                level=diagnostics.Level.ERROR,
                error=persistence_error,
            )
            raise exc from persistence_error

    @staticmethod
    def failure(exc: Exception) -> models.StepFailed:
        diagnostics.log("operation.failed", level=diagnostics.Level.ERROR, error=exc)
        error = (
            exc.error
            if isinstance(exc, ActivityError)
            else models.StepError(type=type(exc).__name__, message=str(exc))
        )
        return models.StepFailed(error=error)

    async def recorded(
        self, operation: typing.Optional[models.WorkflowOperation]
    ) -> typing.Optional[models.WorkflowStep]:
        if self.deliveries:
            raise ReplayMismatchError("operation submitted before recorded deliveries")
        step = await self._peek()
        if step is None:
            return None
        if operation is not None:
            matches = (
                operation.matches_replay(step.operation)
                if isinstance(operation, models.StepOperation)
                and isinstance(step.operation, models.StepOperation)
                else operation == step.operation
            )
            if not matches:
                raise ReplayMismatchError(
                    f"operation differs from recorded history at {step.id}: "
                    f"expected {step.operation.kind}, received {operation.kind}"
                )
        self.buffered = None
        self.ordered_replay = self.ordered_replay or step.deferred_delivery
        self.deliveries.extend(step.deliveries)
        self.head = step.id
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
        self,
        operation: models.WorkflowOperation,
        outcome: models.StepOutcome,
        *,
        resume_step_id: typing.Optional[str] = None,
    ) -> models.WorkflowStep:
        now = timestamp()
        step = models.WorkflowStep(
            id=uuid4().hex,
            workflow_id=self.workflow.id,
            previous_step_id=self.head,
            operation=operation,
            outcome=outcome,
            resume_step_id=resume_step_id,
            created_at=(
                self.workflow.created_at
                if isinstance(operation, models.WorkflowCompleteOperation)
                else now
            ),
            completed_at=(
                now if outcome.kind in ("completed", "failed", "joined") else None
            ),
            deferred_delivery=isinstance(
                operation, (models.StepOperation, models.WorkflowJoinOperation)
            ),
        )
        await self.history.append_step(step)
        self.head = step.id
        self.durable_head = step.id
        self._emit(step, created=True)
        if self.reporter is not None:
            self.reporter.record(self.workflow.id, step.id)
        return step

    async def prepare_workflow(self, workflow: models.Workflow) -> models.Workflow:
        if self.configure_workflow is None:
            configured = workflow
        else:
            configured = await self.configure_workflow(workflow)
        return configured.model_copy(
            update={"tool_policy_version": self.workflow.tool_policy_version}
        )

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
        async with self._history_lock:
            previous = await self.history.get_step(step.id)
            step = step.model_copy(
                update={
                    "authorized": (
                        previous.authorized
                        if previous.authorized is True
                        else step.authorized
                    ),
                    "revision": previous.revision + 1,
                    "deliveries": previous.deliveries,
                    "created_at": previous.created_at,
                    "completed_at": (
                        previous.completed_at or timestamp()
                        if step.outcome.kind in ("completed", "failed", "joined")
                        else None
                    ),
                }
            )
            await self.history.update_step(step)
            self._emit(step)
        if self.reporter is not None and self.durable_head is not None:
            self.reporter.record(self.workflow.id, self.durable_head)

    def activity_context(self, step: models.WorkflowStep) -> ActivityExecutionContext:
        async def authorize() -> None:
            current = await self.history.get_step(step.id)
            if current.workflow_id != self.workflow.id:
                raise ValueError("step belongs to another workflow")
            if current.authorized is not True:
                await self.update(current.authorize())

        return ActivityExecutionContext(
            self.execution_id,
            self.workflow.id,
            step.id,
            self.mailbox,
            self.authentication,
            self.environment,
            self.resources,
            self.mcp,
            authorized=step.authorized is True,
            persist_authorization=authorize,
            created_at=step.created_at,
            scheduler=self.scheduler,
        )

    def _emit(self, step: models.WorkflowStep, *, created: bool = False) -> None:
        kind: typing.Literal[
            "step.created", "step.updated", "step.completed", "step.failed"
        ]
        if step.outcome.kind == "completed":
            kind = "step.completed"
        elif step.outcome.kind == "failed":
            kind = "step.failed"
        else:
            kind = "step.created" if created else "step.updated"
        with diagnostics.bind(
            execution_id=self.execution_id,
            workflow_id=step.workflow_id,
            step_id=step.id,
        ):
            diagnostics.log(
                kind,
                level=(
                    diagnostics.Level.ERROR
                    if kind == "step.failed"
                    else diagnostics.Level.DEBUG
                ),
                fields={
                    "operation": step.operation.kind,
                    "outcome": step.outcome.kind,
                    "revision": step.revision,
                },
            )
        if self.event_sink is None:
            return
        try:
            self.event_sink(
                StepEvent(
                    kind=kind,
                    execution_id=self.execution_id,
                    step=step.model_copy(deep=True),
                )
            )
        except Exception as exc:
            diagnostics.log(
                "step.sink_failed", level=diagnostics.Level.ERROR, error=exc
            )
            logging.getLogger(__name__).exception("Step event sink failed")
