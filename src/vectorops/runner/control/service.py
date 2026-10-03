"""Single-root execution ownership and transport-independent control operations."""

import asyncio
from collections.abc import Callable
import json
from pathlib import Path
import typing
from uuid import uuid4

import pydantic

from vectorops.runner import diagnostics
from vectorops.runner.control import protocol as p
from vectorops.runner.control.request_store import RequestStore
from vectorops.runner.runner import events, history_query, mailbox
from vectorops.runner.runner.project import RunnerProject
from vectorops.runner.runner.state_store import RunnerStateStore
from vectorops.runner.runner.workflow import WorkflowRunner

EventSink: typing.TypeAlias = Callable[[dict[str, pydantic.JsonValue]], None]


class RunnerService:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.instance_id = uuid4().hex
        self.shutdown_requested = asyncio.Event()
        self.shutdown_mode: typing.Literal["cancel", "drain"] = "cancel"
        self._store: typing.Optional[RunnerStateStore] = None
        self._requests: typing.Optional[RequestStore] = None
        self._runner: typing.Optional[WorkflowRunner] = None
        self._task: typing.Optional[asyncio.Task[None]] = None
        self._active: typing.Optional[p.Active] = None
        self._lock = asyncio.Lock()
        self._listeners: set[EventSink] = set()
        self._suspending = False
        self._logs: typing.Optional[diagnostics.DiagnosticLog] = None

    async def __aenter__(self) -> typing.Self:
        project = await RunnerProject.open(self.path)
        self.path = project.root
        self._store = RunnerStateStore(project.data_path)
        await self._store.__aenter__()
        self._logs = diagnostics.DiagnosticLog(
            project.data_path, project.config.logging
        )
        try:
            await self._logs.__aenter__()
        except BaseException:
            await self._store.__aexit__()
            raise
        self._logs.emit("runner.started")
        self._requests = RequestStore(project.data_path / "control.sqlite3")
        self._runner = WorkflowRunner(
            project.root,
            event_sink=self._step,
            state_store=self._store,
            mailbox_sink=self._emit,
            diagnostic_log=self._logs,
        )
        try:
            for candidate in await self.runner.recovery_candidates():
                accepted = p.Accepted(execution_id=candidate.id)
                self._active = p.Active(execution_id=candidate.id)
                self._task = asyncio.create_task(self._execute(accepted, None, None))
        except BaseException:
            await self._logs.aclose()
            await self._store.__aexit__()
            self._runner = None
            raise
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        async with self._lock:
            self.shutdown_requested.set()
        try:
            if self._task is not None:
                if self.shutdown_mode == "cancel" and self._active is not None:
                    if self._active.status != "stopping":
                        self._suspending = self.runner.suspend_inputs()
                    if not self._suspending:
                        self._stop()
                try:
                    await asyncio.shield(self._task)
                except asyncio.CancelledError:
                    self._stop()
                    await self._task
                    raise
        finally:
            try:
                if self._logs is not None:
                    self._logs.emit("runner.stopped")
                    await self._logs.aclose()
            finally:
                if self._store is not None:
                    await self._store.__aexit__()
                self._runner = None
                self._task = None

    @property
    def logs(self) -> diagnostics.DiagnosticLog:
        if self._logs is None:
            raise RuntimeError("service is not open")
        return self._logs

    @property
    def runner(self) -> WorkflowRunner:
        if self._runner is None:
            raise RuntimeError("service is not open")
        return self._runner

    def listen(self, listener: EventSink) -> None:
        self._listeners.add(listener)

    def unlisten(self, listener: EventSink) -> None:
        self._listeners.discard(listener)

    def _emit(self, event: dict[str, pydantic.JsonValue]) -> None:
        kind = event.get("kind")
        execution_id = event.get("execution_id")
        if isinstance(kind, str) and kind.startswith(("execution.", "mailbox.")):
            with diagnostics.bind(
                execution_id=execution_id if isinstance(execution_id, str) else None
            ):
                self.logs.emit(
                    kind,
                    level=(
                        diagnostics.Level.ERROR
                        if kind == "execution.failed"
                        else diagnostics.Level.INFO
                    ),
                )
        for listener in tuple(self._listeners):
            listener(event)

    def _step(self, event: events.StepEvent) -> None:
        self._emit(event.model_dump(mode="json"))

    def _stop(self) -> None:
        if self._active is None or self._active.status == "stopping":
            return
        started = self._active.status != "starting"
        self._active = self._active.model_copy(update={"status": "stopping"})
        if started and self._task is not None:
            self._task.cancel()

    async def _execute(
        self,
        accepted: p.Accepted,
        start: typing.Optional[p.Start],
        reference: typing.Optional[str],
    ) -> None:
        """Retain task ownership across client disconnects and publish terminal cleanup."""
        status = "completed"
        error: typing.Optional[str] = None
        try:
            if self._active is not None and self._active.status == "stopping":
                raise asyncio.CancelledError
            self._active = p.Active(
                execution_id=accepted.execution_id, status="running"
            )
            self._emit(
                {"kind": "execution.started", "execution_id": accepted.execution_id}
            )
            if start is not None:
                assert reference is not None
                await self.runner.run(
                    reference,
                    start.callable,
                    input=start.input,
                    new_execution_id=accepted.execution_id,
                )
            else:
                await self.runner.resume(accepted.execution_id)
        except asyncio.CancelledError:
            status = "suspended" if self._suspending else "cancelled"
        except Exception as exc:
            status, error = "failed", str(exc)
            with diagnostics.bind(execution_id=accepted.execution_id):
                self.logs.emit(
                    "execution.error", level=diagnostics.Level.ERROR, error=exc
                )
        finally:
            self._active = None
            self._emit(
                {
                    "kind": "execution." + status,
                    "execution_id": accepted.execution_id,
                    "error": error,
                }
            )

    async def _launch(self, params: typing.Union[p.Start, p.Resume]) -> p.Accepted:
        assert self._requests is not None
        fingerprint = json.dumps(
            [type(params).__name__, params.model_dump(exclude={"idempotency_key"})],
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        async with self._lock:
            previous = await self._requests.lookup(params.idempotency_key, fingerprint)
            if previous is not None:
                return previous
            if self.shutdown_requested.is_set():
                raise p.RemoteError("runner_stopping", "runner is shutting down")
            if self._active is not None:
                raise p.RemoteError(
                    "runner_busy", "a top-level workflow is already active"
                )
            reference = None
            start = None
            if isinstance(params, p.Start):
                reference = await self.runner.resolve_file(params.file)
                start = params
                execution_id = uuid4().hex
            else:
                await self.runner.execution_state(params.execution_id)
                execution_id = params.execution_id
            accepted = p.Accepted(execution_id=execution_id)
            await self._requests.save(params.idempotency_key, fingerprint, accepted)
            self._active = p.Active(execution_id=execution_id)
            self._task = asyncio.create_task(self._execute(accepted, start, reference))
            return accepted

    async def dispatch(
        self, method: str, params: dict[str, pydantic.JsonValue]
    ) -> pydantic.JsonValue:
        if method == "logs.list":
            p.Empty.model_validate(params)
            return p.json_value(await self.logs.listing())
        if method == "logs.download":
            return p.json_value(
                await self.logs.download(diagnostics.LogDownload.model_validate(params))
            )
        if method == "runner.info":
            p.Empty.model_validate(params)
            return {
                "instance_id": self.instance_id,
                "workspace": str(self.path),
                "version": p.VERSION,
                "max_active_executions": 1,
                "event_replay": False,
                "max_frame": p.MAX_FRAME,
                "max_history_page": 500,
            }
        if method == "runner.shutdown":
            shutdown = p.Shutdown.model_validate(params)
            async with self._lock:
                if not self.shutdown_requested.is_set():
                    self.shutdown_mode = shutdown.mode
                    self.shutdown_requested.set()
            return {"status": "stopping"}
        if method == "executions.current":
            p.Empty.model_validate(params)
            return None if self._active is None else p.json_value(self._active)
        if method == "executions.start":
            return p.json_value(await self._launch(p.Start.model_validate(params)))
        if method == "executions.resume":
            return p.json_value(await self._launch(p.Resume.model_validate(params)))
        if method == "executions.stop":
            selected = p.Execution.model_validate(params)
            async with self._lock:
                if (
                    self._active is None
                    or self._active.execution_id != selected.execution_id
                ):
                    raise p.RemoteError(
                        "execution_conflict", "execution is not the active workflow"
                    )
                self._stop()
                return p.json_value(self._active)
        if method == "executions.get":
            selected = p.Execution.model_validate(params)
            return p.json_value(
                await self.runner.execution_state(selected.execution_id)
            )
        if method == "executions.list":
            listing = p.ExecutionList.model_validate(params)
            ids = await self.runner.execution_ids(
                after=listing.after, limit=listing.limit + 1
            )
            return {
                "execution_ids": [identifier for identifier in ids[: listing.limit]],
                "next_after": (
                    ids[listing.limit - 1] if len(ids) > listing.limit else None
                ),
            }
        if method == "history.query":
            query = history_query.HistoryQuery.model_validate(params)
            return p.json_value(await self.runner.query_history(query))
        if method == "mailbox.list":
            return p.json_value(
                await self.runner.pending_inputs(
                    mailbox.RequestQuery.model_validate(params)
                )
            )
        if method == "mailbox.get":
            return p.json_value(
                await self.runner.input_request(
                    mailbox.RequestRef.model_validate(params)
                )
            )
        if method == "mailbox.respond":
            answer = mailbox.Answer.model_validate(params)
            async with self._lock:
                if self.shutdown_requested.is_set():
                    raise p.RemoteError("runner_stopping", "runner is shutting down")
                try:
                    return p.json_value(await self.runner.answer_input(answer))
                except mailbox.MailboxConflict as exc:
                    raise p.RemoteError("mailbox_conflict", str(exc)) from exc
        raise p.RemoteError("unsupported_method", "unknown method: " + method)
