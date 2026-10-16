"""Concurrent execution ownership and transport-independent control operations."""

import asyncio
from collections.abc import Callable
import json
from pathlib import Path
import typing
from uuid import uuid4

import pydantic

from vectorops.runner import diagnostics, input_schema
from vectorops.runner import configuration as config_api, global_config
from vectorops.runner import configuration_suggestions
from vectorops.runner.authentication import models as am
from vectorops.runner.authentication.service import AuthenticationService
from vectorops.runner.mcp import management as mcp_management
from vectorops.runner.mcp.pool import SharedPool
from vectorops.runner.mcp.transport import MCPError
from vectorops.runner.control import protocol as p
from vectorops.runner.control.request_store import RequestStore
from vectorops.runner.control.updates import SnapshotPublisher
from vectorops.runner.runner import events, history_query, mailbox
from vectorops.runner.runner import inspection
from vectorops.runner.runner import inbox
from vectorops.runner.runner import branching
from vectorops.runner.runner.project import RunnerProject
from vectorops.runner.runner.resources import finish_cleanup
from vectorops.runner.runner.state_store import RunnerStateStore
from vectorops.runner.runner.workflow import WorkflowRunner

EventSink: typing.TypeAlias = Callable[[dict[str, pydantic.JsonValue]], None]


class RunnerService:
    def __init__(
        self,
        path: Path,
        *,
        config_dir: typing.Optional[Path] = None,
        workspace_directory: typing.Optional[str] = None,
    ) -> None:
        self.path = path
        self.workspace_directory = workspace_directory
        self.configuration = global_config.GlobalConfiguration(config_dir)
        self.authentication = AuthenticationService(
            self.configuration, event_sink=self._emit
        )
        self.instance_id = uuid4().hex
        self.shutdown_requested = asyncio.Event()
        self.shutdown_mode: typing.Literal["cancel", "drain"] = "cancel"
        self._store: typing.Optional[RunnerStateStore] = None
        self._requests: typing.Optional[RequestStore] = None
        self._runner: typing.Optional[WorkflowRunner] = None
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._branches: dict[str, asyncio.Task[None]] = {}
        self._active: dict[str, p.Active] = {}
        self._references: dict[str, str] = {}
        self._lock = asyncio.Lock()
        self._listeners: set[EventSink] = set()
        self._suspending: set[str] = set()
        self._close_task: typing.Optional[asyncio.Task[None]] = None
        self._logs: typing.Optional[diagnostics.DiagnosticLog] = None
        self.mcp_pool = SharedPool(self.authentication, self._emit)
        self._mcp: typing.Optional[mcp_management.Management] = None
        self._updates: typing.Optional[SnapshotPublisher] = None

    async def __aenter__(self) -> typing.Self:
        await self.configuration.load()
        project = await RunnerProject.open(
            self.path, workspace_directory=self.workspace_directory
        )
        self.path = project.root
        self._mcp = mcp_management.Management(project.configuration, self.mcp_pool)
        self._store = RunnerStateStore(project.data_path, project.database)
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
        self._requests = RequestStore(project.database)
        self._runner = WorkflowRunner(
            project.root,
            workspace_directory=self.workspace_directory,
            event_sink=self._step,
            state_store=self._store,
            mailbox_sink=self._emit,
            diagnostic_log=self._logs,
            authentication=self.authentication,
            mcp_pool=self.mcp_pool,
            project=project,
            change_sink=self._changed,
        )
        self._updates = SnapshotPublisher(
            self._summary,
            self.runner.workflow_state,
            self._emit,
            execution=self.runner.execution_state,
        )
        try:
            await self.runner.initialize_usage()
            candidates = await self.runner.recovery_candidates()
            await self._store.reconcile([candidate.id for candidate in candidates])
            for candidate in candidates:
                accepted = p.Accepted(execution_id=candidate.id)
                self._schedule(accepted, None, None)
        except BaseException:
            await self._updates.close()
            await self._logs.aclose()
            await self._store.__aexit__()
            self._runner = None
            raise
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())
        await finish_cleanup(self._close_task)

    async def _close(self) -> None:
        async with self._lock:
            self.shutdown_requested.set()
            if self.shutdown_mode == "cancel":
                for identifier, active in self._active.items():
                    if active.status == "stopping":
                        continue
                    self.runner.request_suspend(identifier)
                    self._suspending.add(identifier)
                    self._active[identifier] = active.model_copy(
                        update={"status": "stopping"}
                    )
                    if active.status != "starting":
                        self._tasks[identifier].cancel()
        try:
            await asyncio.gather(*self._branches.values())
            await asyncio.gather(*self._tasks.values())
        finally:
            try:
                if self._updates is not None:
                    await self._updates.close()
                await self.mcp.close()
                await self.mcp_pool.close()
                if self._logs is not None:
                    self._logs.emit("runner.stopped")
                    await self._logs.aclose()
            finally:
                if self._store is not None:
                    await self._store.__aexit__()
                self._runner = None
                self._tasks.clear()
                self._active.clear()

    @property
    def mcp(self) -> mcp_management.Management:
        if self._mcp is None:
            raise RuntimeError("service is not open")
        return self._mcp

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
        if isinstance(execution_id, str) and kind in (
            "execution.paused",
            "execution.resumed",
        ):
            active = self._active.get(execution_id)
            if active is not None and active.status != "stopping":
                self._active[execution_id] = active.model_copy(
                    update={
                        "status": "paused" if kind == "execution.paused" else "running"
                    }
                )
        if (
            isinstance(kind, str)
            and kind.startswith(("execution.", "mailbox."))
            and kind not in ("execution.updated", "execution.state_updated")
        ):
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
        if (
            isinstance(execution_id, str)
            and isinstance(kind, str)
            and kind.startswith("execution.")
            and kind not in ("execution.updated", "execution.state_updated")
        ):
            self._changed(execution_id, ())

    def _changed(self, execution_id: str, workflow_ids: tuple[str, ...]) -> None:
        if self._updates is not None and self._listeners:
            self._updates.changed(execution_id, workflow_ids)

    def _step(self, event: events.StepEvent) -> None:
        self._emit(event.model_dump(mode="json"))

    def _stop(self, execution_id: str) -> None:
        active = self._active.get(execution_id)
        if active is None or active.status == "stopping":
            return
        started = active.status != "starting"
        self._active[execution_id] = active.model_copy(update={"status": "stopping"})
        self._changed(execution_id, ())
        if started:
            self._tasks[execution_id].cancel()
        else:
            self.runner.request_stop(execution_id)

    def _schedule(
        self,
        accepted: p.Accepted,
        start: typing.Optional[p.Start],
        reference: typing.Optional[str],
    ) -> None:
        identifier = accepted.execution_id
        if reference is not None:
            self._references[identifier] = reference
        self._active[identifier] = p.Active(execution_id=identifier)
        self._changed(identifier, ())
        self._tasks[identifier] = asyncio.create_task(
            self._execute(accepted, start, reference)
        )

    async def _execute(
        self,
        accepted: p.Accepted,
        start: typing.Optional[p.Start],
        reference: typing.Optional[str],
    ) -> None:
        """Retain task ownership across client disconnects and publish terminal cleanup."""
        status = "completed"
        error: typing.Optional[str] = None

        def started() -> None:
            if self._active[accepted.execution_id].status == "stopping":
                return
            self._active[accepted.execution_id] = p.Active(
                execution_id=accepted.execution_id, status="running"
            )
            self._emit(
                {"kind": "execution.started", "execution_id": accepted.execution_id}
            )

        try:
            if start is not None:
                assert reference is not None
                await self.runner.run(
                    reference,
                    input=start.input,
                    new_execution_id=accepted.execution_id,
                    on_started=started,
                )
            else:
                await self.runner.resume(accepted.execution_id, on_started=started)
        except asyncio.CancelledError:
            status = (
                "suspended"
                if accepted.execution_id in self._suspending
                else "cancelled"
            )
        except Exception as exc:
            status, error = "failed", str(exc)
            with diagnostics.bind(execution_id=accepted.execution_id):
                self.logs.emit(
                    "execution.error", level=diagnostics.Level.ERROR, error=exc
                )
        finally:
            assert self._store is not None
            if accepted.execution_id not in self._branches:
                self._store.scheduled.discard(accepted.execution_id)
            self._active.pop(accepted.execution_id, None)
            self._references.pop(accepted.execution_id, None)
            self._tasks.pop(accepted.execution_id, None)
            self._suspending.discard(accepted.execution_id)
            self._emit(
                {
                    "kind": "execution." + status,
                    "execution_id": accepted.execution_id,
                    "error": error,
                }
            )

    async def _launch(self, params: typing.Union[p.Start, p.Resume]) -> p.Accepted:
        assert self._requests is not None
        assert self._store is not None
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
            reference = None
            start = None
            if isinstance(params, p.Start):
                reference = params.reference
                await (await self.runner.project()).resolver.resolve(reference)
                start = params
                execution_id = uuid4().hex
            else:
                if (
                    params.execution_id in self._active
                    or params.execution_id in self._branches
                ):
                    raise p.RemoteError(
                        "execution_conflict", "execution is already active"
                    )
                execution_id = params.execution_id
                async with self._store.coordination:
                    await self.runner.execution_state(execution_id)
                    self._store.scheduled.add(execution_id)
            accepted = p.Accepted(execution_id=execution_id)
            try:
                await self._requests.save(params.idempotency_key, fingerprint, accepted)
            except BaseException:
                self._store.scheduled.discard(execution_id)
                raise
            self._schedule(accepted, start, reference)
            return accepted

    async def _branch(self, request: p.Branch) -> p.BranchAccepted:
        async with self._lock:
            if self.shutdown_requested.is_set():
                raise p.RemoteError("runner_stopping", "runner is shutting down")
            assert self._store is not None
            async with self._store.coordination:
                await self.runner.execution_state(request.execution_id)
                protected = request.execution_id in self._store.scheduled
                self._store.scheduled.add(request.execution_id)
            try:
                operation = await self.runner.accept_branch(request)
            except input_schema.InputValidationError as exc:
                raise p.RemoteError(
                    "input_validation", str(exc), exc.details()
                ) from exc
            except branching.BranchConflict as exc:
                raise p.RemoteError("branch_conflict", str(exc)) from exc
            finally:
                if not protected:
                    self._store.scheduled.discard(request.execution_id)
            if (
                operation.status == "accepted"
                and request.execution_id not in self._branches
            ):
                assert self._store is not None
                self._store.scheduled.add(request.execution_id)
                previous = self._tasks.get(request.execution_id)
                self._stop(request.execution_id)
                self._branches[request.execution_id] = asyncio.create_task(
                    self._switch_branch(operation, previous)
                )
            return operation.accepted()

    async def _switch_branch(
        self,
        operation: branching.Operation,
        previous: typing.Optional[asyncio.Task[None]],
    ) -> None:
        """Await the old writer without holding the service-wide acceptance lock."""
        identifier = operation.request.execution_id
        try:
            if previous is not None:
                await finish_cleanup(previous)
            async with self._lock:
                assert self._store is not None
                if not self.shutdown_requested.is_set():
                    self._store.scheduled.add(identifier)
                    self._schedule(p.Accepted(execution_id=identifier), None, None)
        finally:
            self._branches.pop(identifier, None)
            if identifier not in self._tasks:
                assert self._store is not None
                self._store.scheduled.discard(identifier)

    async def _summary(self, identifier: str) -> inspection.ExecutionSummary:
        active = self._active.get(identifier)
        if active is not None and active.status == "starting":
            return inspection.ExecutionSummary(
                execution_id=identifier,
                reference=self._references.get(identifier),
                status="starting",
                active=True,
            )
        try:
            summary = await self.runner.execution_summary(identifier)
        except (FileNotFoundError, KeyError):
            if active is None:
                raise
            summary = inspection.ExecutionSummary(
                execution_id=identifier,
                reference=self._references.get(identifier),
                status=active.status,
            )
        active = self._active.get(identifier)
        return summary.model_copy(
            update={
                "active": active is not None,
                "status": (
                    active.status
                    if active is not None and active.status in ("starting", "stopping")
                    else summary.status
                ),
            }
        )

    async def _summaries(self, query: p.SummaryQuery) -> p.SummaryPage:
        identifiers = await self.runner.execution_ids(limit=None)
        persisted = set(identifiers)
        selected = [
            identifier
            for identifier in reversed(tuple(self._active))
            if identifier not in persisted
        ] + identifiers
        if query.active_only:
            selected = [
                identifier for identifier in selected if identifier in self._active
            ]
        if query.after is not None:
            selected = selected[selected.index(query.after) + 1 :]
        return p.SummaryPage(
            executions=[
                await self._summary(identifier)
                for identifier in selected[: query.limit]
            ],
            next_after=(
                selected[query.limit - 1] if len(selected) > query.limit else None
            ),
        )

    async def dispatch(
        self,
        method: str,
        params: dict[str, pydantic.JsonValue],
        *,
        session_id: str = "embedded",
    ) -> pydantic.JsonValue:
        if method in (
            "configuration.get",
            "configuration.set",
            "configuration.reset",
            "configuration.suggest",
        ):
            async with self._lock:
                if self.shutdown_requested.is_set():
                    raise p.RemoteError("runner_stopping", "runner is shutting down")
                configuration = (await self.runner.project()).configuration
                try:
                    if method == "configuration.set":
                        update = config_api.Update.model_validate(params)
                        await configuration.set(
                            update.name, update.value, permanent=update.permanent
                        )
                        return None
                    if method == "configuration.suggest":
                        return configuration_suggestions.suggest(
                            config_api.SuggestQuery.model_validate(params)
                        ).model_dump(mode="json")
                    config_query = config_api.Query.model_validate(params)
                    if method == "configuration.reset":
                        await configuration.reset(config_query.name)
                        return None
                    return configuration.snapshot(config_query).model_dump(mode="json")
                except ValueError:
                    raise p.RemoteError(
                        "invalid_argument", "invalid configuration parameters"
                    ) from None
                except PermissionError:
                    raise p.RemoteError(
                        "read_only", "startup configuration is read-only"
                    ) from None
                except OSError:
                    raise p.RemoteError(
                        "configuration_error", "cannot persist configuration overrides"
                    ) from None
        if method.startswith("mcp."):
            if self.shutdown_requested.is_set():
                raise p.RemoteError("runner_stopping", "runner is shutting down")
            try:
                result = await self.mcp.dispatch(method, params)
                if len(json.dumps(result).encode()) > p.MAX_FRAME // 2:
                    raise MCPError(
                        "MCP management result exceeds the control response limit"
                    )
                return result
            except pydantic.ValidationError:
                raise p.RemoteError(
                    "invalid_argument", "invalid MCP parameters"
                ) from None
            except (MCPError, am.AuthenticationError) as exc:
                raise p.RemoteError("mcp_error", str(exc)) from None
        if method.startswith("auth."):
            if self.shutdown_requested.is_set() and self.shutdown_mode != "drain":
                raise p.RemoteError("runner_stopping", "runner is shutting down")
            try:
                if method == "auth.providers":
                    p.Empty.model_validate(params)
                    return self.authentication.providers().model_dump(mode="json")
                if method == "auth.credentials.list":
                    return (
                        await self.authentication.credentials(
                            am.CredentialListQuery.model_validate(params)
                        )
                    ).model_dump(mode="json")
                if method == "auth.credentials.check":
                    return (
                        await self.authentication.check_credential(
                            am.CredentialQuery.model_validate(params)
                        )
                    ).model_dump(mode="json")
                if method == "auth.credentials.delete":
                    return (
                        await self.authentication.delete_credential(
                            am.CredentialQuery.model_validate(params)
                        )
                    ).model_dump(mode="json")
                if method == "auth.login":
                    login = am.Login.model_validate(params)
                    return (
                        await self.authentication.login(login.provider, login.profile)
                    ).model_dump(mode="json")
                if method == "auth.profile.list":
                    p.Empty.model_validate(params)
                    return (await self.authentication.profiles.listing()).model_dump(
                        mode="json"
                    )
                if method in (
                    "auth.profile.add",
                    "auth.profile.switch",
                    "auth.profile.delete",
                ):
                    profile_name = am.ProfileQuery.model_validate(params).name
                    operations = {
                        "auth.profile.add": self.authentication.profiles.add,
                        "auth.profile.switch": self.authentication.profiles.switch,
                        "auth.profile.delete": self.authentication.profiles.delete,
                    }
                    return (await operations[method](profile_name)).model_dump(
                        mode="json"
                    )
                if method == "auth.list":
                    p.Empty.model_validate(params)
                    return {
                        "requests": [
                            item.model_dump(mode="json")
                            for item in self.authentication.pending()
                        ]
                    }
                if method == "auth.claim":
                    reference = am.Reference.model_validate(params)
                    return self.authentication.claim(
                        reference.id, session_id
                    ).model_dump(mode="json")
                if method == "auth.ready":
                    attempt = am.Attempt.model_validate(params)
                    return {"url": self.authentication.ready(attempt, session_id)}
                if method == "auth.complete":
                    await self.authentication.complete(
                        am.Callback.model_validate(params), session_id
                    )
                    return None
                if method == "auth.cancel":
                    self.authentication.cancel(am.Reference.model_validate(params).id)
                    return None
                if method == "auth.release":
                    self.authentication.release(
                        am.Attempt.model_validate(params), session_id
                    )
                    return None
                if method == "auth.provision":
                    await self.authentication.provision(
                        am.Credential.model_validate(params)
                    )
                    return None
            except pydantic.ValidationError:
                raise p.RemoteError(
                    "invalid_argument", "invalid authentication parameters"
                ) from None
            except am.AuthenticationError as exc:
                raise p.RemoteError("authentication_error", str(exc)) from None
            except am.CredentialStorageError:
                raise p.RemoteError(
                    "credential_storage_error", "cannot access credential store"
                ) from None
        if method == "workflows.describe":
            description_query = p.DescribeWorkflow.model_validate(params)
            return p.json_value(
                await (await self.runner.project()).resolver.describe(
                    description_query.reference
                )
            )
        if method in ("workflows.list", "workflows.search"):
            workflow_query = (
                p.WorkflowSearch.model_validate(params)
                if method == "workflows.search"
                else p.WorkflowSearch(
                    query="", **p.ExecutionList.model_validate(params).model_dump()
                )
            )
            discovered = await (await self.runner.project()).resolver.discover(
                query=workflow_query.query,
                after=workflow_query.after,
                limit=workflow_query.limit,
            )
            return p.json_value(
                p.WorkflowPage(
                    workflows=[
                        p.WorkflowListing(descriptor=descriptor)
                        for descriptor in discovered.workflows
                    ],
                    errors=discovered.errors,
                    next_after=discovered.next_after,
                )
            )
        if method == "executions.summaries":
            return p.json_value(
                await self._summaries(p.SummaryQuery.model_validate(params))
            )
        if method == "executions.summary":
            return p.json_value(
                await self._summary(p.Execution.model_validate(params).execution_id)
            )
        if method == "workflows.state":
            workflow_state = p.WorkflowState.model_validate(params)
            return p.json_value(
                await self.runner.workflow_state(
                    workflow_state.execution_id, workflow_state.workflow_id
                )
            )
        if method in ("runner.usage", "runner.usage.reset"):
            p.Empty.model_validate(params)
            async with self._lock:
                if self.shutdown_requested.is_set():
                    raise p.RemoteError("runner_stopping", "runner is shutting down")
                return p.json_value(
                    await self.runner.runner_usage(reset=method == "runner.usage.reset")
                )
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
                "max_active_executions": None,
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
            selected_active = p.Current.model_validate(params)
            if selected_active.execution_id is not None:
                active = self._active.get(selected_active.execution_id)
            else:
                if len(self._active) > 1:
                    raise p.RemoteError(
                        "execution_conflict",
                        "multiple executions are active; specify execution_id or use executions.active",
                    )
                active = next(iter(self._active.values()), None)
            return None if active is None else p.json_value(active)
        if method == "executions.active":
            listing = p.ExecutionList.model_validate(params)
            identifiers = sorted(
                identifier
                for identifier in self._active
                if listing.after is None or identifier > listing.after
            )
            return p.json_value(
                p.ActivePage(
                    executions=[
                        self._active[identifier]
                        for identifier in identifiers[: listing.limit]
                    ],
                    next_after=(
                        identifiers[listing.limit - 1]
                        if len(identifiers) > listing.limit
                        else None
                    ),
                )
            )
        if method == "executions.start":
            return p.json_value(await self._launch(p.Start.model_validate(params)))
        if method == "executions.resume":
            return p.json_value(await self._launch(p.Resume.model_validate(params)))
        if method == "executions.branch":
            return p.json_value(await self._branch(p.Branch.model_validate(params)))
        if method == "executions.stop":
            selected = p.Execution.model_validate(params)
            async with self._lock:
                if selected.execution_id in self._branches:
                    raise p.RemoteError(
                        "execution_conflict", "branch transition is in progress"
                    )
                if selected.execution_id not in self._active:
                    try:
                        await self.runner.stop_unloaded(selected.execution_id)
                    except (ValueError, KeyError, FileNotFoundError) as exc:
                        raise p.RemoteError(
                            "execution_conflict", "execution is not active"
                        ) from exc
                    self._emit(
                        {
                            "kind": "execution.cancelled",
                            "execution_id": selected.execution_id,
                            "error": None,
                        }
                    )
                    return p.json_value(
                        p.Active(execution_id=selected.execution_id, status="stopping")
                    )
                self._stop(selected.execution_id)
                return p.json_value(self._active[selected.execution_id])
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
        if method.startswith("mailbox.inboxes."):
            execution = inbox.Execution.model_validate(
                {"execution_id": params.get("execution_id")}
            )
            async with self._lock:
                if execution.execution_id in self._branches:
                    raise p.RemoteError(
                        "execution_conflict", "branch transition is in progress"
                    )
                active = self._active.get(execution.execution_id)
                if active is None or active.status == "stopping":
                    raise p.RemoteError(
                        "mailbox_conflict", "execution inboxes are not active"
                    )
                if self.shutdown_requested.is_set() and self.shutdown_mode != "drain":
                    raise p.RemoteError("runner_stopping", "runner is shutting down")
                try:
                    inbox_result: typing.Optional[pydantic.BaseModel]
                    if method == "mailbox.inboxes.list":
                        selected_inboxes = inbox.Execution.model_validate(params)
                        inbox_result = self.runner.list_mailboxes(
                            selected_inboxes.execution_id
                        )
                    elif method == "mailbox.inboxes.inspect":
                        inbox_result = self.runner.inspect_mailbox(
                            inbox.Address.model_validate(params)
                        )
                    elif method == "mailbox.inboxes.send":
                        inbox_result = self.runner.send_message(
                            inbox.Send.model_validate(params)
                        )
                    elif method == "mailbox.inboxes.messages":
                        inbox_result = self.runner.list_messages(
                            inbox.MessageQuery.model_validate(params)
                        )
                    elif method == "mailbox.inboxes.remove":
                        inbox_result = self.runner.remove_message(
                            inbox.MessageRef.model_validate(params)
                        )
                    elif method == "mailbox.inboxes.pop":
                        inbox_result = self.runner.pop_message(
                            inbox.Address.model_validate(params)
                        )
                    else:
                        raise p.RemoteError(
                            "unsupported_method", "unknown method: " + method
                        )
                    return None if inbox_result is None else p.json_value(inbox_result)
                except inbox.InboxConflict as exc:
                    raise p.RemoteError("mailbox_conflict", str(exc)) from exc
        if method == "mailbox.get":
            return p.json_value(
                await self.runner.input_request(
                    mailbox.RequestRef.model_validate(params)
                )
            )
        if method == "mailbox.respond":
            answer = mailbox.Answer.model_validate(params)
            async with self._lock:
                if answer.execution_id in self._branches:
                    raise p.RemoteError(
                        "execution_conflict", "branch transition is in progress"
                    )
                if self.shutdown_requested.is_set() and self.shutdown_mode != "drain":
                    raise p.RemoteError("runner_stopping", "runner is shutting down")
                try:
                    receipt = await self.runner.answer_input(answer)
                    if answer.execution_id not in self._active:
                        record = await self.runner.execution_state(answer.execution_id)
                        if record.status == "running":
                            assert self._store is not None
                            self._store.scheduled.add(answer.execution_id)
                            self._schedule(
                                p.Accepted(execution_id=answer.execution_id), None, None
                            )
                    return p.json_value(receipt)
                except input_schema.InputValidationError as exc:
                    raise p.RemoteError(
                        "input_validation", str(exc), exc.details()
                    ) from exc
                except mailbox.MailboxConflict as exc:
                    raise p.RemoteError("mailbox_conflict", str(exc)) from exc
        raise p.RemoteError("unsupported_method", "unknown method: " + method)
