"""Resolve and execute asynchronous workflow entry points."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from collections.abc import Mapping
import importlib.util
import importlib
import inspect
from pathlib import Path
import sys
import typing
from types import ModuleType
from uuid import uuid4

import pydantic

from vectorops.runner import models
from vectorops.runner import diagnostics
from vectorops.runner.api import composition
from vectorops.runner.authentication.service import AuthenticationService
from vectorops.runner.runner.checkpoint import CheckpointReporter
from vectorops.runner.runner.context import WorkflowExecutionContext
from vectorops.runner.runner.history import WorkflowHistory
from vectorops.runner.runner.events import StepEventSink
from vectorops.runner.runner.errors import ReplayMismatchError
from vectorops.runner.runner.project import RunnerProject
from vectorops.runner.runner.session import ExecutionSession
from vectorops.runner.runner.mailbox import Mailbox

WorkflowCallable: typing.TypeAlias = Callable[..., Awaitable[pydantic.JsonValue]]
WorkflowEntryPoint: typing.TypeAlias = typing.Union[
    WorkflowCallable, composition.WorkflowDefinition[..., pydantic.JsonValue]
]


class WorkflowExecutor:
    def __init__(
        self,
        project: RunnerProject,
        history: WorkflowHistory,
        reporter: typing.Optional[CheckpointReporter] = None,
        execution_id: typing.Optional[str] = None,
        event_sink: typing.Optional[StepEventSink] = None,
        mailbox: typing.Optional[Mailbox] = None,
        authentication: typing.Optional[AuthenticationService] = None,
    ) -> None:
        self.project = project
        self.history = history
        self.reporter = reporter
        self.execution_id = execution_id
        self.event_sink = event_sink
        self.mailbox = mailbox
        self.authentication = authentication
        self._definitions: dict[str, WorkflowEntryPoint] = {}

    async def configure(self, configuration: models.Workflow) -> models.Workflow:
        """Resolve decorator metadata before the immutable configuration is saved."""
        entry = await self._definition(configuration.definition_ref)
        if not isinstance(entry, composition.WorkflowDefinition):
            return configuration
        if configuration.kind != "workflow" or configuration.version != 1:
            if (configuration.kind, configuration.version) != (
                entry.kind,
                entry.version,
            ):
                raise ReplayMismatchError("workflow contract differs from declaration")
        return models.Workflow(
            **{
                **configuration.model_dump(),
                "kind": entry.kind,
                "version": entry.version,
            }
        )

    async def _definition(self, reference: str) -> WorkflowEntryPoint:
        if reference not in self._definitions:
            workflow_name, separator, callable_name = reference.rpartition(":")
            if not separator:
                raise ValueError(
                    "workflow definition must identify a file and callable"
                )
            self._definitions[reference] = await asyncio.to_thread(
                self._load, workflow_name, callable_name
            )
        return self._definitions[reference]

    async def execute(
        self,
        configuration: models.Workflow,
        *,
        continue_from_step_id: typing.Optional[str] = None,
        child_heads: typing.Optional[Mapping[str, str]] = None,
    ) -> models.WorkflowStep:
        """Run a session and return its exact workflow completion record."""
        entry = await self._definition(configuration.definition_ref)
        if isinstance(entry, composition.WorkflowDefinition):
            if (configuration.kind, configuration.version) != (
                entry.kind,
                entry.version,
            ):
                raise ReplayMismatchError("workflow contract differs from declaration")
            workflow = entry.func
        else:
            workflow = entry
        session = ExecutionSession(
            self.history,
            configuration,
            self.execute,
            continue_from_step_id=continue_from_step_id,
            child_heads={} if child_heads is None else child_heads,
            reporter=self.reporter,
            execution_id=self.execution_id,
            event_sink=self.event_sink,
            configure_workflow=self.configure,
            mailbox=self.mailbox,
            authentication=self.authentication,
        )
        context = WorkflowExecutionContext(self.project, configuration, session)
        inspect.signature(workflow).bind(context, **configuration.input)

        async def invoke() -> pydantic.JsonValue:
            token = composition._CURRENT_CONTEXT.set(context)
            try:
                return await workflow(context, **configuration.input)
            finally:
                composition._CURRENT_CONTEXT.reset(token)

        with diagnostics.bind(
            execution_id=self.execution_id, workflow_id=configuration.id
        ):
            diagnostics.log(
                "workflow.started", fields={"replay": continue_from_step_id is not None}
            )
            try:
                completion = await session.run(invoke)
            except asyncio.CancelledError:
                diagnostics.log("workflow.cancelled")
                raise
            except Exception as exc:
                diagnostics.log(
                    "workflow.failed", level=diagnostics.Level.ERROR, error=exc
                )
                raise
            diagnostics.log(
                "workflow." + completion.outcome.kind,
                level=(
                    diagnostics.Level.ERROR
                    if completion.outcome.kind == "failed"
                    else diagnostics.Level.INFO
                ),
            )
            return completion

    async def load(self, workflow_name: str, callable_name: str) -> WorkflowCallable:
        """Import trusted workflow code without blocking the event loop."""
        entry = await self._definition(f"{workflow_name}:{callable_name}")
        return (
            entry.func if isinstance(entry, composition.WorkflowDefinition) else entry
        )

    def _load(self, workflow_name: str, callable_name: str) -> WorkflowEntryPoint:
        if not workflow_name or not callable_name.isidentifier():
            raise ValueError("workflow name and callable identifier are required")
        if workflow_name.startswith("python:"):
            module_name = workflow_name.removeprefix("python:")
            if not module_name or not all(
                part.isidentifier() for part in module_name.split(".")
            ):
                raise ValueError("workflow module must be an importable module name")
            return self._entry_point(
                importlib.import_module(module_name), callable_name
            )
        workspace_file = workflow_name.startswith("workspace:")
        base = self.project.root if workspace_file else self.project.workflows_path
        relative_path = Path(
            workflow_name.removeprefix("workspace:")
            if workspace_file
            else workflow_name
        )
        if relative_path.suffix == "":
            relative_path = relative_path.with_suffix(".py")
        if relative_path.is_absolute() or relative_path.suffix != ".py":
            raise ValueError("workflow must be a relative Python file")
        path = (base / relative_path).resolve()
        if not path.is_relative_to(base.resolve()):
            raise ValueError("workflow must remain inside its source directory")
        if not path.is_file():
            raise FileNotFoundError(f"workflow file does not exist: {path}")
        module_name = f"_runner_workflow_{uuid4().hex}"
        spec = importlib.util.spec_from_file_location(module_name, path)
        if spec is None or spec.loader is None:
            raise ImportError(f"cannot load workflow file: {path}")
        module = importlib.util.module_from_spec(spec)
        module.__dict__["__runner_contract_module__"] = (
            path.relative_to(base.resolve())
            .with_suffix("")
            .as_posix()
            .replace("/", ".")
        )
        sys.modules[module_name] = module
        try:
            spec.loader.exec_module(module)
            return self._entry_point(module, callable_name)
        finally:
            sys.modules.pop(module_name, None)

    @staticmethod
    def _entry_point(module: ModuleType, callable_name: str) -> WorkflowEntryPoint:
        entry_point = vars(module).get(callable_name)
        if isinstance(entry_point, composition.WorkflowDefinition):
            return typing.cast(
                composition.WorkflowDefinition[..., pydantic.JsonValue], entry_point
            )
        if not inspect.iscoroutinefunction(entry_point):
            raise TypeError(f"{callable_name} must be an async workflow function")
        return typing.cast(WorkflowCallable, entry_point)
