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
from vectorops.runner.api import composition
from vectorops.runner.runner.checkpoint import CheckpointReporter
from vectorops.runner.runner.context import WorkflowExecutionContext
from vectorops.runner.runner.history import WorkflowHistory
from vectorops.runner.runner.events import StepEventSink
from vectorops.runner.runner.project import RunnerProject
from vectorops.runner.runner.session import ExecutionSession

WorkflowCallable: typing.TypeAlias = Callable[..., Awaitable[pydantic.JsonValue]]


class WorkflowExecutor:
    def __init__(
        self,
        project: RunnerProject,
        history: WorkflowHistory,
        reporter: typing.Optional[CheckpointReporter] = None,
        execution_id: typing.Optional[str] = None,
        event_sink: typing.Optional[StepEventSink] = None,
    ) -> None:
        self.project = project
        self.history = history
        self.reporter = reporter
        self.execution_id = execution_id
        self.event_sink = event_sink

    async def execute(
        self,
        configuration: models.Workflow,
        *,
        continue_from_step_id: typing.Optional[str] = None,
        child_heads: typing.Optional[Mapping[str, str]] = None,
    ) -> models.WorkflowStep:
        """Run a session and return its exact workflow completion record."""
        workflow_name, separator, callable_name = (
            configuration.definition_ref.rpartition(":")
        )
        if not separator:
            raise ValueError("workflow definition must identify a file and callable")
        workflow = await self.load(workflow_name, callable_name)
        session = ExecutionSession(
            self.history,
            configuration,
            self.execute,
            continue_from_step_id=continue_from_step_id,
            child_heads={} if child_heads is None else child_heads,
            reporter=self.reporter,
            execution_id=self.execution_id,
            event_sink=self.event_sink,
        )
        context = WorkflowExecutionContext(self.project, configuration, session)
        inspect.signature(workflow).bind(context, **configuration.input)

        async def invoke() -> pydantic.JsonValue:
            token = composition._CURRENT_CONTEXT.set(context)
            try:
                return await workflow(context, **configuration.input)
            finally:
                composition._CURRENT_CONTEXT.reset(token)

        return await session.run(invoke)

    async def load(self, workflow_name: str, callable_name: str) -> WorkflowCallable:
        """Import trusted workflow code without blocking the event loop."""
        return await asyncio.to_thread(self._load, workflow_name, callable_name)

    def _load(self, workflow_name: str, callable_name: str) -> WorkflowCallable:
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
        relative_path = Path(workflow_name)
        if relative_path.suffix == "":
            relative_path = relative_path.with_suffix(".py")
        if relative_path.is_absolute() or relative_path.suffix != ".py":
            raise ValueError("workflow must be a relative Python file")
        path = (self.project.workflows_path / relative_path).resolve()
        if not path.is_relative_to(self.project.workflows_path.resolve()):
            raise ValueError("workflow must remain inside the workflows directory")
        if not path.is_file():
            raise FileNotFoundError(f"workflow file does not exist: {path}")
        module_name = f"_runner_workflow_{uuid4().hex}"
        spec = importlib.util.spec_from_file_location(module_name, path)
        if spec is None or spec.loader is None:
            raise ImportError(f"cannot load workflow file: {path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        try:
            spec.loader.exec_module(module)
            return self._entry_point(module, callable_name)
        finally:
            sys.modules.pop(module_name, None)

    @staticmethod
    def _entry_point(module: ModuleType, callable_name: str) -> WorkflowCallable:
        entry_point = vars(module).get(callable_name)
        if isinstance(entry_point, composition.WorkflowDefinition):
            return typing.cast(WorkflowCallable, entry_point.func)
        if not inspect.iscoroutinefunction(entry_point):
            raise TypeError(f"{callable_name} must be an async workflow function")
        return typing.cast(WorkflowCallable, entry_point)
