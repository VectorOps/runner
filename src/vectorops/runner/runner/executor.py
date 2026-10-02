"""Resolve and execute asynchronous workflow entry points."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
import importlib.util
import inspect
from pathlib import Path
import sys
import typing
from uuid import uuid4

import pydantic

from vectorops.runner import models
from vectorops.runner.runner.context import WorkflowExecutionContext
from vectorops.runner.runner.project import RunnerProject

WorkflowCallable: typing.TypeAlias = Callable[..., Awaitable[pydantic.JsonValue]]


class WorkflowExecutor:
    def __init__(self, project: RunnerProject) -> None:
        self.project = project

    async def execute(
        self,
        workflow_name: str,
        callable_name: str,
        context: WorkflowExecutionContext,
    ) -> pydantic.JsonValue:
        """Resolve the entry point, bind workflow inputs, and validate its result."""
        workflow = await self.load(workflow_name, callable_name)
        inspect.signature(workflow).bind(context, **context.workflow.input)
        result = await workflow(context, **context.workflow.input)
        return models.StepCompleted(result=result).result

    async def load(self, workflow_name: str, callable_name: str) -> WorkflowCallable:
        """Import trusted workflow code without blocking the event loop."""
        return await asyncio.to_thread(self._load, workflow_name, callable_name)

    def _load(self, workflow_name: str, callable_name: str) -> WorkflowCallable:
        if not workflow_name or not callable_name.isidentifier():
            raise ValueError("workflow name and callable identifier are required")
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
            entry_point = vars(module).get(callable_name)
            if not inspect.iscoroutinefunction(entry_point):
                raise TypeError(f"{callable_name} must be an async workflow function")
            return typing.cast(WorkflowCallable, entry_point)
        finally:
            sys.modules.pop(module_name, None)
