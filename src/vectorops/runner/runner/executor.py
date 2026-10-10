"""Resolve and execute asynchronous workflow entry points."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
import inspect
import typing

import pydantic

from vectorops.runner import models
from vectorops.runner import diagnostics
from vectorops.runner.api import composition, definitions
from vectorops.runner.authentication.service import AuthenticationService
from vectorops.runner.environment.protocol import TargetEnvironment
from vectorops.runner.mcp.runtime import MCPRuntime
from vectorops.runner.runner.checkpoint import CheckpointReporter
from vectorops.runner.runner.context import WorkflowExecutionContext
from vectorops.runner.runner.history import WorkflowHistory
from vectorops.runner.runner.events import StepEventSink
from vectorops.runner.runner.errors import ReplayMismatchError
from vectorops.runner.runner.project import RunnerProject
from vectorops.runner.runner.session import ExecutionSession
from vectorops.runner.runner.mailbox import Mailbox
from vectorops.runner.runner.resources import CleanupRegistry
from vectorops.runner.runner.resolver import WorkflowCallable, WorkflowEntryPoint


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
        environment: typing.Optional[TargetEnvironment] = None,
        resources: typing.Optional[CleanupRegistry] = None,
        mcp: typing.Optional[MCPRuntime] = None,
    ) -> None:
        self.project = project
        self.history = history
        self.reporter = reporter
        self.execution_id = execution_id
        self.event_sink = event_sink
        self.mailbox = mailbox
        self.authentication = authentication
        self.environment = environment
        self.resources = resources
        self.mcp = mcp
        self._configured: dict[str, WorkflowEntryPoint] = {}

    async def configure(self, configuration: models.Workflow) -> models.Workflow:
        """Resolve decorator metadata before the immutable configuration is saved."""
        configuration = configuration.model_copy(update={"tool_policy_version": 1})
        entry = await self._definition(configuration.definition_ref)
        self._configured[configuration.id] = entry
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
        return await self.project.resolver.resolve(reference)

    async def execute(
        self,
        configuration: models.Workflow,
        *,
        continue_from_step_id: typing.Optional[str] = None,
        child_heads: typing.Optional[Mapping[str, str]] = None,
    ) -> models.WorkflowStep:
        """Run a session and return its exact workflow completion record."""
        entry = self._configured.pop(configuration.id, None)
        if entry is None:
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
            environment=self.environment,
            resources=self.resources,
            mcp=self.mcp,
        )
        context = WorkflowExecutionContext(self.project, configuration, session)
        inspect.signature(workflow).bind(context, **configuration.input)

        async def invoke() -> pydantic.JsonValue:
            token = composition._CURRENT_CONTEXT.set(context)
            try:
                arguments = definitions.validated_input(workflow, configuration.input)
                return typing.cast(
                    pydantic.JsonValue, await workflow(context, **arguments)
                )
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

    async def load(self, reference: str) -> WorkflowCallable:
        """Import trusted workflow code without blocking the event loop."""
        entry = await self._definition(reference)
        return (
            entry.func if isinstance(entry, composition.WorkflowDefinition) else entry
        )
