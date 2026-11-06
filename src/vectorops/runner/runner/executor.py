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
from vectorops.runner.environment.local import LocalEnvironment
from vectorops.runner.environment.target import LocalExecutionTarget
from vectorops.runner.worktrees.runtime import ExecutionTargets
from vectorops.runner.mcp.runtime import MCPRuntime
from vectorops.runner.runner.checkpoint import CheckpointReporter
from vectorops.runner.runner.context import WorkflowExecutionContext
from vectorops.runner.runner.history import WorkflowHistory
from vectorops.runner.runner.events import StepEventSink
from vectorops.runner.runner.errors import ReplayMismatchError
from vectorops.runner.runner.execution_state import timestamp
from vectorops.runner.runner.project import RunnerProject
from vectorops.runner.runner.session import ExecutionSession
from vectorops.runner.runner.mailbox import Mailbox
from vectorops.runner.runner.resources import CleanupRegistry, ExecutionResources
from vectorops.runner.runner.resolver import WorkflowCallable, WorkflowEntryPoint
from vectorops.runner.runner.scheduler import Scheduler


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
        environment: typing.Optional[LocalEnvironment] = None,
        resources: typing.Optional[CleanupRegistry] = None,
        mcp: typing.Optional[MCPRuntime] = None,
        scheduler: typing.Optional[Scheduler] = None,
        targets: typing.Optional[ExecutionTargets] = None,
        target: typing.Optional[LocalExecutionTarget] = None,
    ) -> None:
        self.project = project
        self.history = history
        self.reporter = reporter
        self.execution_id = execution_id
        self.event_sink = event_sink
        self.mailbox = mailbox
        self.authentication = authentication
        self.environment = environment
        self.targets = targets
        self.target = target
        self.resources = resources
        self.mcp = mcp
        self.scheduler = scheduler if scheduler is not None else Scheduler()
        self._configured: dict[str, WorkflowEntryPoint] = {}

    async def configure(self, configuration: models.Workflow) -> models.Workflow:
        """Resolve decorator metadata before the immutable configuration is saved."""
        configuration = configuration.model_copy(
            update={
                "tool_policy_version": 1,
                "inbox_version": 1,
                "child_cancellation_version": 1,
                "compaction_version": 2,
                "created_at": configuration.created_at or timestamp(),
            }
        )
        entry = await self._definition(configuration.definition_ref)
        self._configured[configuration.id] = entry
        if not isinstance(entry, composition.WorkflowDefinition):
            return configuration
        if configuration.kind != "workflow" or configuration.version != 1:
            if configuration.kind != entry.kind:
                raise ReplayMismatchError("workflow contract differs from declaration")
        return models.Workflow(
            **{
                **configuration.model_dump(),
                "kind": entry.kind,
                "version": entry.version,
                "name": (
                    configuration.name if configuration.name is not None else entry.name
                ),
                "presentation": entry.presentation.model_copy(deep=True),
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
        inherited_environment: typing.Optional[LocalEnvironment] = None,
        inherited_target: typing.Optional[LocalExecutionTarget] = None,
        inherited_resources: typing.Optional[CleanupRegistry] = None,
    ) -> models.WorkflowStep:
        """Run a session and return its exact workflow completion record."""
        parent_target = inherited_target or self.target
        target = configuration.target or parent_target
        environment = inherited_environment or self.environment
        resources = inherited_resources or self.resources
        if target != parent_target:
            if self.targets is None or target is None:
                raise ValueError("target attachment service is unavailable")
            async with self.targets.attach(target) as selected:
                async with ExecutionResources() as child_resources:
                    return await self._execute(
                        configuration,
                        selected,
                        target,
                        continue_from_step_id,
                        child_heads,
                        child_resources,
                    )
        return await self._execute(
            configuration,
            environment,
            target,
            continue_from_step_id,
            child_heads,
            resources,
        )

    async def _execute(
        self,
        configuration: models.Workflow,
        environment: typing.Optional[LocalEnvironment],
        target: typing.Optional[LocalExecutionTarget],
        continue_from_step_id: typing.Optional[str],
        child_heads: typing.Optional[Mapping[str, str]],
        resources: typing.Optional[CleanupRegistry],
    ) -> models.WorkflowStep:
        async def execute_child(
            configuration: models.Workflow,
            *,
            continue_from_step_id: typing.Optional[str] = None,
            child_heads: typing.Optional[Mapping[str, str]] = None,
        ) -> models.WorkflowStep:
            return await self.execute(
                configuration,
                continue_from_step_id=continue_from_step_id,
                child_heads=child_heads,
                inherited_environment=environment,
                inherited_target=target,
                inherited_resources=resources,
            )

        session = ExecutionSession(
            self.history,
            configuration,
            execute_child,
            continue_from_step_id=continue_from_step_id,
            child_heads={} if child_heads is None else child_heads,
            reporter=self.reporter,
            execution_id=self.execution_id,
            event_sink=self.event_sink,
            configure_workflow=self.configure,
            mailbox=self.mailbox,
            authentication=self.authentication,
            environment=environment,
            targets=self.targets,
            target=target,
            resources=resources,
            mcp=self.mcp,
            scheduler=self.scheduler,
        )
        inboxes = self.mailbox.inboxes if self.mailbox is not None else None
        context = WorkflowExecutionContext(
            self.project,
            configuration,
            session,
            inboxes=(
                inboxes
                if inboxes is not None
                and inboxes.root_workflow_id == configuration.id
                and configuration.inbox_version == 1
                else None
            ),
        )
        try:
            entry = self._configured.pop(configuration.id, None)
            if entry is None:
                entry = await self._definition(configuration.definition_ref)
            if isinstance(entry, composition.WorkflowDefinition):
                if configuration.kind != entry.kind:
                    raise ReplayMismatchError(
                        "workflow contract differs from declaration"
                    )
                workflow = entry.func
            else:
                workflow = entry
            inspect.signature(workflow).bind(context, **configuration.input)
        except Exception as exc:
            await self.history.save_workflow(configuration)
            await session.fail(exc)
            raise

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

    async def fail(
        self,
        configuration: models.Workflow,
        exc: Exception,
        head_step_id: typing.Optional[str],
    ) -> models.WorkflowStep:
        """Finalize runner setup or cleanup errors through the session's failure owner."""
        session = ExecutionSession(
            self.history,
            configuration,
            self.execute,
            continue_from_step_id=head_step_id,
            child_heads={},
            reporter=self.reporter,
            execution_id=self.execution_id,
            event_sink=self.event_sink,
        )
        return await session.fail(exc)
