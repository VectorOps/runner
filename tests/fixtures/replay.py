from collections.abc import Awaitable, Callable, Mapping
from pathlib import Path
import typing
from typing import Literal, Optional
from uuid import uuid4

import pydantic

from tests.fixtures.history import MemoryHistory
from vectorops.runner import models, tool_policy
from vectorops.runner import tool_authorization, tool_authorization_store
from vectorops.runner.api import composition
from vectorops.runner.runner.context import WorkflowExecutionContext
from vectorops.runner.runner.events import StepEventSink
from vectorops.runner.runner import inbox
from vectorops.runner.runner.mailbox import Mailbox
from vectorops.runner.runner.project import RunnerConfig, RunnerProject
from vectorops.runner.runner.session import ExecutionSession


def allow(
    tool_name: str, properties: Optional[dict[str, pydantic.JsonValue]] = None
) -> tool_policy.ScopedRule:
    return tool_policy.ScopedRule(
        scope="workspace",
        rule=tool_policy.AuthorizationRule(
            id=uuid4(),
            tool_name=tool_name,
            decision="approve",
            properties=properties or {},
        ),
    )


async def run(
    history: MemoryHistory,
    invoke: Callable[[], Awaitable[pydantic.JsonValue]],
    head: Optional[str] = None,
    *,
    mailbox: Optional[Mailbox] = None,
    workspace: Optional[Path] = None,
    tool_policy_version: Optional[Literal[1]] = None,
    policy: Optional[tool_policy.ToolPolicy] = None,
    event_sink: Optional[StepEventSink] = None,
    project: Optional[RunnerProject] = None,
    inboxes: Optional[inbox.Registry] = None,
    rules: Optional[list[tool_policy.ScopedRule]] = None,
) -> models.WorkflowStep:
    async def child(
        configuration: models.Workflow,
        *,
        continue_from_step_id: Optional[str] = None,
        child_heads: Optional[Mapping[str, str]] = None,
    ) -> models.WorkflowStep:
        raise AssertionError("unexpected child")

    workflow = models.Workflow(
        id="test",
        definition_ref="test:workflow",
        tool_policy_version=tool_policy_version,
    )
    await history.save_workflow(workflow)
    session = ExecutionSession(
        history,
        workflow,
        child,
        continue_from_step_id=head,
        child_heads={},
        mailbox=mailbox,
        event_sink=event_sink,
    )
    if project is None:
        project = RunnerProject(
            Path.cwd() if workspace is None else workspace,
            (
                RunnerConfig()
                if workspace is None
                else (await RunnerProject.open(workspace)).config
            ),
        )
    context = WorkflowExecutionContext(
        project,
        workflow,
        session,
        inboxes=inboxes,
    )
    token = composition._CURRENT_CONTEXT.set(context)
    try:
        grants = None
        if mailbox is not None:
            grants = typing.cast(tool_authorization_store.WorkflowGrants, mailbox.store)

        class Data(tool_authorization_store.AuthorizationStore):
            async def rules(
                self, workflow_id: str, tool_name: str
            ) -> list[tool_policy.ScopedRule]:
                return [*(rules or []), *await super().rules(workflow_id, tool_name)]

        authorization = tool_authorization.ToolAuthorizationService(
            Data(project.configuration, grants), policy
        )
        with authorization.activate():
            return await session.run(invoke)
    finally:
        composition._CURRENT_CONTEXT.reset(token)
