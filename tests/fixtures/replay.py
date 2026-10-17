from collections.abc import Awaitable, Callable, Mapping
from pathlib import Path
import typing
from typing import Literal, Optional

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
        authorization = tool_authorization.ToolAuthorizationService(
            tool_authorization_store.AuthorizationStore(project.configuration, grants),
            policy,
        )
        with authorization.activate():
            return await session.run(invoke)
    finally:
        composition._CURRENT_CONTEXT.reset(token)
