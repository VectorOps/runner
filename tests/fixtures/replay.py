from collections.abc import Awaitable, Callable, Mapping
from pathlib import Path
from typing import Literal, Optional

import pydantic

from tests.fixtures.history import MemoryHistory
from vectorops.runner import models, tool_policy
from vectorops.runner.api import composition
from vectorops.runner.runner.context import WorkflowExecutionContext
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
    )
    context = WorkflowExecutionContext(
        RunnerProject(
            Path.cwd() if workspace is None else workspace,
            RunnerConfig(),
            tool_policy.ToolPolicy() if policy is None else policy,
        ),
        workflow,
        session,
    )
    token = composition._CURRENT_CONTEXT.set(context)
    try:
        return await session.run(invoke)
    finally:
        composition._CURRENT_CONTEXT.reset(token)
