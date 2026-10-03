from collections.abc import Awaitable, Callable, Mapping
from pathlib import Path
from typing import Optional

import pydantic

from tests.fixtures.history import MemoryHistory
from vectorops.runner import models
from vectorops.runner.api import composition
from vectorops.runner.runner.context import WorkflowExecutionContext
from vectorops.runner.runner.project import RunnerConfig, RunnerProject
from vectorops.runner.runner.session import ExecutionSession


async def run(
    history: MemoryHistory,
    invoke: Callable[[], Awaitable[pydantic.JsonValue]],
    head: Optional[str] = None,
) -> models.WorkflowStep:
    async def child(
        configuration: models.Workflow,
        *,
        continue_from_step_id: Optional[str] = None,
        child_heads: Optional[Mapping[str, str]] = None,
    ) -> models.WorkflowStep:
        raise AssertionError("unexpected child")

    workflow = models.Workflow(id="test", definition_ref="test:workflow")
    await history.save_workflow(workflow)
    session = ExecutionSession(
        history, workflow, child, continue_from_step_id=head, child_heads={}
    )
    context = WorkflowExecutionContext(
        RunnerProject(Path.cwd(), RunnerConfig()), workflow, session
    )
    token = composition._CURRENT_CONTEXT.set(context)
    try:
        return await session.run(invoke)
    finally:
        composition._CURRENT_CONTEXT.reset(token)
