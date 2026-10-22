import asyncio
from pathlib import Path

import pydantic
import pytest

from tests.fixtures.history import MemoryHistory
from vectorops.runner import models, presentation as p
from vectorops.runner.control import protocol
from vectorops.runner.control.service import RunnerService
from vectorops.runner.runner.workflow import WorkflowRunner

SOURCE = """
import asyncio
from vectorops.runner.api import composition as c
from vectorops.runner.api.builtin_tools.subworkflows import subworkflow_tool
from vectorops.runner.presentation import WorkflowPresentation, WorkflowPresentationOverride

@c.workflow(presentation=WorkflowPresentation(short_name="Research", bgcolor="#ABCDEF", options=["llm_only"]))
async def child(ctx):
    return "message"

@c.workflow(presentation=WorkflowPresentation(short_name="Root", bgcolor="#123456", options=["prefix_messages", "hide_subworkflows"]))
async def main(ctx):
    first, second = await asyncio.gather(
        ctx.spawn_workflow("presentation:child", presentation=WorkflowPresentationOverride(short_name="First", options=[])),
        ctx.spawn_workflow("presentation:child", presentation=WorkflowPresentationOverride(bgcolor="#FEDCBA")),
    )
    await ctx.join_workflows([first, second])
    await child()
    tool = await subworkflow_tool(["presentation:child"])
    await tool.execute_workflow(ctx, {"reference": "presentation:child", "presentation": {"short_name": "Tool", "options": []}})
    return "done"
"""


@pytest.fixture
def presentation_source(workspace: Path) -> Path:
    path = workspace / "workflows/presentation.py"
    path.write_text(SOURCE)
    return path


async def test_parallel_direct_and_tool_spawn_overrides(
    runner: WorkflowRunner, history: MemoryHistory, presentation_source: Path
) -> None:
    assert (
        await runner.run(
            "presentation:main",
            workflow_id="root",
            presentation=p.WorkflowPresentationOverride(bgcolor="#654321"),
        )
        == "done"
    )
    root = history.workflows["root"]
    assert root.presentation.short_name == "Root"
    assert root.presentation.bgcolor == "#654321"
    children = [item for item in history.workflows.values() if item.id != "root"]
    assert len(children) == 4
    by_name = {item.presentation.short_name: item for item in children}
    assert by_name["First"].presentation.options == []
    assert by_name["First"].presentation.bgcolor == "#abcdef"
    assert by_name["Tool"].presentation.options == []
    assert all(
        p.PresentationOption.HIDE_SUBWORKFLOWS not in item.presentation.options
        for item in children
    )
    research = [item for item in children if item.presentation.short_name == "Research"]
    assert {item.presentation.bgcolor for item in research} == {"#abcdef", "#fedcba"}
    assert all(
        item.presentation.options == [p.PresentationOption.LLM_ONLY]
        for item in research
    )
    descriptor = await (await runner.project()).resolver.describe("presentation:child")
    assert descriptor.presentation.short_name == "Research"
    saved = {key: value.model_dump() for key, value in history.workflows.items()}
    appended = list(history.appended)
    presentation_source.write_text(
        SOURCE.replace("Research", "Changed").replace("First", "Other")
    )
    assert (
        await WorkflowRunner(runner.path, history).run(
            "presentation:main",
            continue_from_step_id=history.heads["root"],
            presentation=p.WorkflowPresentationOverride(options=[]),
        )
        == "done"
    )
    assert {
        key: value.model_dump() for key, value in history.workflows.items()
    } == saved
    assert history.appended == appended


async def test_persistent_start_and_resume_keep_resolved_metadata(
    workspace: Path, presentation_source: Path
) -> None:
    runner = WorkflowRunner(workspace)
    assert (
        await runner.run(
            "presentation:main",
            workflow_id="root",
            presentation=p.WorkflowPresentationOverride(short_name="Run", options=[]),
        )
        == "done"
    )
    execution_id = runner.last_execution_id
    assert execution_id is not None
    saved = await runner.workflow_state(execution_id, "root")
    assert saved.presentation == p.WorkflowPresentation(
        short_name="Run", bgcolor="#123456"
    )
    await runner.aclose()
    presentation_source.write_text(
        SOURCE.replace("Root", "New default").replace("Research", "New child")
    )
    restarted = WorkflowRunner(workspace)
    assert await restarted.resume(execution_id) == "done"
    assert (
        await restarted.workflow_state(execution_id, "root")
    ).presentation == saved.presentation
    assert (
        await restarted.run("presentation:child", workflow_id="independent")
        == "message"
    )
    other_id = restarted.last_execution_id
    assert other_id is not None
    independent = await restarted.workflow_state(other_id, "independent")
    assert independent.presentation.short_name == "New child"
    assert independent.presentation.options == [p.PresentationOption.LLM_ONLY]
    await restarted.aclose()


async def test_control_start_exposes_presentation_in_workflow_updates(
    workspace: Path, presentation_source: Path
) -> None:
    async with RunnerService(workspace) as service:
        terminal = asyncio.Event()
        snapshots: list[models.Workflow] = []

        def receive(event: dict[str, pydantic.JsonValue]) -> None:
            if event.get("kind") == "workflow.updated":
                snapshots.append(models.Workflow.model_validate(event["workflow"]))
            if event.get("kind") == "execution.completed":
                terminal.set()

        service.listen(receive)
        accepted = protocol.Accepted.model_validate(
            await service.dispatch(
                "executions.start",
                {
                    "reference": "presentation:child",
                    "idempotency_key": "presentation",
                    "presentation": {
                        "short_name": "Control",
                        "bgcolor": "#112233",
                        "options": [],
                    },
                },
            )
        )
        await asyncio.wait_for(terminal.wait(), 10)
        record = await service.runner.execution_state(accepted.execution_id)
        workflow = await service.runner.workflow_state(
            accepted.execution_id, record.root_workflow_id
        )
        assert workflow.presentation == p.WorkflowPresentation(
            short_name="Control", bgcolor="#112233"
        )
    assert snapshots
    assert all(item.presentation == workflow.presentation for item in snapshots)
