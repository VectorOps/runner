import asyncio
from pathlib import Path
import shutil

from tests.fixtures import sources
from vectorops.runner.api import llm
from vectorops.runner.api.composition import activity
from vectorops.runner.runner.context import WorkflowExecutionContext


@activity()
async def generate(ctx: object, directory: str) -> None:
    await asyncio.to_thread(
        shutil.copyfile,
        sources.path("generated_echo.py"),
        Path(directory) / "generated.py",
    )


async def main(ctx: WorkflowExecutionContext) -> list[str]:
    first = await llm.subworkflow_tool(["child:double"])
    await generate(str(ctx.project.workflows_path))
    second = await llm.subworkflow_tool(["child:double", "generated:main"])
    return [first.definition.description, second.definition.description]
