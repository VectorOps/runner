"""A subprocess runner stopped after publishing a streaming recovery checkpoint."""

import asyncio
from pathlib import Path
import sys

from vectorops.runner.runner.workflow import WorkflowRunner


async def main(path: Path) -> None:
    runner = WorkflowRunner(path)
    task = asyncio.create_task(
        runner.run("streaming:workflow", input={"mode": "blocked"}, workflow_id="root")
    )
    while True:
        pointer = await runner.project_state()
        if pointer.last_execution_id is not None:
            execution = await runner.execution_state(pointer.last_execution_id)
            if execution.checkpoint.heads:
                print(execution.id, flush=True)
                break
        await asyncio.sleep(0.01)
    await task


if __name__ == "__main__":
    asyncio.run(main(Path(sys.argv[1])))
