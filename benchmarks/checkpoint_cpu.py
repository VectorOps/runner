"""Measure repeated checkpoints with retained completed heads in temporary histories."""

import argparse
import asyncio
import json
from pathlib import Path
import statistics
import tempfile
import time

from vectorops.runner import models
from vectorops.runner.runner import execution_state
from vectorops.runner.runner.cached_history import CachedHistory
from vectorops.runner.runner.checkpoint import ExecutionCheckpointer
from vectorops.runner.runner.sqlite_history import SQLiteHistory


async def measure(heads: int, runs: int) -> dict[str, float]:
    with tempfile.TemporaryDirectory(prefix="checkpoint-cpu-") as directory:
        async with SQLiteHistory(Path(directory) / "history.sqlite3") as database:
            await database.save_workflow(
                models.Workflow(id="root", definition_ref="benchmark")
            )
            await database.save_execution(
                execution_state.ExecutionRecord(id="execution", root_workflow_id="root")
            )
            for index in range(heads):
                identifier = "root" if index == 0 else f"child-{index}"
                if index:
                    await database.save_workflow(
                        models.Workflow(id=identifier, definition_ref="benchmark")
                    )
                await database.append_step(
                    models.WorkflowStep(
                        id=f"head-{index}",
                        workflow_id=identifier,
                        operation=models.WorkflowCompleteOperation(),
                        outcome=models.StepCompleted(result="x" * 16384),
                    )
                )
            async with CachedHistory(database) as cache:
                checkpoint = ExecutionCheckpointer(
                    database, cache, await database.get_execution()
                )
                await checkpoint.persist()
                cpu, wall = [], []
                for _ in range(runs):
                    checkpoint.record("root", "head-0")
                    started_cpu, started_wall = time.process_time(), time.perf_counter()
                    await checkpoint.persist()
                    cpu.append((time.process_time() - started_cpu) * 1000)
                    wall.append((time.perf_counter() - started_wall) * 1000)
                record = await database.get_execution()
                assert len(record.checkpoint.heads) == heads
                return {
                    "heads": heads,
                    "cpu_ms": statistics.median(cpu),
                    "wall_ms": statistics.median(wall),
                }


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=int, default=3)
    args = parser.parse_args()
    if args.runs < 1:
        parser.error("runs must be positive")
    for heads in (8, 256, 2304):
        print(json.dumps(await measure(heads, args.runs)))


if __name__ == "__main__":
    asyncio.run(main())
