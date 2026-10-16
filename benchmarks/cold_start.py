"""Benchmark builtin agent recovery using real payloads without live operations."""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from contextlib import suppress
import json
from pathlib import Path
import resource
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time
from unittest.mock import patch


def prepare(source: Path, destination: Path, count: int) -> dict[str, object]:
    """Copy a consistent snapshot and assemble complete, replayable agent rounds."""
    from vectorops.runner import models

    with sqlite3.connect(f"{source.as_uri()}?mode=ro", uri=True) as original:
        with sqlite3.connect(destination) as database:
            original.backup(database)
    with sqlite3.connect(destination) as database:
        execution = json.loads(
            database.execute("SELECT payload FROM execution").fetchone()[0]
        )
        workflow_id = execution["root_workflow_id"]
        rows = {
            step.id: step
            for (payload,) in database.execute("SELECT payload FROM steps")
            for step in [models.WorkflowStep.model_validate_json(payload)]
        }
        head = dict(database.execute("SELECT * FROM workflow_heads"))[workflow_id]
        path = []
        while head is not None:
            step = rows[head]
            path.append(step)
            head = step.previous_step_id
        path.reverse()
        prefix = path[
            : next(i for i, s in enumerate(path) if s.operation.kind == "mailbox.input")
            + 1
        ]
        config = next(
            s
            for s in path
            if isinstance(s.operation, models.StepOperation)
            and s.operation.kind == "configuration.get"
            and s.operation.input.get("name") == "llm"
        )
        drain = next(s for s in path if s.operation.kind == "mailbox.drain")
        user = prefix[-1]
        tools = {
            s.outcome.result["tool_call_id"]: s
            for s in path
            if s.operation.kind == "llm.tool"
            and isinstance(s.outcome, models.StepCompleted)
            and isinstance(s.outcome.result, dict)
            and s.deliveries == [s.id]
        }
        rounds = []
        for step in path:
            if step.operation.kind != "llm.generate" or not isinstance(
                step.outcome, models.StepCompleted
            ):
                continue
            result = step.outcome.result
            assert isinstance(result, dict)
            content = result["content"]
            assert isinstance(content, list)
            calls = [
                block
                for block in content
                if isinstance(block, dict) and block.get("type") == "tool_call"
            ]
            if any(call["id"] not in tools for call in calls):
                continue
            rounds.append(
                [
                    config,
                    drain,
                    step,
                    *(
                        [tools[call["id"]] for call in calls]
                        if calls
                        else [drain, user]
                    ),
                ]
            )
        terminal = next(r for r in rounds if r[-1] is user)
        remaining = count - len(prefix) - len(terminal)
        possible = [True] + [False] * remaining
        for size in range(1, remaining + 1):
            possible[size] = any(
                len(r) <= size and possible[size - len(r)] for r in rounds
            )
        if not possible[remaining]:
            raise ValueError("Requested step count cannot contain complete rounds")
        selected = list(prefix)
        index = 0
        while remaining:
            candidates = rounds[index % len(rounds) :] + rounds[: index % len(rounds)]
            chosen = next(
                r
                for r in candidates
                if len(r) <= remaining and possible[remaining - len(r)]
            )
            selected.extend(chosen)
            remaining -= len(chosen)
            index += 1
        selected.extend(terminal)
        normalized = []
        for index, step in enumerate(selected):
            identifier = f"bench-{index}"
            normalized.append(
                step.model_copy(
                    update={
                        "id": identifier,
                        "workflow_id": workflow_id,
                        "previous_step_id": f"bench-{index - 1}" if index else None,
                        "deferred_delivery": True,
                        "deliveries": [identifier] if index + 1 < count else [],
                        "outcome": (
                            step.outcome if index + 1 < count else models.StepPending()
                        ),
                        "completed_at": (
                            step.completed_at if index + 1 < count else None
                        ),
                        "revision": 1,
                    }
                )
            )
        database.execute("PRAGMA foreign_keys=OFF")
        for table in [
            "input_requests",
            "tool_approval_grants",
            "branch_operations",
            "workflow_parents",
            "workflow_heads",
            "steps",
        ]:
            database.execute(f"DELETE FROM {table}")
        database.execute("DELETE FROM workflows WHERE id != ?", (workflow_id,))
        database.executemany(
            "INSERT INTO steps VALUES (?, ?, ?, ?)",
            [
                (s.id, s.workflow_id, s.previous_step_id, s.model_dump_json())
                for s in normalized
            ],
        )
        database.execute(
            "INSERT INTO workflow_heads VALUES (?, ?)", (workflow_id, normalized[-1].id)
        )
        execution.update(status="paused", completion_step_id=None)
        execution["checkpoint"]["heads"] = {workflow_id: normalized[-1].id}
        database.execute("UPDATE execution SET payload = ?", (json.dumps(execution),))
        kinds = Counter(s.operation.kind for s in normalized)
        payload_bytes = sum(len(s.model_dump_json().encode()) for s in normalized)
        return {
            "source_id": source.parent.name,
            "source_steps": len(rows),
            "steps": count,
            "kinds": dict(kinds),
            "payload_bytes": payload_bytes,
            "eligible_rounds": len(rounds),
        }


async def measure(database: Path, workspace: Path, mode: str) -> dict[str, object]:
    """Time imports, metadata loading, and recovery to the first live input wait."""
    start = time.perf_counter()
    from vectorops.runner.runner.executor import WorkflowExecutor
    from vectorops.runner.runner.inbox import Registry
    from vectorops.runner.runner.mailbox import Mailbox
    from vectorops.runner.runner.project import RunnerProject
    from vectorops.runner.runner.sqlite_history import SQLiteHistory

    imported = time.perf_counter()
    project = await RunnerProject.open(workspace)
    async with SQLiteHistory(database) as history:
        execution = await history.get_execution()
        workflow = await history.get_workflow(execution.root_workflow_id)
        executor = WorkflowExecutor(project, history, execution_id=execution.id)
        await executor.load(workflow.definition_ref)
        loaded = time.perf_counter()
        head = execution.checkpoint.heads[workflow.id]
        if mode == "scan":
            steps = 0
            async for step in history.replay_path(workflow.id, head):
                steps += 1
        else:
            ready = asyncio.Event()
            steps = 0

            async def wait(self: Mailbox, *args: object, **kwargs: object) -> None:
                ready.set()
                await asyncio.Future()

            async def forbidden(*args: object, **kwargs: object) -> None:
                raise AssertionError(
                    "Benchmark attempted a new operation or history write"
                )

            executor.mailbox = Mailbox(
                history, inboxes=Registry(execution.id, workflow.id)
            )
            with (
                patch.object(Mailbox, "wait", wait),
                patch.object(history, "append_step", forbidden),
                patch.object(history, "update_step", forbidden),
                patch.object(history, "record_delivery", forbidden),
            ):
                task = asyncio.create_task(
                    executor.execute(
                        workflow,
                        continue_from_step_id=head,
                        child_heads=execution.checkpoint.heads,
                    )
                )
                waiter = asyncio.create_task(ready.wait())
                try:
                    done, _ = await asyncio.wait(
                        [task, waiter], timeout=120, return_when=asyncio.FIRST_COMPLETED
                    )
                    if task in done:
                        await task
                        raise AssertionError(
                            "Agent returned before reaching the input boundary"
                        )
                    if waiter not in done:
                        raise TimeoutError(
                            "Agent replay did not reach the input boundary"
                        )
                    recovered = time.perf_counter()
                finally:
                    task.cancel()
                    waiter.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
                    with suppress(asyncio.CancelledError):
                        await waiter
        if mode == "scan":
            recovered = time.perf_counter()
    end = time.perf_counter()
    return {
        "mode": mode,
        "import_ms": (imported - start) * 1000,
        "setup_ms": (loaded - imported) * 1000,
        "replay_ms": (recovered - loaded) * 1000,
        "ready_ms": (recovered - start) * 1000,
        "cleanup_ms": (end - recovered) * 1000,
        "peak_rss_mib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024,
        "scanned_steps": steps,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path)
    parser.add_argument("--steps", type=int, default=2000)
    parser.add_argument("--runs", type=int, default=7)
    parser.add_argument("--child", type=Path)
    parser.add_argument("--workspace", type=Path)
    parser.add_argument("--mode", choices=["scan", "replay"], default="replay")
    args = parser.parse_args()
    if args.child is not None:
        print(json.dumps(asyncio.run(measure(args.child, args.workspace, args.mode))))
        return
    if args.source is None:
        parser.error("--source is required")
    with tempfile.TemporaryDirectory(prefix="runner-cold-start-") as temporary:
        root = Path(temporary)
        database = root / "history.sqlite3"
        print(
            json.dumps(
                {"fixture": prepare(args.source.resolve(), database, args.steps)}
            ),
            flush=True,
        )
        for mode in ["scan", "replay"]:
            results = []
            for index in range(args.runs):
                started = time.perf_counter()
                process = subprocess.run(
                    [
                        sys.executable,
                        __file__,
                        "--child",
                        str(database),
                        "--workspace",
                        str(root),
                        "--mode",
                        mode,
                    ],
                    capture_output=True,
                    text=True,
                )
                if process.returncode:
                    raise RuntimeError(process.stderr)
                result = json.loads(process.stdout)
                result["process_ms"] = (time.perf_counter() - started) * 1000
                results.append(result)
                print(json.dumps({"run": index + 1, **result}), flush=True)
            print(
                json.dumps(
                    {
                        "summary": mode,
                        "median": {
                            key: statistics.median(float(r[key]) for r in results)
                            for key in results[0]
                            if key != "mode"
                        },
                    }
                ),
                flush=True,
            )


if __name__ == "__main__":
    main()
