"""Measure synthetic streaming CPU without providers or access to live histories.

Run unprofiled repetitions for timings; --profile adds a separate Yappi CPU-clock
run covering all Python threads. Setup, warmup and final verification are excluded.
Summary publication uses fixed update batches so profiling cannot change its count.
Session mode includes observation, cached history, usage/summary persistence and
one JSON-encoded step-event consumer. It excludes provider transport, checkpoint
timers, control snapshot read fan-out, websockets and frontend work.
"""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Mapping
from contextlib import AsyncExitStack
from dataclasses import asdict, dataclass
import json
from pathlib import Path
import platform
import sqlite3
import statistics
import tempfile
import time
from typing import Literal, Optional

import connect
import pydantic
import yappi

from vectorops.runner import models
from vectorops.runner.api.llm import models as llm_models, observability, snapshots
from vectorops.runner.environment.target import LocalExecutionTarget
from vectorops.runner.runner import events, execution_state, summary_history
from vectorops.runner.runner.cached_history import CachedHistory
from vectorops.runner.runner.session import ExecutionSession
from vectorops.runner.runner.sqlite_history import SQLiteHistory
from vectorops.runner.runner.summary_runtime import SummaryRuntime
from vectorops.runner.runner.summary_store import SummaryStore
from vectorops.runner.runner.usage_store import UsageStore
from vectorops.runner.runner.workspace_database import WorkspaceDatabase


@dataclass(frozen=True)
class Configuration:
    mode: Literal["observation", "history", "session"] = "session"
    streams: int = 8
    updates: int = 100
    warmup: int = 5
    request_chars: int = 32768
    response_chars: int = 16384
    blocks: int = 16
    chunk_chars: int = 32
    publication_every: int = 25
    retained: int = 0
    events: bool = True

    def __post_init__(self) -> None:
        if self.mode not in ("observation", "history", "session"):
            raise ValueError("unsupported benchmark mode")
        for value in (
            self.streams,
            self.updates,
            self.blocks,
            self.chunk_chars,
            self.publication_every,
        ):
            if value < 1:
                raise ValueError(
                    "streams, updates, blocks, chunk_chars and publication_every must be positive"
                )
        for value in (
            self.warmup,
            self.request_chars,
            self.response_chars,
            self.retained,
        ):
            if value < 0:
                raise ValueError(
                    "warmup, payload sizes and retained must be nonnegative"
                )


@dataclass
class Result:
    updates: int
    wall_seconds: float
    cpu_seconds: float
    cpu_ms_per_update: float
    updates_per_second: float
    event_count: int
    event_bytes: int
    incoming_delta_bytes: int
    summary_writes: int
    verified: bool = False


class EventConsumer:
    def __init__(self) -> None:
        self.count = 0
        self.bytes = 0

    def __call__(self, event: events.StepEvent) -> None:
        payload = json.dumps(event.model_dump(mode="json"), separators=(",", ":"))
        self.bytes += len(payload.encode("utf-8"))
        self.count += 1


class MeasuredSummaryStore(SummaryStore):
    def __init__(self, database: WorkspaceDatabase) -> None:
        super().__init__(database)
        self.writes = 0

    async def record(self, snapshot: summary_history.Snapshot) -> None:
        await super().record(snapshot)
        self.writes += 1


async def forbidden_child(
    configuration: models.Workflow,
    *,
    continue_from_step_id: Optional[str] = None,
    child_heads: Optional[Mapping[str, str]] = None,
) -> models.WorkflowStep:
    raise AssertionError("benchmark must not execute a child workflow")


class Stream:
    def __init__(self, config: Configuration, index: int) -> None:
        self.workflow = models.Workflow(
            id=f"stream-{index}", definition_ref="benchmark"
        )
        request = connect.GenerateRequest(
            messages=[connect.UserMessage(content="q" * config.request_chars)]
        )
        settings = llm_models.Settings(model="openai/gpt-4o-mini")
        self.observation = observability.Observation(settings, request)
        self.accumulator = snapshots.Accumulator()
        self.progress = snapshots.Progress()
        self.value: dict[str, pydantic.JsonValue] = {}
        for block in range(config.blocks):
            size = config.response_chars // config.blocks + int(
                block < config.response_chars % config.blocks
            )
            self.value = self.accumulator.update(
                connect.TextDeltaEvent(index=block, delta="x" * size)
            )
        self.step = models.WorkflowStep(
            id=f"generation-{index}",
            workflow_id=self.workflow.id,
            operation=models.StepOperation(
                kind="llm.generate",
                name="generate",
                input={
                    "settings": settings.model_dump(mode="json"),
                    "request": request.model_dump(mode="json"),
                },
            ),
            outcome=models.StepStreaming(state=self.value),
            metrics=self.observation.snapshot(self.value, 1),
        )
        self.delta = connect.TextDeltaEvent(index=0, delta="y" * config.chunk_chars)

    def advance(self) -> models.WorkflowStep:
        self.value = self.accumulator.update(self.delta)
        self.progress.update(self.value)
        self.step = self.step.model_copy(
            update={
                "outcome": models.StepStreaming(state=self.value),
                "metrics": self.observation.snapshot(self.value, 1),
                "revision": self.step.revision + 1,
            }
        )
        return self.step


def save_profile(path: Path) -> None:
    """Save all-thread CPU call statistics and an inspectable attribution summary."""
    stats = yappi.get_func_stats()
    stats.save(str(path), type="pstat")
    report = {
        "clock": yappi.get_clock_type(),
        "timing_note": (
            "Inclusive times overlap and profiling adds overhead. Use unprofiled "
            "benchmark results for absolute costs. Inherited methods can share one "
            "profiler function name across model classes."
        ),
        "threads": [
            {"name": thread.name, "id": thread.id, "cpu_seconds": thread.ttot}
            for thread in yappi.get_thread_stats()
        ],
        "top_self": [
            {
                "function": entry.full_name,
                "calls": entry.ncall,
                "self_cpu_seconds": entry.tsub,
                "inclusive_cpu_seconds": entry.ttot,
            }
            for entry in list(stats.sort("tsub"))[:30]
        ],
        "top_inclusive": [
            {
                "function": entry.full_name,
                "calls": entry.ncall,
                "self_cpu_seconds": entry.tsub,
                "inclusive_cpu_seconds": entry.ttot,
            }
            for entry in list(stats.sort("ttot"))[:30]
        ],
    }
    path.with_suffix(".json").write_text(json.dumps(report, indent=2) + "\n")


async def measure(config: Configuration, profile: Optional[Path] = None) -> Result:
    """Run concurrent replacement streams in fresh temporary databases and verify them.

    Timings include draining deferred summary work, but exclude initialization and
    warmup. The process CPU clock includes writer threads and excludes disk waits.
    Profiling is a separate run because deterministic instrumentation adds overhead.
    """
    if yappi.is_running():
        raise RuntimeError("stop the existing profiler before running this benchmark")
    if profile is not None and profile.suffix != ".pstat":
        raise ValueError("profile output must end in .pstat")
    with tempfile.TemporaryDirectory(prefix="runner-streaming-cpu-") as directory:
        async with AsyncExitStack() as stack:
            root = Path(directory)
            streams = [Stream(config, index) for index in range(config.streams)]
            consumer = EventConsumer()
            store = MeasuredSummaryStore(WorkspaceDatabase(root))
            runtime = SummaryRuntime(store, None)
            runtime.interval = 3600
            database = SQLiteHistory(
                root / "executions" / "benchmark" / "history.sqlite3",
                summary_store=store,
                summary_runtime=runtime,
                usage_store=UsageStore(store.database),
            )
            history = CachedHistory(database, write_through=True, cache_workflows=False)
            sessions: list[ExecutionSession] = []
            if config.mode != "observation":
                await stack.enter_async_context(store.database)
                await stack.enter_async_context(database)
                await stack.enter_async_context(history)
                await database.save_workflow(
                    models.Workflow(id="root", definition_ref="benchmark")
                )
                await database.save_execution(
                    execution_state.ExecutionRecord(
                        id="benchmark", root_workflow_id="root"
                    )
                )
                target = LocalExecutionTarget(root=str(root))
                for index in range(config.retained):
                    await database.save_workflow(
                        models.Workflow(
                            id=f"retained-{index}",
                            definition_ref="benchmark",
                            target=target,
                        )
                    )
                previous: Optional[str] = None
                for stream in streams:
                    await history.save_workflow(stream.workflow)
                    spawn = models.WorkflowStep(
                        id=f"spawn-{stream.workflow.id}",
                        workflow_id="root",
                        previous_step_id=previous,
                        operation=models.WorkflowSpawnOperation(
                            child_workflow_id=stream.workflow.id
                        ),
                        outcome=models.StepCompleted(),
                    )
                    await history.append_step(spawn)
                    previous = spawn.id
                    await history.append_step(stream.step)
                    sessions.append(
                        ExecutionSession(
                            history,
                            stream.workflow,
                            forbidden_child,
                            continue_from_step_id=None,
                            child_heads={},
                            execution_id="benchmark",
                            event_sink=consumer if config.events else None,
                        )
                    )

            async def run_stream(index: int, count: int) -> None:
                for _ in range(count):
                    step = streams[index].advance()
                    if config.mode == "session":
                        await sessions[index].update(step)
                    elif config.mode == "history":
                        await database.update_step(step)
                    else:
                        await asyncio.sleep(0)

            async def run(count: int) -> None:
                for offset in range(0, count, config.publication_every):
                    batch = min(config.publication_every, count - offset)
                    await asyncio.gather(
                        *(run_stream(index, batch) for index in range(config.streams))
                    )
                    if config.mode != "observation":
                        await history.flush()
                        await runtime.flush()

            await run(config.warmup)
            consumer.count = consumer.bytes = store.writes = 0
            if profile is not None:
                yappi.clear_stats()
                yappi.set_clock_type("cpu")
                yappi.start(builtins=True, profile_threads=True)
            start_wall, start_cpu = time.perf_counter(), time.process_time()
            try:
                await run(config.updates)
            finally:
                elapsed_cpu = time.process_time() - start_cpu
                elapsed_wall = time.perf_counter() - start_wall
                if profile is not None:
                    yappi.stop()
            updates = config.streams * config.updates
            result = Result(
                updates=updates,
                wall_seconds=elapsed_wall,
                cpu_seconds=elapsed_cpu,
                cpu_ms_per_update=elapsed_cpu * 1000 / updates,
                updates_per_second=updates / elapsed_wall,
                event_count=consumer.count,
                event_bytes=consumer.bytes,
                incoming_delta_bytes=updates * config.chunk_chars,
                summary_writes=store.writes,
            )
            if profile is not None:
                save_profile(profile)
            if config.mode != "observation":
                for stream in streams:
                    saved = await database.get_step(stream.step.id)
                    if (
                        saved.outcome != stream.step.outcome
                        or saved.metrics != stream.step.metrics
                    ):
                        raise AssertionError(
                            "persisted snapshot differs from final update"
                        )
                    if saved.revision != stream.step.revision:
                        raise AssertionError("stream revision mismatch")
                expected = await database.summary_snapshot()
                if runtime.working.snapshot != expected:
                    raise AssertionError(
                        "incremental summary differs from durable state"
                    )
                if await store.get("benchmark") != expected.summary:
                    raise AssertionError("published summary differs from durable state")
            if config.mode == "session" and config.events and consumer.count != updates:
                raise AssertionError("step events were lost")
            result.verified = True
            return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode", choices=("observation", "history", "session"), default="session"
    )
    parser.add_argument("--streams", type=int, default=8)
    parser.add_argument("--updates", type=int, default=100)
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument("--request-chars", type=int, default=32768)
    parser.add_argument("--response-chars", type=int, default=16384)
    parser.add_argument("--blocks", type=int, default=16)
    parser.add_argument("--chunk-chars", type=int, default=32)
    parser.add_argument(
        "--publication-every",
        type=int,
        default=25,
        help="flush summaries after this many updates per stream, independent of wall time",
    )
    parser.add_argument("--retained", type=int, default=0)
    parser.add_argument("--no-events", action="store_true")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--profile", type=Path)
    args = parser.parse_args()
    try:
        config = Configuration(
            mode=args.mode,
            streams=args.streams,
            updates=args.updates,
            warmup=args.warmup,
            request_chars=args.request_chars,
            response_chars=args.response_chars,
            blocks=args.blocks,
            chunk_chars=args.chunk_chars,
            publication_every=args.publication_every,
            retained=args.retained,
            events=not args.no_events,
        )
        if args.runs < 1:
            raise ValueError("runs must be positive")
        if args.profile is not None and args.profile.suffix != ".pstat":
            raise ValueError("profile output must end in .pstat")
    except ValueError as exc:
        parser.error(str(exc))
    print(
        json.dumps(
            {
                "configuration": asdict(config),
                "environment": {
                    "python": platform.python_version(),
                    "platform": platform.platform(),
                    "sqlite": sqlite3.sqlite_version,
                },
            }
        ),
        flush=True,
    )
    results = []
    for index in range(args.runs):
        result = asyncio.run(measure(config))
        results.append(result)
        print(json.dumps({"run": index + 1, **asdict(result)}), flush=True)
    print(
        json.dumps(
            {
                "median_cpu_ms_per_update": statistics.median(
                    result.cpu_ms_per_update for result in results
                ),
                "median_updates_per_second": statistics.median(
                    result.updates_per_second for result in results
                ),
            }
        ),
        flush=True,
    )
    if args.profile is not None:
        result = asyncio.run(measure(config, args.profile))
        print(
            json.dumps({"profiled_run": asdict(result), "profile": str(args.profile)})
        )


if __name__ == "__main__":
    main()
