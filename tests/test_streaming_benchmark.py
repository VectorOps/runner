from pathlib import Path
import json
from typing import Literal

import pytest

from benchmarks import streaming_cpu as benchmark


@pytest.mark.parametrize("mode", ["observation", "history", "session"])
async def test_benchmark_modes(
    mode: Literal["observation", "history", "session"],
) -> None:
    config = benchmark.Configuration(
        mode=mode, streams=2, updates=3, warmup=1, request_chars=100, response_chars=100
    )
    result = await benchmark.measure(config)
    assert result.verified
    assert result.updates == 6
    assert result.cpu_seconds > 0
    assert result.wall_seconds > 0
    assert result.event_count == (6 if mode == "session" else 0)
    assert result.incoming_delta_bytes == 6 * config.chunk_chars
    assert result.summary_writes == (0 if mode == "observation" else 1)


async def test_profile_includes_worker_threads(tmp_path: Path) -> None:
    output = tmp_path / "streaming.pstat"
    config = benchmark.Configuration(
        streams=1, updates=4, warmup=1, publication_every=2
    )
    baseline = await benchmark.measure(config)
    result = await benchmark.measure(config, output)
    assert result.verified
    assert result.summary_writes == baseline.summary_writes == 2
    assert result.event_bytes == baseline.event_bytes
    assert output.stat().st_size > 0
    report = json.loads(output.with_suffix(".json").read_text())
    assert report["clock"] == "cpu"
    assert len(report["threads"]) >= 3
    assert all(thread["cpu_seconds"] > 0 for thread in report["threads"])
    assert any(
        "SQLiteWriter._invoke" in row["function"] for row in report["top_inclusive"]
    )


async def test_events_can_be_disabled() -> None:
    result = await benchmark.measure(
        benchmark.Configuration(streams=1, updates=2, warmup=1, events=False)
    )
    assert result.verified
    assert result.event_count == 0
    assert result.event_bytes == 0


def test_invalid_configuration() -> None:
    with pytest.raises(ValueError):
        benchmark.Configuration(updates=0)
    with pytest.raises(ValueError):
        benchmark.Configuration(request_chars=-1)
