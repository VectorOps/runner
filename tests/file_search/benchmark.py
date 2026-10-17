"""Opt-in discovery, index, query, and traced-allocation benchmark."""

import argparse
import json
from pathlib import Path
import statistics
import threading
import time
import tracemalloc

from vectorops.runner import file_search as fs
from vectorops.runner.file_search import discovery, index, matcher


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--fallback", action="store_true")
    parser.add_argument("--query", action="append")
    args = parser.parse_args()
    config = fs.SearchConfig(rg_executable=None if args.fallback else "rg")
    stop = threading.Event()
    tracemalloc.start()
    started = time.perf_counter()
    paths = list(discovery.discover(args.root, config, stop, time.monotonic() + 300))
    discovered = time.perf_counter()
    view = index.IndexView((index.Segment.create(map(index.Entry.create, paths)),))
    built = time.perf_counter()
    worker = matcher.Matcher(config.max_cache_candidates)
    latencies: list[float] = []
    queries = args.query or ["", "src", "test", "py", "config", "fss"]
    for _ in range(10):
        for query in queries:
            before = time.perf_counter()
            worker.evaluate(view, query, 20, stop)
            latencies.append((time.perf_counter() - before) * 1000)
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    print(
        json.dumps(
            {
                "root": str(args.root),
                "backend": "fallback" if args.fallback else "rg-preferred",
                "files": len(paths),
                "discovery_seconds": discovered - started,
                "index_build_seconds": built - discovered,
                "query_median_ms": statistics.median(latencies),
                "query_p95_ms": sorted(latencies)[int(len(latencies) * 0.95)],
                "traced_current_bytes": current,
                "traced_peak_bytes": peak,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
