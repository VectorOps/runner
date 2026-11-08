# Workspace file search

Use `vectorops.runner.file_search` to add file autocomplete to a client or integration.
It returns ranked paths, not file contents. Create one service per workspace on one
event loop; it does not require a UI or a network transport.

## Quick start

```python
from pathlib import Path
from vectorops.runner import file_search

service = file_search.FileSearchService(Path.cwd(), file_search.SearchConfig())
try:
    await service.refresh()
    updates = service.search("config", limit=20)
    try:
        async for update in updates:
            print(update)
    finally:
        await updates.aclose()
finally:
    await service.close()
```

`refresh()` returns immediately with a refresh ID and reuses an active refresh.
Search starts with the current index, including an empty index before the first
refresh. It stays subscribed until closed or cancelled. Closing a query does not
cancel discovery. Always close the service to join its workers.

## Snapshots and selection

Every update contains an index revision, complete ranked results, `refreshing`,
`incomplete`, `truncated`, a refresh ID, and an optional `refresh_error`.
`incomplete` is true before a successful scan and after a failed scan.
`truncated` indicates either more matches than the requested limit or a scan
resource limit. `refreshing` separately indicates a best-effort in-flight snapshot.
Highlight ranges are half-open Python string offsets into the original relative
path, not UTF-8 bytes or JavaScript UTF-16 offsets.

Discovery and indexing share a worker; matching uses a separate worker. Published
segments are immutable. A subscription has one active evaluation and coalesces
pending revisions; its delivery queue holds only the latest snapshot. Slow
consumers can skip intermediate revisions. Successful refresh replaces the old
generation and removes deleted paths. Any failed or limited refresh rolls back to
the last completed snapshot, including discarding newly discovered partial paths.

Results during refresh are best-effort. The caller must validate that a selected
path is still a permissible workspace file when resolving or attaching it. Your
integration owns session/query IDs, obsolete-query filtering, and selection by path.
The service does not persist, watch, or poll the filesystem.

## Discovery backends

The preferred command is `rg --files --hidden -0 -g '!.git' .`, without a shell,
with the workspace root as its working directory. Cancellation and deadlines kill
and reap the child. Exit codes other than 0 or 1 make the scan incomplete.
Ripgrep stderr is discarded to keep diagnostic buffering bounded; failures report
the exit code. Only a missing executable triggers fallback.

The iterative `os.scandir()` fallback applies workspace-root and nested
`.gitignore` rules through `pathspec`, including inherited rules and negation. It
prunes ignored directories and excludes `.git` and symlinks. Neither backend
follows directory symlinks or includes symlink entries.
Inaccessible paths fail the fallback scan rather than silently deleting indexed
files. Set `rg_executable=None` to force fallback.

The fallback does not read ancestor `.gitignore` files, `.ignore`, `.rgignore`,
`.git/info/exclude`, global Git excludes, or ripgrep configuration. Ripgrep uses
its own ignore sources and repository-boundary behavior. Install ripgrep for the
preferred backend, or explicitly select the fallback for consistent behavior.

## Limits

`SearchConfig` is immutable and rejects unknown settings. Measure representative
repositories before choosing production limits; the defaults are not sizing guarantees.

| Setting | Default | Meaning |
| --- | --- | --- |
| `max_candidates` | 100000 | Maximum union of old and new indexed paths |
| `max_memory_bytes` | 134217728 | Conservative index admission budget |
| `max_scan_entries` | 1000000 | Fallback traversal entry budget |
| `scan_timeout` | 30 seconds | Discovery and index construction deadline |
| `max_results` | 100 | Maximum accepted search limit |
| `max_query_length` | 256 | Maximum query length in Python characters |
| `batch_size` | 256 | Entries per index segment |
| `publication_interval` | 0.05 seconds | Minimum discovery publication interval |
| `max_ignore_bytes` | 1048576 | Per-file fallback ignore-rule read limit |
| `max_cache_candidates` | 100000 | Total retained prefix-cache candidates |
| `rg_executable` | `rg` | Executable name/path; `None` forces fallback |

The memory estimate is an index admission budget, not an operating-system RSS cap.
Workers, temporary snapshots, ignore rules, and subscriber results also use memory.

## Verification and benchmarks

```bash
uv run pytest tests/file_search
uv run python tests/file_search/benchmark.py /path/to/repository
uv run python tests/file_search/benchmark.py /path/to/repository --fallback
```

The opt-in benchmark reports discovery time, index construction time, median/p95
query latency, and current/peak traced Python allocations. Tracing adds overhead;
measure process RSS separately for production sizing. Run it on representative
small, medium, and large repositories, with application queries, before selecting
production thresholds. It does not run as part of pytest.

## Related

- [Control client integration](../reference/control-protocol.md)
- [Contributor setup](../../CONTRIBUTING.md)