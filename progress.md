# Runner v2 progress

Current implementation status and suggested next development horizon.

## Implemented

- The Python 3.13 uv project preserves the old project's dependency requirements and package layout.
- Pydantic models provide JSON-serializable workflow configuration and normalized step records.
- Predecessor references represent branching histories without copying existing steps.
- Typed operations represent child spawning, ordered joins, and workflow completion.
- Workspace setup supports `workflows/`, `data/`, and optional directory overrides in `config.toml`.
- The executor loads named async callables from workspace Python files with path validation.
- Executor sessions drive workflow operation requests through registered typed handlers.
- Local activity decorators bind arguments, apply defaults, and validate JSON-compatible inputs and results.
- Ordinary and streaming activities receive an explicit first-argument context with execution, workflow, and step identities.
- Activity identities are persisted before invocation and reused during interrupted recovery for implementation-owned idempotency.
- Streaming activities yield replacement snapshots and one final result while history retains only the latest step version.
- Completed streams replay without reinvocation, and interrupted streams restart under the same step identity.
- History access is injected through an asynchronous protocol rather than an executor-owned step collection.
- Each execution retains its root and child workflow histories in `data/executions/<execution_id>/history.sqlite3`.
- Runner-owned `data/runner-state.json` stores only the last execution identity for restart discovery.
- Each history database contains execution identity, lifecycle status, root configuration, and selected root and child heads.
- Durable step writes advance selected heads transactionally, independently of global runner state.
- Periodic execution checkpoints flush buffered snapshots and persist recovery metadata inside the history database.
- Explicit resume uses recorded execution configuration and checkpoint heads without automatically rerunning interrupted workflows.
- Exclusive project writer ownership prevents concurrent processes from conflicting over global state.
- Explicit cleanup removes terminal execution histories while preserving active executions and unrelated records.
- Optional age and count retention limits prune completed histories after successful execution while protecting the last execution.
- Transactional streaming updates rewrite one unfinished step while preserving its identity and immutable terminal outcome.
- SQLite replay uses indexed lookup and database-side path snapshots with bounded Python-side batches.
- Bounded LRU caches reduce repeated configuration and step lookups during execution.
- Write-behind buffering coalesces streaming snapshots and flushes periodically, on completion, or at runner shutdown.
- Replay streams selected history from the root and returns recorded outcomes without repeating activities.
- Continuation appends new descendants while preserving existing branches.
- Child workflows execute concurrently or sequentially with independent sessions and histories.
- Decorated async workflows support direct child awaits and explicit start/gather composition using function-derived persistent targets.
- Executor step-event hooks expose accepted snapshots for future UI listeners without making progress part of workflow results.
- Joins reference exact child completion records and propagate or collect failures in invocation order.
- Explicit child replay heads support partial recovery and forks across multiple nesting levels.
- Cancellation closes replay streams, settles pending requests, and cancels owned child tasks.
- A comprehensive test suite covers implemented behaviors with reusable fixtures under `tests/fixtures/`.
- Black, mypy, warnings-as-errors, and asyncio debug checks pass.

## Suggested next horizon

- Add a minimal CLI to run, resume, fork, and inspect workflows.
- Update the README with runnable authoring, execution-state inspection, and recovery examples.
- Keep RPC, parallel activities, retries, timeouts, and compensation outside the current scope.