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
- Streaming activities yield replacement snapshots and one final result while history retains only the latest step version.
- Completed streams replay without reinvocation, and interrupted streams restart under the same step identity.
- History access is injected through an asynchronous protocol rather than an executor-owned step collection.
- Replay streams selected history from the root and returns recorded outcomes without repeating activities.
- Continuation appends new descendants while preserving existing branches.
- Child workflows execute concurrently or sequentially with independent sessions and histories.
- Joins reference exact child completion records and propagate or collect failures in invocation order.
- Explicit child replay heads support partial recovery and forks across multiple nesting levels.
- Cancellation closes replay streams, settles pending requests, and cancels owned child tasks.
- A comprehensive test suite covers implemented behaviors with reusable fixtures under `tests/fixtures/`.
- Black, mypy, warnings-as-errors, and asyncio debug checks pass.

## Suggested next horizon

- Implement a production history provider with durable configuration, immutable terminal records, and replaceable streaming snapshots under `data/`.
- Add indexed step lookup and bounded-memory root-to-head replay for long-running workflows.
- Define recovery-head selection for root and child workflows without requiring callers to supply every head.
- Define interruption and idempotency policies for external effects whose results were not recorded.
- Add a minimal CLI to run, resume, fork, and inspect workflows.
- Update the README with runnable authoring and recovery examples once a history provider exists.
- Keep RPC, parallel activities, retries, timeouts, and compensation outside the current scope.