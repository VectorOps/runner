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
- A persistent FIFO dispatcher owns request acceptance and replay ordering while a single supervisor handles fatal errors and cleanup.
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
- Explicit resume uses recorded execution configuration and checkpoint heads; the control service automatically recovers the last execution when its persisted status is `running`.
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
- Decorated async workflows compose with standard asyncio gather, tasks, waits, and task groups using function-derived persistent targets.
- Durable waiting joins preserve request order while children complete independently without regressing history heads.
- Executor step-event hooks expose accepted snapshots for future UI listeners without making progress part of workflow results.
- Joins reference exact child completion records and propagate or collect failures in invocation order.
- Explicit child replay heads support partial recovery and forks across multiple nesting levels.
- Cancellation closes replay streams, settles pending requests, and cancels owned child tasks.
- The transport-independent control service and client expose execution and mailbox operations over embedded or TCP connections.
- The built-in `vectorops.runner.api.mailbox.request_input` activity waits on a named channel with a JSON request payload and a text response by default, or a JSON response when requested.
- Mailboxes accept responses only for existing request IDs; channels classify requests without buffering unsolicited messages.
- Input requests and accepted responses persist in the execution database, and recovery reuses request identities and delivers accepted answers without asking again.
- Hosts can list pending requests with channel and workflow filters, inspect a request, and submit an atomic, idempotent response through `mailbox.list`, `mailbox.get`, and `mailbox.respond`.
- Live `mailbox.requested` and `mailbox.answered` notifications supplement durable queries; reconnecting hosts discover outstanding requests without relying on event replay.
- Host disconnects preserve waits. Service shutdown while input is pending preserves waits for restart, while explicit execution stops cancel them.
- Multiple input waits can run concurrently, including on the same channel. The service retains its single-active-root execution limit, and rewinding an execution with pending requests is rejected.
- `vops-runner` starts workflows with JSON input and optional execution IDs, and resumes persisted executions after interruption.
- Read-only CLI status exposes persisted execution metadata and counts by operation and outcome across root, child, and retained branch histories while a writer is active.
- Full-execution JSONL export writes a versioned metadata/configuration header followed by normalized steps from one committed snapshot, without overwriting existing files.
- The CLI guide documents runnable authoring, inspection, recovery, and JSONL analysis contracts.
- A comprehensive test suite covers implemented behaviors with reusable fixtures under `tests/fixtures/`.
- Mailbox coverage includes disconnect/reconnect, parallel channels, shutdown/restart, process crashes, undelivered accepted answers, competing responses, idempotent retries, cancellation, and notification failures.
- Mailbox implementation validation passes all 372 tests, Black, mypy, and whitespace checks.
- Black, mypy, warnings-as-errors, and asyncio debug checks pass.

## Suggested next horizon

- Add explicit CLI fork and execution-listing commands if needed.
- Add host-side mailbox presentation or HTTP adapters as needed without coupling them to workflow execution.
- Keep unsolicited-message queueing on the host side; retries, timeouts, and compensation remain outside the current scope.