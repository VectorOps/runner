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
- `@activity_wrapper` injects an `ActivityCall` helper into async functions and instance methods. Wrapper code always executes, while `run()` and `stream()` establish a single recorded activity boundary with explicit JSON inputs.
- Workflow-local `LLM` objects own opaque conversation state, expose detached history snapshots, and reconstruct deterministic state changes from recorded results during replay. New instances reset history, and overlapping mutations on one instance are rejected.
- LLM activity inputs contain instance identities and request digests rather than accumulated transcripts, preventing quadratic conversation duplication in saga storage and detecting changed requests during replay.
- The `connect` provider adapter accumulates streaming events into replacement snapshots, preserves final response metadata, resolves authentication at runtime, and rejects unsuccessful or missing terminal responses.
- Explicit named tool bindings derive schemas from Pydantic argument models. The workflow-side `invoke_tool` helper validates calls, optionally waits for per-call mailbox approval, and records execution independently of the LLM. Bounded LLM loops execute tools in parallel and append results in model-call order; expected errors become model-visible results while infrastructure failures propagate after all calls settle without appending a partial round.
- Basic opt-in compaction records summary generation as `llm.compact` and applies history replacement outside the callback so replay reconstructs the same state. It uses a character-count threshold, the generation model/settings, and a configurable instruction; it preserves tool exchanges but does not guarantee that context fits the model limit.
- History access is injected through an asynchronous protocol rather than an executor-owned step collection.
- Each execution retains its root and child workflow histories in `data/executions/<execution_id>/history.sqlite3`.
- Runner-owned `data/runner-state.json` stores the last execution identity and active execution IDs for restart discovery, with serialized updates and backward-compatible loading of last-pointer-only manifests.
- Each history database contains execution identity, lifecycle status, root configuration, and selected root and child heads.
- Durable step writes advance selected heads transactionally, independently of global runner state.
- Periodic execution checkpoints flush buffered snapshots and persist recovery metadata inside the history database.
- Explicit resume uses recorded execution configuration and checkpoint heads; the control service reconciles workspace state against retained histories and automatically recovers all executions whose persisted status is `running`, including when the manifest is missing.
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
- Multiple input waits and independent root executions can run concurrently, including on the same channel. Input ownership and responses remain execution-specific, and rewinding an execution with pending requests is rejected.
- `vops-runner` starts workflows with JSON input and optional execution IDs, and resumes persisted executions after interruption.
- Read-only CLI status exposes persisted execution metadata and counts by operation and outcome across root, child, and retained branch histories while a writer is active.
- Full-execution JSONL export writes a versioned metadata/configuration header followed by normalized steps from one committed snapshot, without overwriting existing files.
- The CLI guide documents runnable authoring, inspection, recovery, and JSONL analysis contracts.
- A comprehensive test suite covers implemented behaviors with reusable fixtures under `tests/fixtures/`.
- Mailbox coverage includes disconnect/reconnect, parallel channels, shutdown/restart, process crashes, undelivered accepted answers, competing responses, idempotent retries, cancellation, and notification failures.
- Mailbox implementation validation passes all 372 tests, Black, mypy, and whitespace checks.
- LLM integration coverage includes state reconstruction, request mismatches, branching, cancellation, tool errors, compaction interruption, and linear saga-input storage growth. Integration validation passes all 398 tests, Black, mypy, and whitespace checks; provider tests use fakes rather than live model calls.
- Black, mypy, warnings-as-errors, and asyncio debug checks pass.
- Structured diagnostic logging persists versioned JSONL outside saga history, with UTC timestamps, severity filtering, task-local execution/workflow/step correlation, nested sensitive-field redaction, and metadata-only error frames.
- Bounded background writes, size rotation, count retention, private file permissions, and dropped/write-error counters keep diagnostics independent of workflow outcomes. Authenticated hosts can list retained files and download bounded byte chunks over either control transport; configuration and delivery guarantees are documented in [Structured diagnostic logs](docs/config/logging.md).
- Logging validation passes all 429 tests with warnings treated as errors and asyncio debug enabled, plus Black, mypy, and whitespace checks. Coverage includes redaction, queue overflow, disk failures, shutdown draining, rotation/restart, bounded downloads, parallel activity correlation, and replay.
- Workflow code reads schema-free nested component settings with `await configuration.get(name, default)`. Each read is a recorded `configuration.get` activity: completed reads replay their old values while new reads load the current workspace file off the event loop. There is no global configuration digest or component-schema registration.
- LLM generation and tool rounds read recorded configuration snapshots for omitted model/provider, thinking (`reasoning.effort`), temperature, output budget, timeouts, and compaction. Async `resolve_settings` supports workflow helpers; explicit parameters override workspace defaults. Existing LLM objects adopt live model changes on the next fresh round, while interrupted generations retry with their recorded settings.
- [Component configuration](docs/config/components.md) documents lookup semantics, persisted-value security, LLM precedence, round-limit scope, and history compatibility. Configuration values can safely drive workflow branches because reads participate in deterministic replay.
- Configuration validation passes all 448 tests with warnings-as-errors and asyncio debug, plus Black and mypy; provider-facing tests use fakes.

- Runner-global configuration automatically creates and reuses separate encryption and control-authentication secrets in a private user-level directory. Daemon startup, local clients, and embedded mode no longer require per-start token provisioning; explicit token overrides remain supported.
- Provider credentials are stored in per-profile AES-GCM encrypted files with private permissions, atomic publication, and cross-process refresh serialization. The LLM adapter resolves credentials through the runner instead of environment variables; non-secret profile references participate in settings while default-profile request digests preserve history compatibility.
- ChatGPT OAuth is runner-owned: hosts claim pending authentication requirements, start their own loopback callback listener, open a browser, and relay state/code over authenticated control RPCs. The runner retains PKCE material, exchanges codes, persists tokens, and refreshes independently of host presence. Dedicated queries and advisory events support reconnects; authentication payloads never enter mailbox, saga, or control receipt storage.
- Missing API keys fail without prompting; interactive OAuth waits survive host disconnect and clean runner shutdown/recovery. Claims expire, stale callbacks are rejected, and explicit authentication cancellation fails shared waiters. Token endpoint failures are sanitized and classified, refresh retries are bounded, and rotated tokens finish publication before cancellation propagates.
- [Provider authentication](docs/config/authentication.md) documents provisioning, host coordination, RPC contracts, restart behavior, Unix filesystem requirements, and the trusted same-user execution boundary. Authentication coverage includes mocked token endpoints and host callback HTTP rather than live provider login.
- Authentication validation passes all 511 tests with warnings-as-errors and asyncio debug, plus Black, mypy, and whitespace checks.

- One daemon now manages concurrent independent executions within a workspace while retaining exclusive project writer ownership. Start creates a new root; resume rejects duplicate active execution IDs; stop affects only the selected execution and waits for owned cleanup before terminal notification. Startup stops persist cancellation after history initialization. Roots retain separate histories, mailboxes, environment attachments, and process managers, but shared source-tree edits are not isolated.
- The execution-oriented control API adds paginated `executions.active` and typed client listing results. `executions.current` accepts an execution ID and rejects ambiguous no-ID queries; start, resume, stop, persisted lookup, history, and event subscriptions remain execution-specific. Daemon status/connect display active execution pages. [Control protocol](docs/reference/control-protocol.md#concurrent-execution-lifecycle) documents lifecycle and compatibility behavior.
- Cancel shutdown handles each root independently, preserving mailbox/authentication waits for restart without suspending unrelated work or overriding explicit stops. Drain waits for every root while accepting responses and reconnects. Active, scheduled, and reserved histories are protected against retention during startup, resume, and cleanup.
- Multi-execution validation passes all 612 tests with warnings-as-errors and asyncio debug enabled, plus Black, whitespace checks, and mypy over production code and changed tests. Coverage includes concurrent memory/TCP clients, independent failures and stops, idempotent start/resume, active pagination, event filtering, atomic state updates, legacy manifests, hard-crash recovery without a manifest, incomplete unpublished histories, multi-waiter restart, authentication isolation, drain/reconnect, cancelled/repeated shutdown, and retention protection. A broader `mypy src tests` run reports eight pre-existing errors in four unchanged authentication/diagnostics test files.

## Suggested next horizon

- Improve LLM compaction beyond the initial summarizer: use model-aware token budgets that include system prompts, tool schemas, and output headroom; introduce structured summaries and explicit repeated-summary updates that preserve task intent, constraints, decisions, and unfinished work; support dedicated summary model/settings and output budgets; verify context reduction and define bounded recovery when compaction cannot fit the request. Add summary-quality and context-limit tests while retaining deterministic replay, compact saga inputs, and intact tool exchanges.
- Add transient provider-error handling: classify retryable errors, including rate limits, timeouts, and transient transport/provider failures, and retry with bounded attempts and backoff before failing the activity. Define cancellation, partial-stream reset, and replay behavior rather than copying codeng's retry logic unchanged.
- Add configurable concurrency limits to the implemented parallel tool execution while preserving independently recorded calls, deterministic result ordering, and cancellation/replay behavior.
- Replace binding-local approval policy with a separate tool-whitelisting service keyed by stable tool names, which bindings already require. The service decides whether a named tool can execute automatically or requires explicit host approval through the mailbox service. Keep authorization separate from argument validation; define whitelist configuration, approval scope, rejection behavior, and replay-safe decisions without changing workflow call sites.
- Extend structured diagnostics as needed with opt-in standard-library logging adapters, age-based retention, and richer provider metrics; preserve payload minimization and keep diagnostic events separate from saga history.
- Implement a subworkflow tool backed by a workflow manager or an explicit catalog of permitted workflows in workspace `config.toml`. Define discovery, input schemas, result mapping, and access restrictions; use workflow-level spawn/join orchestration rather than attempting nested workflow execution inside an activity callback.
- Design a steering API for messages arriving when no input request is pending. Evaluate queued mailbox inputs and a nonblocking peek/receive API so the LLM loop can inject steering between rounds. Decide queue ownership, routing, persistence, consumption/acknowledgment, and deterministic replay semantics before changing the current request/response mailbox contract.
- Extend provider authentication beyond ChatGPT OAuth as needed, add encryption-key rotation tooling, and consider native platform-specific locking/storage support beyond the initial Unix implementation. Remote control continues to require a protected tunnel; direct remote TLS is not implemented.
- Extend LLM observability with cost estimates, model-aware context budgets, remaining output headroom, retry attempts, and compaction savings. Distinguish estimates from provider-reported usage and expose metrics through activity events and inspection without duplicating transcripts or leaking credentials.
- Add explicit CLI fork and execution-listing commands if needed.
- Add host-side mailbox presentation or HTTP adapters as needed without coupling them to workflow execution.