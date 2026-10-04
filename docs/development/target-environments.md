# Target environments

Status: filesystem operations, local pipe processes, and root-workflow resource ownership are implemented. Patching, remote drivers, PTYs, and durable target-binding recovery remain proposed design.

A target environment is the filesystem and execution context that agent tools can inspect and modify. This design separates target operations from the runner so the same tools can work locally or against future remote backends.

## Implemented filesystem foundation

The package provides an async `TargetEnvironment` protocol, typed Pydantic requests/results, a POSIX `LocalEnvironment`, a backend factory, and shared bounded UTF-8 line reading. It has no saga, activity, persistence, or patching dependencies. The runner attaches the configured environment per root execution and injects it into activity contexts.

### Configuration and attachment

The optional `[environment]` section in the runner workspace's `config.toml` selects the backend:

```toml
[environment]
kind = "local"
base_project_path = "/home/user/projects/example"
max_read_bytes = 1048576
max_write_bytes = 16777216
max_list_entries = 1000
```

`kind` and `base_project_path` are required in runner configuration. Only `local` is supported. The base path must be absolute and identify an existing directory at attachment time; attachment never creates the project. It is independent of the runner workspace. The three limits are positive integers with the defaults shown above. Unknown settings and backends are rejected. Omitting `environment` leaves the runner without a target configuration.

Explicit attachment is available independently of runner sessions:

```python
from vectorops.runner.environment import models
from vectorops.runner.environment.configuration import LocalEnvironmentConfig
from vectorops.runner.environment.factory import open_environment

environment = await open_environment(
    LocalEnvironmentConfig(base_project_path="/home/user/projects/example")
)
result = await environment.read_lines(
    models.LineReadRequest(path="src/main.py", start_line=10, max_lines=20)
)
```

Call `await environment.close()` when releasing an attachment that has launched processes. Filesystem operations hold no persistent handles. Blocking filesystem work runs off the event loop. Cancelling an await does not stop an in-flight filesystem operation; a cancelled mutation can still take effect.

### Filesystem contracts

All operations accept request models from `environment.models`. Paths use `/`, are target-root-relative, and default to `.` where applicable. Empty paths, NULs, absolute paths, and any `..` component are rejected. The target root cannot be written, removed, renamed, or created through mutation methods.

| Method | Request | Behavior |
| --- | --- | --- |
| `describe` | None | Identity as a resolved file URI, platform, capabilities, effective limits, and isolation flags |
| `stat` | `PathRequest` | Metadata for the entry itself; does not follow a final symlink |
| `list_dir` | `ListRequest` | Unsorted metadata entries, default limit 1000, explicit `truncated` flag; no pagination or snapshot guarantee |
| `read_file` | `ReadRequest` | Regular-file bytes, nonnegative offset, positive limit (default 65536), explicit EOF |
| `read_lines` | `LineReadRequest` | UTF-8 text, one-based `start_line`, positive `max_lines` (default 100), positive `byte_limit` (default 1048576) |
| `write_file` | `WriteRequest` | Bytes with `create` (default), `replace`, or `upsert` mode; parent must exist |
| `mkdir` | `MkdirRequest` | `parents` and `exist_ok` default to false |
| `remove` | `RemoveRequest` | Removes an entry or empty directory; `recursive` defaults to false; missing paths fail |
| `rename` | `RenameRequest` | Required destination, `overwrite` defaults to false; cross-filesystem moves are unsupported |

Reads follow symlinks only when the resolved target stays inside the root. Parent components are resolved and checked for every operation. Listing and `stat` inspect final symlinks without following them, including dangling or outside-pointing links. Removal and rename operate on the final entry itself, not its symlink target. Writes reject existing final symlinks. Creation and rename destinations have their resolved parents checked against the root.

Writes publish a same-directory temporary file atomically. New files have mode `0600`; replacement preserves permission bits but does not promise preservation of ownership, ACLs, extended attributes, or hard-link relationships. Atomic publication is not crash durability or a multi-file transaction. `replace` requires an existing regular file at validation time; `upsert` also permits creation. Neither replacement nor non-overwriting rename offers an atomic conditional-update guarantee against concurrent writers.

Byte reads return at most the requested limit, using up to one extra byte internally to determine EOF. Line reads count bytes scanned before `start_line` toward their budget, preserve LF/CRLF and final-newline state, and use strict incremental UTF-8 decoding. Bare CR is not a line separator. Results include `text`, `complete_lines`, `bytes_read`, `eof`, and `truncated`; an EOF-terminated final line counts as complete. Exhausting the budget before fulfilling the range sets `truncated`, including when the starting line has not been reached. A partial multibyte character at the budget boundary is withheld rather than corrupted. Returning the requested number of lines is not truncation merely because later lines exist. Chunk read-ahead counts toward `bytes_read`, but is excluded from returned text. Reads do not provide a consistent snapshot across concurrent edits.

Per-request read limits and line byte budgets cannot exceed `max_read_bytes`; writes and listings similarly enforce their configured caps. Binary content serializes as base64 in JSON. `EnvironmentError` distinguishes missing paths, denied access, outside-root paths, invalid paths, conflicts, unsupported operations, resource limits, invalid UTF-8, and other I/O failures. Its `may_have_occurred` flag conservatively marks OS failures during mutations, which can leave partial effects, particularly recursive removal or parent creation.

Race-resistant containment is not implemented. Resolved-path checks are not a sandbox or protection against hostile concurrent path replacement. The local process capability is true; patching is unsupported. Runtime workflow injection is implemented; durable target binding, process replay, rollback, and agent-facing tools are not.

## Implemented process management

Every `exec` allocates a fresh opaque session ID and starts a separate process. It never writes a new command into an existing shell. The environment owns a `ProcessManager` whose registry, output collection, and lifecycle tasks survive cancellation of individual requests and model turns. This is in-memory ownership, not runner-crash recovery.

Local execution is available whenever a local environment is attached; it needs no additional enable flag. Commands have the runner account's permissions and are not confined by the project working directory. Shell variants require `allow_shell = true`; this is API policy, not a security boundary against an argv command that explicitly invokes a shell.

### Process configuration

These optional tables extend the existing environment configuration:

```toml
[environment.processes]
allow_shell = false
max_sessions = 32
max_output_bytes = 1048576
max_response_bytes = 65536
max_stdin_bytes = 65536
retention_seconds = 300
termination_grace_seconds = 1
io_timeout_seconds = 5

[environment.processes.environment]
inherit_parent = true
```

The values shown are defaults. Limits and durations must be positive. `max_sessions` counts starting, running, and retained completed sessions. `io_timeout_seconds` bounds attachment, stdin I/O, backend control/cleanup, and post-exit output draining; it is not a command runtime limit.

Environment-variable handling matches `codeng`'s `EnvPolicy`: inherit the parent environment at spawn time by default, restrict inherited names with optional `allowlist`, remove inherited names in optional `denylist`, apply configured `defaults`, then apply the request's `env` overrides. Filters apply only to inheritance; explicit defaults and overrides can reintroduce filtered names. The parent environment is never mutated. Inheritance can include credentials: use `inherit_parent = false` or an explicit allowlist when that is not intended. Future remote drivers choose their own target-side parent environment rather than inheriting runner-host variables automatically.

### Requests and results

Process models live in `environment.processes.models`.

| Environment method | Request | Contract |
| --- | --- | --- |
| `exec` | `ExecRequest` | Fresh command and session; waits up to `yield_timeout_ms` (default 1000, maximum 30000) for completion |
| `poll` | `PollRequest` | Session ID, byte cursor (default 0), optional wait up to 30000 ms; reads the shared output buffer |
| `write_stdin` | `StdinRequest` | Session ID, byte data, optional EOF; writes are serialized and concurrent submissions rejected |
| `kill` | `SessionRequest` | Graceful termination followed by forced termination; returns confirmation and attempted scope |
| `list_processes` | None | Non-consuming session status for inspection and recovery of IDs after cancelled calls |
| `close` | None | Idempotent process-manager shutdown; rejects new executions and returns termination reports |

`ExecRequest.command` is an `ArgvCommand(kind="argv", argv=[...])` or `ShellCommand(kind="shell", script="...")`. Local shell commands run in a fresh `/bin/sh -c`. `cwd` defaults to `.` and uses the filesystem path checks. `env` defaults to no overrides. `stdin` defaults to empty bytes and `keep_stdin_open` defaults to false; set it true for later interaction. `execution_timeout_ms` is optional and measures runtime after attachment, independently of the yield deadline. `mode` defaults to `pipe`; `pty` fails explicitly with the initial local driver.

```python
from vectorops.runner.environment.processes import models as processes

session = await environment.exec(
    processes.ExecRequest(
        command=processes.ArgvCommand(argv=["python3", "-u", "server.py"]),
        keep_stdin_open=True,
        yield_timeout_ms=100,
    )
)
if session.retained:
    update = await environment.poll(
        processes.PollRequest(
            process_id=session.process_id, cursor=session.cursor, wait_timeout_ms=100
        )
    )
await environment.close()
```

Results contain state (`starting`, `running`, `exited`, `failed`, or `unknown`), tagged byte chunks, the next byte cursor, dropped-byte count, exit code when known, an optional error, timeout status, and retention/output-completion flags. A nonzero exit code is a normal exit. Unknown outcomes never invent an exit code. Spawn failures return a failed result; invalid policies, unknown IDs, and resource-limit violations raise `EnvironmentError`. Binary payloads serialize as base64.

Output collectors run continuously, even without polling. Bounded buffers evict oldest bytes and report gaps relative to the supplied cursor while continuing to drain pipes. Each response is capped by `max_response_bytes`. Stream order is preserved individually; interleaving stdout and stderr reflects collection order, not a guaranteed target-time ordering. `output_complete` means collection has ended and this response reaches the buffer end; inspect `error` and `dropped_bytes` for incomplete capture.

Fully collected quick exits are forgotten immediately. Otherwise the session is retained before returning, including completed commands with unread output. Final polling releases a completed session; uncollected completed sessions expire after `retention_seconds`. Listing does not consume output; use cursor 0 to recover its retained output. IDs belong to one attachment, not an OS PID. Cancelling exec, poll, stdin, kill, or close does not implicitly kill a process; submitted lifecycle/control tasks remain manager-owned. A cancelled stdin call can still send bytes.

### Reusable drivers and termination guarantees

`ProcessDriver.spawn` returns a runtime `ProcessHandle` supporting output iteration, stdin/EOF, waiting, termination, and transport cleanup. Drivers report PTY, EOF, and termination-scope capabilities. The shared manager owns IDs, buffering, cursors, retention, deadlines, and escalation. `build_environment` is a pure helper accepting a parent mapping; only the local driver reads `os.environ`.

Drivers must clean up or report uncertainty when spawning is cancelled or fails after an effect. `EnvironmentError(may_have_occurred=True)` during spawn produces an unknown outcome. A confirmed `ExitResult` requires a return code. Driver `close` releases transport resources; it must not silently claim termination. Cancellation of manager shutdown continues cleanup in the background.

`ExecRequest.kill_tree` defaults to true. The local driver starts a fresh POSIX process group; kill, runtime timeout, and environment shutdown signal that group with SIGTERM and escalate to SIGKILL. When the command exits, remaining group members are forcibly cleaned up before the session is released, including children that ignored SIGTERM or closed their output pipes. This also applies on normal command exit: background children do not outlive a tree-owned command. Set `kill_tree=false` to signal only the command and leave its descendants unmanaged.

Confirmation concerns the managed command's exit, while `scope` describes the signal target; it is not proof that every descendant is gone. Detached descendants can escape group cleanup. This flag does not provide automatic termination after runner SIGKILL or a crash. Pipes retained by unmanaged descendants are drained for a bounded interval after the main command exits, then closed with an explicit capture error. Shutdown does not delete target files or destroy an environment.

Docker and SSH drivers are not implemented. Docker exec IDs and SSH channels can supply backend handles without changing shared polling or retention. Neither channel closure nor a remote PID alone proves termination. Reattachment across connection loss, PTYs, process isolation, LLM tools, and saga integration are outside this implementation.

## Scope and decisions

- Expose one async `TargetEnvironment` interface for filesystem operations, patching, and process execution.
- Implement only `LocalEnvironment` in v1. Design for SSH and Docker without implementing their transports or provisioning.
- Require no Python installation, custom helper, or patch executable on the target. File operations and patching must work with process execution disabled.
- Support bounded partial reads, including byte ranges and model-facing line ranges.
- Keep target identity, capabilities, policy, and actual isolation guarantees explicit.
- Preserve existing recorded activities, tool validation, and approvals.

## Architecture and ownership

```text
Workflow / agent loop
    -> recorded tool activity
        -> ActivityExecutionContext.environment
            -> TargetEnvironment
                -> filesystem backend
                -> shared runner-side patch engine -> filesystem backend
                -> process backend

Runner composition
    -> EnvironmentManager
        -> configuration, target binding, attachment, lifecycle
```

`TargetEnvironment` is the sole target I/O interface used by standard tools. Tools must not branch on backend type, access target files with runner-local filesystem APIs, or launch target commands directly through subprocess APIs. Internal filesystem and process components must refer to the same bound target.

The environment subsystem owns configuration validation, path interpretation, policy enforcement, backend capabilities, and patch execution. The manager owns attachment and cleanup; tools receive only the operational interface. Connection credentials and live handles are not serialized into workflow history.

The runner owns trusted workflow code, configuration, history, logs, and connection credentials. The target owns project files, build outputs, and target processes. `RunnerProject` remains runner infrastructure, not the target root.

Trusted Python activities can still bypass an interface by importing filesystem or subprocess modules. This design assumes trusted runner/tool implementations handling untrusted model arguments and target content; it does not sandbox arbitrary activity code.

## Interface surface

Use a runtime `Protocol` with async I/O methods and typed request/result models. Serializable contracts use Pydantic v2; variant models use stable `kind` discriminators. Runtime process handles and connections stay separate from those models.

Method names below describe the proposed surface, not a finalized signature specification.

| Operation | Contract |
| --- | --- |
| `describe` | Bound target identity, platform, capabilities, effective policy, and isolation guarantees |
| `stat`, `list_dir` | Typed file metadata and bounded directory listings, with explicit symlink behavior |
| `read_file` | Byte-range read using an offset and positive size limit |
| `write_file` | Byte content, explicit create/replace behavior, and declared atomic-replacement guarantees |
| `mkdir`, `remove`, `rename` | Explicit parent-creation, recursive, and overwrite options; non-destructive defaults |
| `apply_patch` | Format-tagged patch request and structured per-file results, conflicts, and errors |
| `exec`, `poll`, `write_stdin`, `kill` | Implemented managed process sessions; backend handles remain private |

Capabilities distinguish backend support from permission. For example, a backend can support execution while the configured policy disables it. Unsupported guarantees must fail explicitly, never silently fall back to runner-local I/O.

Errors distinguish missing paths, denied access, paths outside the root, conflicts, unsupported operations, resource limits, transport failures, and unknown outcomes. Error reporting must state when a mutation or command could already have occurred.

## Filesystem and partial reads

### Paths and containment

Paths are target-root-relative and use a defined API separator independent of the runner OS. Reject absolute paths, NULs, and traversal outside the configured root. Do not expose local `Path` objects or connection-specific paths to tools.

The backend validates resolved paths against the configured root, including parent components, symlinks, creation destinations, and rename destinations. Use path-component containment checks rather than string-prefix comparisons.

Race-resistant containment is out of scope. Paths can change between validation and I/O; handle-relative/no-follow operations and protection against concurrent symlink or directory replacement are not required. These checks are not a security boundary against hostile concurrent filesystem changes.

Scoped filesystem access does not guarantee that underlying data is exclusive to the root: hard links and mounted content need consideration when provisioning a security boundary.

### Byte ranges

`read_file` accepts a nonnegative byte `offset` and a positive `limit`. It returns no more than that limit and reports whether EOF was reached. An offset beyond EOF returns empty content with EOF set. Backend short reads are not, by themselves, evidence of EOF.

For example, reading `src/main.py` with offset `4096` and limit `4096` requests at most the next 4 KiB. Full-file reading builds on the same primitive and enforces a total size limit.

Byte operations do not decode text. This avoids corrupting binary files or splitting multibyte characters incorrectly in the filesystem layer.

### Line ranges

Model-facing file tools support a one-based `start_line` and positive `max_lines`, with an additional byte limit. A shared text-reading layer performs incremental decoding and scanning through the byte API; backends can optimize without changing the tool contract.

Locating line N requires scanning preceding content unless an index or backend optimization exists. A single line can exceed the byte limit, so results must identify truncation rather than claim to contain complete requested lines.

Multiple partial reads are not automatically a consistent snapshot. Detect and report changes when backend metadata permits, but do not claim snapshot consistency without an enforceable backend guarantee.

## Patching

### Public operation, shared engine

`apply_patch` belongs to `TargetEnvironment`, so callers do not orchestrate transfers or understand backend details. Its implementation uses a shared runner-side engine:

1. Parse the selected format and validate affected paths.
2. Load necessary target files through the backend under size limits.
3. Compute changes in memory and validate the plan before mutation.
4. Apply planned filesystem operations through the backend.
5. Return actual changes, conflicts, skipped operations, and failures.

V1 generally reads each affected existing file in full. Partial reads remain independently useful for browsing large files. Additions need no existing content; deletions and pure renames do not inherently require content transfer unless the selected patch semantics require content validation.

The engine owns decoding, newline and final-newline preservation, context matching, and format-specific behavior. Filesystem primitives remain byte-oriented. Unsupported encodings or binary patch features must fail explicitly rather than corrupt content.

### Format examples

The reviewed examples in `~/work/codeng/src/vocode/patch/` separate parsing, file loading, change planning, and application. `PatchFileOps` supplies read/write/delete callbacks; V4A moves are implemented as destination writes followed by source deletion. The examples permit partial application and collect errors rather than providing transactions.

The format named `patch` in those examples is SEARCH/REPLACE, not standard unified diff. The environment contract accommodates V4A and diff adapters, but the parser scope still needs confirmation: SEARCH/REPLACE, standard unified diff, or both. Keep distinct format identifiers rather than silently treating these formats as interchangeable.

Reuse the algorithmic separation, not synchronous callback I/O: load asynchronously, plan without I/O, and apply asynchronously. Reverse application and preview can use the same planning layer without becoming separate backend implementations.

### Failure and concurrency guarantees

Validate the complete plan before starting mutations. Known parse, path, or context errors should prevent mutation; best-effort application of a partially valid plan is not an implicit default. Runtime failures can still leave partial changes, which must be reported accurately.

Avoid unchanged writes and use atomic single-file replacement where supported. Edited moves can require several operations. There is no general multi-file transaction or automatic rollback guarantee.

Detect stale content before replacement, but distinguish detection from atomic compare-and-swap. A read/hash/write sequence does not exclude concurrent writers. Conditional updates can be advertised only where the backend enforces them against relevant writers.

### Future remote backends

An SSH implementation can use SFTP to download necessary files and upload replacements. Docker can use file-transfer APIs. Neither needs to execute the patch engine on the target.

Transferring affected files is the tradeoff for avoiding target-side dependencies. Uploading a temporary file followed by rename can provide atomic single-file replacement where supported, but does not make the whole patch transactional or eliminate concurrent-edit races. Future optimizations must preserve the public contract and must not make target-side helpers mandatory.

## Processes and isolation

Process requests specify argv, target-relative working directory, environment overrides, optional stdin, timeout, and output limits. Shell scripts are a separate, explicitly permitted command variant. Do not use runner-global `chdir` or mutable shared command state.

The local backend inherits the runner's environment by default, with configurable filtering and overrides as described above. Remote backends must define their own environment policy explicitly.

A process handle supports bounded stdout/stderr streaming, waiting, and cancellation. Preserve ordering within each output stream, without promising a total order across streams. Continue draining or terminate according to limit policy after captured output reaches its bound; do not deadlock the child on a full pipe.

Nonzero exit status is a process result, distinct from transport failure. Execution timeouts and explicit kill attempt termination; request cancellation leaves the process managed and running. A future remote disconnect can leave a process running and must produce an unknown outcome rather than a false success.

Directory-scoped filesystem access is not process isolation. Local commands can access anything permitted to the runner's OS account unless an actual sandbox is configured. Selecting a local environment enables host execution. Future SSH commands have remote-account permissions; Docker commands have the container's permissions and mounts. Backend names alone do not imply adequate isolation.

Enforce policy at the environment boundary as well as tool approval. Bind approvals to the operation and target identity; approval does not strengthen isolation guarantees.

## Runner integration and recovery

### Implemented runtime ownership

`WorkflowRunner` creates an `ExecutionResources` scope for each root run, attaches the optional configured environment, and passes both through `WorkflowExecutor` and `ExecutionSession`. Activities receive `ctx.environment` and `ctx.resources`; neither is serialized into history or compared as part of activity identity. Without environment configuration, `ctx.environment` is `None`, but the cleanup registry is still available. Direct executor/session construction accepts optional externally owned dependencies and does not close them.

Child workflows borrow the root's attachment and registry. Finishing a child or a tool call does not close the environment. Concurrent root executions have independent attachments and process managers, even when their target path is the same.

On root completion, failure, cancellation, or suspension, the engine first stops and awaits activities and child workflows. It then awaits registered cleanup callbacks in reverse registration order. Environment cleanup is registered first, so other resources can still use it while closing. Repeated cancellation does not interrupt owned teardown. `runner.stop()` requests cancellation and returns immediately; awaiting the run task waits for cleanup.

Other activity-owned resources can register an async, no-argument finalizer with `ctx.resources.register("resource name", cleanup)`. Registration is synchronous, is rejected once closing starts, and each callback is invoked once. Finalizers must implement their own bounded I/O/timeouts. Cleanup continues after callback failures; failures are logged as `execution.cleanup_failed`. On an otherwise successful run they raise an exception group; an existing workflow failure or cancellation remains the primary outcome. Environment termination reports with unconfirmed processes become cleanup failures rather than being silently ignored.

This is runtime resource ownership, not an event subscription or saga compensation. Step notifications remain observational. Forced runner termination cannot execute finalizers. On resume a new attachment is created; a recorded process ID is not restored or automatically respawned.

### Deferred durable binding and recovery

Persist the selected target identity and relevant configuration/policy fingerprint with the execution before effects. Child workflows inherit the binding by default. Resume must not silently switch targets after a profile changes. Older executions remain valid without an environment binding.

Completed activities replay recorded results rather than repeat effects. Interrupted writes and commands can already have taken effect; they need reconciliation or an explicit recovery decision before retry. Operation identifiers help track effects but do not create exactly-once execution.

Workflow replay does not restore historical target files. Branching from earlier history still uses the current filesystem unless a future snapshot mechanism explicitly restores it.

Adapt process output to the existing replacement-snapshot activity streaming contract instead of treating raw stdout chunks as durable snapshots. Keep captured output and persisted snapshots bounded.

The manager owns resource release, not implicit target destruction. Closing an attachment is distinct from deleting a directory, stopping a container, or destroying a remote target.

## Delivery and validation

1. Add typed contracts, a fake backend, optional activity-context injection, and additive target-binding persistence.
2. Implement local filesystem operations, bounded byte reads, and the shared line-reading layer.
3. Implement the shared patch engine and environment patch operation for the confirmed formats.
4. Local execution, bounded output, timeouts, and request-independent lifecycle are implemented; workflow recovery handling remains deferred.

Run shared contract tests against fake and local environments. Cover path traversal, resolved symlink paths outside the root, creation and rename destinations outside the root, byte-range boundaries, EOF, multibyte decoding, oversized lines/files, newline preservation, patch add/update/delete/move behavior, conflicts, partial runtime failures, and unsupported guarantees. Concurrent path-replacement races are outside the validation scope.

Extend activity/tool tests for dependency injection, child binding inheritance, approvals, replay without repeated effects, unknown outcomes, and target mismatch on resume. Process tests cover argv handling, cwd, environment inheritance/filtering, output bounds, cancellation, retention, termination, and unsupported capabilities.

Docker/SSH transports, provisioning, remote helpers, PTYs, snapshots, and distributed filesystem transactions are outside v1. Before implementation, finalize supported local platforms, exact patch formats, and concrete resource-limit defaults; none are implied by this proposal.

## Related

- [Activities](activities.md)
- [Workflows](workflows.md)
- [Component configuration](../config/components.md)