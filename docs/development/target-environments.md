# Target environments

Status: the filesystem foundation is implemented in `vectorops.runner.environment`. Process management, patching, and workflow integration below remain proposed design, not implemented APIs.

A target environment is the filesystem and execution context that agent tools can inspect and modify. This design separates target operations from the runner so the same tools can work locally or against future remote backends.

## Implemented filesystem foundation

The initial package provides an async `TargetEnvironment` protocol, typed Pydantic requests/results, a POSIX `LocalEnvironment`, a backend factory, and shared bounded UTF-8 line reading. It has no saga, activity, persistence, patching, or process dependencies. Runner configuration selects a backend but does not attach it or inject consumers yet.

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

The local backend holds no persistent file handles and needs no close operation. Blocking filesystem work runs off the event loop. Cancelling an await does not stop an in-flight filesystem operation; a cancelled mutation can still take effect.

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

Race-resistant containment is not implemented. Resolved-path checks are not a sandbox or protection against hostile concurrent path replacement. Process and patch capabilities are false. There is no automatic workflow binding, replay handling, rollback, or consumer integration in this foundation.

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
| `start_process` | Explicit command request; returns a handle for output, completion, and cancellation |

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

Do not implicitly forward the runner's entire environment or credentials. Define the base process environment and permitted overrides explicitly.

A process handle supports bounded stdout/stderr streaming, waiting, and cancellation. Preserve ordering within each output stream, without promising a total order across streams. Continue draining or terminate according to limit policy after captured output reaches its bound; do not deadlock the child on a full pipe.

Nonzero exit status is a process result, distinct from transport failure. Timeouts and cancellation attempt termination and report whether it was confirmed. A future remote disconnect can leave a process running and must produce an unknown outcome rather than a false success.

Directory-scoped filesystem access is not process isolation. Local commands can access anything permitted to the runner's OS account unless an actual sandbox is configured. V1 host execution is explicitly enabled, not enabled by default. Future SSH commands have remote-account permissions; Docker commands have the container's permissions and mounts. Backend names alone do not imply adequate isolation.

Enforce policy at the environment boundary as well as tool approval. Bind approvals to the operation and target identity; approval does not strengthen isolation guarantees.

## Runner integration and recovery

Add an optional environment dependency to `ActivityExecutionContext`. Runner composition resolves the configuration and injects the bound environment through executor/session construction. Keep target side effects inside recorded activities and preserve existing tool validation and approval composition.

Persist the selected target identity and relevant configuration/policy fingerprint with the execution before effects. Child workflows inherit the binding by default. Resume must not silently switch targets after a profile changes. Older executions remain valid without an environment binding.

Completed activities replay recorded results rather than repeat effects. Interrupted writes and commands can already have taken effect; they need reconciliation or an explicit recovery decision before retry. Operation identifiers help track effects but do not create exactly-once execution.

Workflow replay does not restore historical target files. Branching from earlier history still uses the current filesystem unless a future snapshot mechanism explicitly restores it.

Adapt process output to the existing replacement-snapshot activity streaming contract instead of treating raw stdout chunks as durable snapshots. Keep captured output and persisted snapshots bounded.

The manager owns resource release, not implicit target destruction. Closing an attachment is distinct from deleting a directory, stopping a container, or destroying a remote target.

## Delivery and validation

1. Add typed contracts, a fake backend, optional activity-context injection, and additive target-binding persistence.
2. Implement local filesystem operations, bounded byte reads, and the shared line-reading layer.
3. Implement the shared patch engine and environment patch operation for the confirmed formats.
4. Add explicitly enabled local execution, bounded output, timeouts, cancellation, and recovery handling.

Run shared contract tests against fake and local environments. Cover path traversal, resolved symlink paths outside the root, creation and rename destinations outside the root, byte-range boundaries, EOF, multibyte decoding, oversized lines/files, newline preservation, patch add/update/delete/move behavior, conflicts, partial runtime failures, and unsupported guarantees. Concurrent path-replacement races are outside the validation scope.

Extend activity/tool tests for dependency injection, child binding inheritance, approvals, replay without repeated effects, unknown outcomes, and target mismatch on resume. Process tests cover argv handling, cwd, environment leakage, output bounds, cancellation, and execution-disabled mode.

Docker/SSH transports, provisioning, remote helpers, PTYs, snapshots, and distributed filesystem transactions are outside v1. Before implementation, finalize supported local platforms, exact patch formats, and concrete resource-limit defaults; none are implied by this proposal.

## Related

- [Activities](activities.md)
- [Workflows](workflows.md)
- [Component configuration](../config/components.md)