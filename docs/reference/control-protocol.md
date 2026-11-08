# Runner control protocol

The host controls one workspace runner through authenticated JSON packets. A runner owns multiple concurrent top-level executions, each with its own history and resources; children run within their root execution. One daemon retains exclusive workspace writer ownership. Workflow code remains on the runner's filesystem.

Use this reference when building a client or integration. For terminal usage, start
with the [CLI guide](../usage/cli.md) and [daemon setup](../usage/daemon.md).

## Contents

- [Quick start](#quick-start)
- [Framing and authentication](#framing-and-authentication)
- [Packet families](#packet-families)
- [Methods](#methods)
- [History and live reconciliation](#history-and-live-reconciliation)
- [Tool approval](#workflow-scoped-tool-approval)
- [Conversation inboxes and steering](#conversation-inboxes-and-llm-steering)

## Quick start

```python
from pathlib import Path
from vectorops.runner.control.client import embedded
from vectorops.runner.control.protocol import Start
from vectorops.runner.runner.history_query import HistoryQuery

async def run():
    async with embedded(Path("/path/to/workspace")) as host:
        await host.subscribe()
        accepted = await host.start(Start(
            reference="report:main",
            input={"date": "2026-01-15"},
            idempotency_key="report-2026-01-15",
        ))
        while True:
            packet = await host.next_event()
            if packet.event.get("execution_id") == accepted.execution_id and packet.event["kind"] in (
                "execution.completed", "execution.failed", "execution.cancelled"
            ):
                break
        history = await host.history(HistoryQuery(
            execution_id=accepted.execution_id, limit=100
        ))
        return history
```

`embedded` owns the service lifetime and stops remaining executions on exit, preserving executions waiting for mailbox input for recovery. Authentication must be configured before workflows run; it never suspends execution. Closing an ordinary `RunnerClient` does not stop its runner. `RunnerService` and `RunnerServer` can be managed separately when an embedded service must outlive clients.

## Framing and authentication

Every frame is a four-byte unsigned big-endian length followed by UTF-8 JSON. The maximum payload is 8 MiB. Memory and TCP transports use the same framing and validation. TCP binds only to `127.0.0.1`; use SSH forwarding for remote access.

Protocol version 2 uses a bearer token loaded from runner-global configuration by default, with optional explicit provisioning. Authentication completes within 10 seconds before any operational packet is accepted. The client sends:

```json
{"kind":"authenticate","version":2,"token":"<harness-generated-token>"}
```

The server compares the token in constant time and acknowledges:

```json
{"kind":"authenticated","version":2,"instance_id":"runner-instance-id","session_id":"session-id"}
```

The default token is generated once using 32 random bytes and reused across restarts. Explicit tokens contain 43-4096 URL-safe characters (`A-Z`, `a-z`, `0-9`, `_`, `-`) and must use at least 32 cryptographically random bytes. Length validation does not establish randomness. All authenticated sessions have full controller access. Failed authentication closes the connection without echoing the token. Authentication packets must never be logged.

The token is transmitted to the endpoint. Bearer authentication does not independently authenticate the runner, prevent replay of a stolen token, encrypt traffic, or sandbox workflow code. Trust the harness-provided local endpoint or a host-key-verified SSH tunnel. Loopback TCP assumes a trusted local machine; privileged local processes and compromised same-user processes are outside this security boundary. Direct remote connections would require TLS and are not supported.

Pass a text token to `RunnerClient.tcp(port, token)` or
`RunnerClient.connect(stream, token)`, or omit the TCP token for local global-config
lookup. See [compatibility](compatibility.md#storage-and-control-clients) when
upgrading clients using the earlier handshake.

## Packet families

```json
{"kind":"request","id":"req-1","method":"executions.current","params":{}}
```

```json
{"kind":"response","id":"req-1","result":null}
```

```json
{"kind":"error","id":"req-1","code":"execution_conflict","message":"execution is already active"}
```

```json
{"kind":"event","subscription_id":"sub-1","sequence":1,"event":{"kind":"execution.started","execution_id":"exec-1"}}
```

Requests may run concurrently and responses may arrive out of order. Events interleave with responses. IDs must be unique among outstanding requests. `ping` and `pong` carry an `id`. Session limits are 32 outstanding requests and 16 subscriptions; the server permits 64 connections.

Error codes include `invalid_argument`, `not_found`, `runner_busy`, `execution_conflict`, `idempotency_conflict`, `runner_stopping`, `unsupported_method`, `resource_limit`, `frame_too_large`, and `internal_error`. Invalid framing, invalid session packets, failed authentication, and overloaded event consumers close the connection. Internal request tracebacks remain in runner logs.

## Methods

| Method | Parameters | Result |
| --- | --- | --- |
| `runner.info` | None | Identity, workspace, protocol version, limits |
| `runner.disk_usage` | `top`: integer, default 10, range 0–500 | Workspace file bytes, workflow count, and largest workflow execution directories |
| `executions.disk_usage` | Required `execution_id` | Selected execution's `execution_id` and `size_bytes` |
| `runner.shutdown` | `mode`: `cancel` (default) or `drain` | Shutdown accepted |
| `workflows.list` | `after`, `limit` (default 100, maximum 500) | Decorated workspace descriptors, active execution/input/auth counts, discovery errors, and `next_after` |
| `workflows.describe` | `reference` | Workflow descriptor including input schema |
| `executions.summaries` | `after`, `limit`, `active_only` (default false) | Execution summaries and `next_after` |
| `executions.summary` | `execution_id` | Root reference, lifecycle status, pending-input count, active flag, authentication wait |
| `executions.current` | Optional `execution_id` | Selected active execution/status, or null; without an ID, conflicts when multiple executions are active |
| `executions.active` | `after`, `limit` (default 100, maximum 500) | Lexically ordered `executions` with ID/status and `next_after` |
| `executions.list` | `after`, `limit` (default 100, maximum 500) | Lexically ordered execution IDs and `next_after` |
| `executions.get` | `execution_id` | Persisted execution record and checkpoint |
| `executions.start` | `reference`, `input`, `idempotency_key`, optional `base_project_path` or `worktree_id` | Execution ID and `starting` status |
| `worktrees.create`, `worktrees.list`, `worktrees.get`, `worktrees.update`, `worktrees.delete` | [Worktree CRUD parameters](../usage/worktrees.md#crud-protocol) | Worktree records or paginated listing |
| `executions.resume` | `execution_id`, `idempotency_key` | Execution ID and `starting` status |
| `executions.stop` | `execution_id` | Active execution with `stopping` status |
| `history.query` | History query below | Ordered history items and `next_cursor` |
| `events.subscribe` | Optional `execution_id`, optional `kinds` list | `subscription_id` |
| `events.unsubscribe` | `subscription_id` | Null |
| `logs.list` | None | Retained log filenames/sizes and dropped/write-error counters |
| `logs.download` | `file`, `offset`, `limit`, optional `end` | Base64 bytes, `next_offset`, `end`, `eof` |
| `configuration.get` | Optional dotted string `name`, default `""` | Canonical `name`, `exists`, effective JSON `value` |
| `configuration.set` | String `name`, required JSON `value`, strict boolean `permanent` (default false) | Null acknowledgment after application |
| `configuration.reset` | String `name` | Null acknowledgment after removing overlapping overrides |
| `configuration.suggest` | String `name`, optional string `prefix` (default empty) | Advisory `values` array |

Workflow references are `path:callable` relative to the workflows directory,
`python:package.module:callable` for library workflows, or registered built-ins such
as `builtin:agent`. Local references omit `.py` and must remain inside the workflows
directory after symlink resolution. The callable must be async. Resolution failures
reject the request before acceptance; execution failures arrive as lifecycle events.
Discovery imports trusted Python code and is not a sandbox.

`workflows.list` dynamically scans Python files under the configured workflows
directory and returns only locally defined `@workflow` bindings. Hidden paths and
`__pycache__` are skipped; symlink directories are not traversed. Unchanged files use
the existing resolver cache. Per-source failures appear in `errors` with `source`
and `error`; other files remain discoverable. `after` is the last canonical reference,
and definitions are sorted by reference. Errors are returned on each page.
`workflows.describe` also accepts explicit undecorated and library references.

Each catalog entry contains `descriptor`, `active_executions`, `pending_inputs`,
and `awaiting_authentication` (retained field, always zero).
Counts aggregate active root executions matching that definition; pending inputs
include all children of those roots. An unavailable definition does not hide its
running executions from `executions.summaries`.

Execution summaries contain `execution_id`, nullable `reference` and
`root_workflow_id`, `status`, `active`, `pending_inputs`, and
`awaiting_authentication` (retained boolean, always false). Startup entries can lack root identity before
history is initialized. `status` includes live `starting`/`stopping` states as well
as persisted lifecycle states. `active` identifies current service ownership;
persisted `running` alone is not proof of active execution. The input count is
queried across root and child mailbox records without decoding step histories.
Authentication is managed separately through explicit `auth.login` or `mcp.login`
and the [authentication protocol](../config/authentication.md#authentication-rpc-reference).
Missing or failing authentication fails workflow execution without creating a login request.
Pagination is lexical by execution ID
and views can change between requests. The `executions.list` response is unchanged.
`executions.get` returns the persisted execution record, including its nullable
`target` binding (`{"kind":"local","root":"/absolute/resolved/path"}`).

Start and resume receipts are persisted before launch. Repeating the same idempotency key and arguments returns the original acceptance, even after restart; different arguments produce `idempotency_conflict`. Acceptance is not proof of successful execution. A crash between receipt persistence and launch can leave an accepted ID without execution history; retries do not automatically execute it. Retention can also remove history without removing its receipt. Use a new key only for an intentional new attempt.

Stop is cooperative and addresses the specific active execution. It returns before cleanup; wait for `execution.cancelled`, `execution.completed`, or `execution.failed`. Workflows that block the event loop or suppress cancellation can delay shutdown. Forced termination is not supported.

### Workspace disk usage

Use `runner.disk_usage` to measure the configured workspace directory (`.voc` by
default), including workflow definitions, execution databases and their SQLite
sidecars, artifacts, logs, and shared runner state. The typed client exposes
`await client.runner_disk_usage(top=5)`.

```json
{"kind":"request","id":"disk-1","method":"runner.disk_usage","params":{"top":5}}
```

```json
{"kind":"response","id":"disk-1","result":{"total_bytes":8192,"workflow_count":2,"top_workflows":[{"execution_id":"exec-large","size_bytes":4096},{"execution_id":"exec-small","size_bytes":1024}]}}
```

| Result field | Type | Meaning |
| --- | --- | --- |
| `total_bytes` | Nonnegative integer | Sum of regular-file sizes throughout the workspace |
| `workflow_count` | Nonnegative integer | Number of execution directories under the configured data directory, before applying `top` |
| `top_workflows` | Array | At most `top` entries, sorted by descending `size_bytes`, then ascending `execution_id` for ties |
| `top_workflows[].execution_id` | String | Execution ID; use `executions.summary` to look up its workflow identity |
| `top_workflows[].size_bytes` | Nonnegative integer | Recursive file sizes for this execution, including its root workflow and children |

Root and child workflows share an execution database, so usage is attributed to
the execution rather than individual child workflows. Execution directories are
counted even when empty or partially created; they need not have a readable
history record. Shared runner files and workflow definitions contribute to
`total_bytes`, not individual workflow entries. `top: 0` returns totals without
ranked entries. Unknown parameters and non-integer or out-of-range limits are rejected.

Sizes are logical file bytes, not allocated filesystem blocks; hard-linked files
are counted per path. Symlinks and special files are excluded. Scanning runs off
the event loop and does not read database contents. Results are a live estimate,
not an atomic snapshot: files removed during scanning are skipped, and other I/O
errors fail the request rather than returning an unmarked partial total.

### Execution disk usage

Use `executions.disk_usage` to measure one execution without scanning the rest of
the workspace or relying on the top-workflow ranking. The typed client exposes
`await client.execution_disk_usage("exec-large")`.

```json
{"kind":"request","id":"disk-2","method":"executions.disk_usage","params":{"execution_id":"exec-large"}}
```

```json
{"kind":"response","id":"disk-2","result":{"execution_id":"exec-large","size_bytes":4096}}
```

The required `execution_id` uses the existing execution-ID contract: 1–128 letters,
digits, underscores, or hyphens. Invalid IDs and extra parameters return
`invalid_argument`. Missing directories, non-directory entries, and symlinked
paths below the workspace return `not_found`; an existing empty directory returns
zero bytes. No history database or execution record is required.

`size_bytes` follows the same accounting rules as `top_workflows[].size_bytes`,
including the root workflow and its children but excluding shared runner files.
The scan runs off the event loop. If the selected directory disappears before
scanning begins, the request returns `not_found`; files removed during scanning
are skipped as with workspace disk usage.

### Execution target override

An optional UUID `worktree_id` selects a managed workspace worktree instead of a
raw path. It is mutually exclusive with `base_project_path`; the persisted target
includes its nullable `worktree_id`. See [Git worktrees](../usage/worktrees.md) for
child-workflow inheritance, exec overrides, limits, and safe deletion.

`executions.start.base_project_path` is an optional absolute directory path.
Omitting it or sending null uses `environment.base_project_path` from workspace
configuration. The directory must already exist; starting an execution does not
create it. Empty paths, relative paths, and NULs are rejected.

```json
{"kind":"request","id":"start-1","method":"executions.start","params":{"reference":"review:main","input":{},"idempotency_key":"review-worktree-1","base_project_path":"/projects/app-worktree"}}
```

Before workflow activities begin, the runner resolves symlinks and persists the
selected target. Root and child activities use this root for file tools,
internal/external authorization scope, process working directories, and agent
context. The override does not change workspace configuration, workflow discovery,
or the runner data directory. Different root executions can use different targets.

`executions.resume` restores the persisted target and does not accept an override.
Changing workspace configuration does not redirect an existing execution. A missing
target fails execution rather than falling back to the configured default. Legacy
records without a target bind and persist the current configured default on first
resume. The target binding does not snapshot filesystem contents or recover processes.

The Python API accepts the same optional keyword:

```python
await runner.run("review:main", base_project_path="/projects/app-worktree")
```

### Configuration reads and overrides

Names are canonical dotted strings rooted at `config.toml`, such as
`components.llm.model`. Component-relative shorthand, such as `llm.model` or
`tools.approval.whitelist`, is normalized by the configuration owner in exactly
the same way as internal and workflow reads. Startup root names (`data_directory`,
`workflows_directory`, `environment`, `logging`, `retention`) remain root-relative.
Path arrays are not accepted. Segments must be nonempty; `~1` encodes a literal
dot and `~0` a literal tilde within a segment.
Omitted or empty names select the entire snapshot, including
startup defaults. Missing paths return `exists: false, value: null`; an explicit
null returns `exists: true`. Snapshots contain raw effective values, not resolved
consumer settings or built-in LLM profiles. Arrays are whole values, not indexed.

```json
{"kind":"request","id":"config-1","method":"configuration.set","params":{"name":"components.llm.model","value":"openai/o3","permanent":true}}
```

Only `components` and descendants are editable. Root writes/resets and startup
settings such as directories, logging, retention, and environment return
`read_only`. Malformed requests return `invalid_argument`; persistence failures
return `configuration_error`. Values must be finite JSON; component schema
validation is deferred to consumers, so invalid configuration remains inspectable
and editable. Suggestions return known Connect model/provider names and enum
values from known LLM/workflow schemas, filtered by prefix; unknown paths return
`values: []`. Suggestions never restrict accepted values.

Writes return null only after application. Subsequent fresh reads see the new
value without restarting or reinitializing resources. Cached consumer state,
in-flight operations, and recorded workflow/replay snapshots remain unchanged.
Requests have no revision or batch contract. Temporary writes replace permanent
overrides rather than masking them. Reset removes overlapping overrides and
restores startup TOML; an absent key leaves defaults to its consumer. Permanent
overrides survive restart only while their startup TOML baseline is unchanged.
Parent and child overrides replace each other using the same rules as internal
configuration writes. See [Component configuration](../config/components.md#override-component-settings)
for persistence, inheritance, and Python client examples.

### Concurrent execution lifecycle

Start creates a new independent execution even when others are active. Resume uses an existing execution's recorded inputs and selected history heads; it is not a fresh run. Resuming an already active or stopping execution returns `execution_conflict`. Repeating an accepted idempotency key still returns its original receipt. Stop affects only the selected execution and its children/resources; other roots continue. A stop during startup is recorded once execution history is initialized.

`RunnerClient.active()` returns an `ActivePage`; `RunnerClient.executions()` returns an `ExecutionPage`. Both accept `ExecutionList` pagination parameters. Active pages are live snapshots, not a frozen listing. Use `current(execution_id)` for live status and `execution(execution_id)` for persisted status. The no-argument `current()` remains supported for zero or one active execution, but never silently chooses among multiple executions. `runner.info.max_active_executions` is null: no fixed root-execution limit is imposed.

```python
import asyncio
from vectorops.runner.control.protocol import Start, Resume

async def manage(host):
    first, second = await asyncio.gather(
        host.start(Start(reference="agent:main", idempotency_key="agent-a")),
        host.start(Start(reference="agent:main", idempotency_key="agent-b")),
    )
    await host.stop(first.execution_id)
    while await host.current(first.execution_id) is not None:
        await asyncio.sleep(0.05)
    await host.resume(Resume(execution_id=first.execution_id, idempotency_key="resume-a"))
    return await host.current(second.execution_id)
```

Shutdown rejects new start/resume requests. Cancel mode stops all roots, but suspends each execution waiting for mailbox input instead of cancelling its durable wait. Explicit stops remain cancellations. Drain mode waits for every active execution and continues accepting input responses and connections; it can wait indefinitely for mailbox input, but never for login. Shutdown waits for owned cleanup before releasing workspace ownership.

Workspace `data/workspace.sqlite3` stores the last and active execution IDs,
configuration overrides, request receipts, and discovery state. Execution databases
remain authoritative for status and recovery heads. Startup reconciles workspace
state with retained histories and recovers executions still marked `running`.
Completed, failed, and cancelled histories do not auto-resume. Runtime process
sessions are not restored.

Independent roots can target the same source directory. This does not serialize their file edits or isolate their working trees; use separate Git worktrees when filesystem isolation is required.

## History and live reconciliation

Subscribe first and wait for acknowledgement, query active executions or a selected execution, then fetch the desired history windows. Subscriptions become active before their acknowledgement. The application merges events received before, during, and after backfill, keyed by execution ID.

Step events retain the existing kinds `step.created`, `step.updated`, `step.completed`, and `step.failed`. They contain `execution_id` and a full `step` replacement snapshot, not a patch. A step's first event can already be terminal. Key records by execution/step ID and accept only newer `step.revision` values. Revisions start at 1 and persist across resume; legacy records default to revision 1. History items include a stable storage `position`; workflow predecessor links describe workflow order.

The live feed is not a durable event journal. Reconnect, resubscribe, and refresh history after disconnection. Intermediate snapshots missed while disconnected are not replayed. Subscription sequence numbers order delivery within a connection, not stored history. Both server and client event queues are bounded to 256 packets. Overflow disconnects the consumer; the client reports `resync_required` on its own queue overflow. Execution never waits for a network consumer.

Protocol-backed executions persist streaming snapshots before emitting updates. Ordinary local runner execution retains write-behind caching.

### Workflow and execution updates

Connected subscribers receive full replacement snapshots without polling:

| Event kind | Payload fields | Meaning |
| --- | --- | --- |
| `workflow.updated` | `execution_id`, `workflow` | Full workflow state, including definition-derived `name`, generated `description`, presentation, and usage |
| `execution.updated` | `execution_id`, `summary` | Full execution summary, including name, description, status, active flag, pending-input count, and usage |

New decorated workflow instances default `name` from the workflow definition unless
an execution name is already supplied. The optional decorator `name` defaults to the
module-qualified function name. Display labels prefer `presentation.short_name`,
then `name`, then `definition_ref`; a presentation override does not replace the
stored name.

The optional `description` holds the short LLM-generated title derived from the
first answered user-message request, limited to 120 characters. Generation does
not change `name`. Execution summaries expose the root workflow's name and
description separately. For example, `name` can be `"Code review"` while
`description` is `"Investigate failing authentication tests"`.

Older records without `description` default to null. Saved names and presentation
are retained during replay; historical generated names are not reclassified or
replaced from current workflow definitions.

Persistence changes trigger publication after successful commit. This includes
generated titles even when no subsequent step runs, child workflow usage changes,
input changes, and lifecycle transitions. Rapid changes are coalesced and identical
snapshots are suppressed; intermediate states are not guaranteed. Existing step
and lifecycle events remain available and unchanged.

Apply these payloads directly, keyed by execution ID and workflow ID; an event is
not an instruction to refetch. Include these kinds when using filtered subscriptions.
Subscribe before fetching initial summaries and workflow state, buffer events during
the fetch, and apply buffered snapshots afterward so an older response cannot replace
a newer push. Repeat this synchronization after reconnect; updates are not a durable
event journal. Changes made by external database writers are not observed.

### History query

```json
{"execution_id":"exec-1","selection":"tail","limit":100,"include_children":true}
```

| Field | Default | Meaning |
| --- | --- | --- |
| `execution_id` | Required | Execution to query |
| `selection` | `tail` | `tail` selects latest records; `head` selects earliest |
| `limit` | 100 | Maximum records, range 1-500 |
| `include_children` | true | False restricts to the root unless `workflow_id` is specified |
| `workflow_id` | null | Exact workflow filter |
| `step_ids` | null | Up to 500 exact IDs; empty list selects nothing |
| `head_step_id` | null | Restrict to this step and its predecessor branch |
| `before_step_id` | null | Restrict to records inserted strictly before this step; excludes the anchor |
| `cursor` | null | Opaque continuation for the same query |

Responses contain `execution_id`, `items` (`position`, `step`), and nullable `next_cursor`. Items are always ascending by position. Tail continuation loads older records; head continuation loads newer records. Iterate until the cursor is null to download the complete selection.

Cursors freeze the insertion boundary of a selection, not mutable record values. New inserts do not shift pages; newer revisions may appear on subsequent reads. All filters except `limit` must remain unchanged when continuing. SQLite applies filtering and limits before records are decoded, so a last-100 query does not load all steps into Python. History responses also stop at the frame-size budget, so a page can contain fewer items than `limit` even when more history exists. Follow `next_cursor` until it is null; a short page does not mean the history is exhausted. Step contents are not truncated. A single step that cannot fit in a response still fails with `frame_too_large` and requires a future chunked-content extension.

For a frontend retaining a moving window, use its oldest retained step ID as an exclusive anchor:

```json
{"execution_id":"exec-1","selection":"tail","before_step_id":"12345","limit":200}
```

This returns up to 200 matching records immediately before step `12345`, oldest-to-newest, without including that step. Ordering uses insertion position, not the step ID's value. Unlike `head_step_id`, this does not restrict records to the anchor's predecessor branch. Other filters still apply; the anchor must exist in the execution but need not match those filters. An unknown anchor returns `not_found`.

To load another older page, either use `next_cursor` with the same anchor and filters, or start a new query without a cursor using the new oldest retained step as `before_step_id`. Do not reuse a cursor after changing the anchor. With `selection="head"`, the anchor still bounds the selection, but pages start with the earliest matching records rather than those nearest the anchor.

## Workflow-scoped tool approval

Tool confirmations use the existing mailbox methods over either transport:

| Method | Parameters | Result |
| --- | --- | --- |
| `mailbox.list` | `execution_id`, optional `workflow_id`, `channel`, `after`, `limit` | Pending `requests` and `next_after` |
| `mailbox.get` | `execution_id`, `request_id` | Durable request |
| `mailbox.respond` | `execution_id`, `request_id`, `response`, `idempotency_key` | Accepted request/answer receipt |

Each request identifies its `execution_id`, `workflow_id`, and `request_id`.
For new tool workflows, the request ID is the recorded tool step ID. The default
channel is `tool.approval`; tools can select custom channels. Payload fields
`instance`, `call`, `tool`, and `tool_version` describe the invocation. Filter
`mailbox.list` by workflow ID to present confirmations separately per workflow.
Use durable queries on reconnect; `mailbox.requested` and `mailbox.answered`
notifications are advisory.

Approve once with `response: {"approved": true}`. To approve the current call
and future calls to the same exact tool name in the requesting workflow:

```json
{
  "kind": "request",
  "id": "approve-1",
  "method": "mailbox.respond",
  "params": {
    "execution_id": "exec-1",
    "request_id": "tool-step-1",
    "response": {"approved": true, "scope": "workflow"},
    "idempotency_key": "approve-tool-step-1"
  }
}
```

The response schema has strict `approved: bool`, optional `reason: str`, and
`scope: "call" | "workflow"` defaulting to `call`. Workflow scope requires
`approved: true`. Reject a single call with `{"approved": false, "reason": "..."}`.
Malformed responses fail the tool without granting permission; mailbox acceptance
is a delivery receipt, not proof that a tool response passed validation or executed.

The runner derives the tool name and workflow from the stored request. A grant
covers all arguments for that exact name, not children, siblings, or other
executions. It never bypasses argument validation or mandatory policy checks.
Already-pending confirmations still require individual answers. Grants and answers
commit atomically, survive restart/resume, and persist for the workflow instance,
including continuations. They are not branch-local and cannot yet be revoked.
Idempotent retries return the same receipt; a conflicting answer cannot create a
grant. Ordinary mailbox inputs and legacy binding-local approval do not create
workflow grants. Ordinary tools keep their gate and implementation in one activity;
workflow-side tools record a gate activity before durable child operations.

See [Tool approval configuration](../config/tool-approval.md) for
workspace defaults, extension points, and history compatibility.

## Conversation inboxes and LLM steering

Conversation inboxes accept user messages without a pending input request. New
root `builtin:agent` executions pass an addressed `steer` inbox
to their user-facing LLM; children and helper LLMs do not inherit the root queue.
Existing executions without the recorded `inbox_version: 1` configuration skip
steering reads so historical activity positions remain replayable. Blocking user
input remains unchanged and uses `llm.request_user_message()`.

| Method | Parameters | Result |
| --- | --- | --- |
| `mailbox.inboxes.list` | `execution_id` | `mailboxes`, each with `address`, `status`, and `queued_messages` |
| `mailbox.inboxes.inspect` | Inbox address | Inbox status and queue count |
| `mailbox.inboxes.send` | Inbox address, `payload` | Enqueued message with `id`, `created_at`, and `payload` |
| `mailbox.inboxes.messages` | Inbox address, optional `after`, `limit` | FIFO `messages` and nullable `next_after` |
| `mailbox.inboxes.remove` | Inbox address, `message_id` | Removed message, or null if it is no longer queued |
| `mailbox.inboxes.pop` | Inbox address | Oldest queued message, or null if empty |

An inbox address contains required `execution_id`, `workflow_id`, and
`conversation_id` fields. Discover the address with `list_mailboxes`; sending never
creates an inbox. Unknown destinations, closed executions, and stopping
executions return `mailbox_conflict`. Branch transitions return
`execution_conflict`. Queue counts and listings are snapshots, not reservations.
Listing defaults to 100 messages with a maximum of 500. `after` is a queued
message ID; a removed or consumed cursor returns `mailbox_conflict`.

```python
from vectorops.runner.api.chat_input import UserMessageInput
from vectorops.runner.runner.inbox import Send, MessageRef

async def steer(host, execution_id):
    listing = await host.list_mailboxes(execution_id)
    address = listing.mailboxes[0].address
    message = await host.send_message(Send(
        **address.model_dump(),
        payload=UserMessageInput.model_validate({
            "content": [{"type": "text", "text": "Focus on the failing test first."}]
        }),
    ))
    return await host.remove_message(MessageRef(
        **address.model_dump(), message_id=message.id
    ))
```

Payloads use the existing bounded user-only chat contract: text and inline image
blocks, not assistant messages or tool approvals. Sending acknowledges enqueue,
not LLM injection. Sends have no idempotency receipts; retrying sends another
message. Queue state is in memory and is discarded on restart, stop, or branch
switch. Messages consumed through a completed activity are reconstructed through
normal history replay; replayed drains do not consume fresh messages, including
when their recorded result is empty.

The LLM drains its inbox before generation and between complete tool rounds. It
also checks before returning a final response when another round is available;
queued steering causes another generation without returning to the workflow.
Steering never cancels running tools or an in-flight model request and never
resets the round limit. Input remaining at the limit stays queued until a later
LLM invocation. `LLM.generate` drains only before its single generation.
Steering sent while the workflow waits for user input stays queued; it does not
answer or wake that input request. Use `mailbox.respond` for idle user input.

Host removal and LLM consumption use the same queue owner: whichever removes a
message first wins. Host removal cannot retract input already injected into model
context. `mailbox.enqueued` and `mailbox.removed` events carry `execution_id`,
`address`, `message_id`, `reason`, and the resulting `queued_messages` count.
Removal reasons are `llm_drain`, `workflow_receive`, and `host_remove`; enqueue
reason is null. These advisory queue events omit payloads; consumed messages still
appear in normal activity history and its step events.

Custom workflows pass an addressed inbox to the LLM without changing their input
loop:

```python
from vectorops.runner.api import inbox, llm

conversation = llm.LLM(
    "agent",
    inbox=inbox.Inbox("steer", workflow_id=ctx.workflow.id),
)
```

`workflow_id` defaults to the current workflow ID. Handles targeting the same
queue share it; construction is not exclusive consumer registration.
`messages.drain()` returns currently queued user payloads without waiting.
An absent queue or unavailable inbox registry yields no messages. Root queue
registration and routing stay inside the inbox owner; subagents cannot consume
the root queue. The LLM performs all reads and user-message injection internally.
Normal input requests and answers are independent of steering.

## Related

- [Provider authentication RPCs and global configuration](../config/authentication.md)
- [Structured diagnostic logging](../config/logging.md)
- [Daemon and SSH usage](../usage/daemon.md)