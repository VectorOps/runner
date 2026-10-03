# Runner control protocol

The host controls one workspace runner through authenticated JSON packets. A runner owns one top-level execution at a time; children run within that execution. Workflow code remains on the runner's filesystem.

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
            file="workflows/report.py",
            callable="main",
            input={"date": "2026-01-15"},
            idempotency_key="report-2026-01-15",
        ))
        while True:
            packet = await host.next_event()
            if packet.event["kind"] in (
                "execution.completed", "execution.failed", "execution.cancelled"
            ):
                break
        history = await host.history(HistoryQuery(
            execution_id=accepted.execution_id, limit=100
        ))
        return history
```

`embedded` owns the service lifetime and cancels remaining execution on exit. Closing an ordinary `RunnerClient` does not stop its runner. `RunnerService` and `RunnerServer` can be managed separately when an embedded service must outlive clients.

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

### Migration from the initial handshake

Version 2 replaces version 1's HMAC hello/challenge exchange. Old handshake packets are rejected; update clients and daemons together. Pass a text token to `RunnerClient.tcp(port, token)` or `RunnerClient.connect(stream, token)` instead of secret bytes, or omit the TCP token for local global-config lookup. Existing `~/.vectorops-runner/` files are ignored. New global secrets live in `~/.config/vops-runner`; endpoint discovery still uses daemon readiness output. Operational request/event shapes are unchanged.

## Packet families

```json
{"kind":"request","id":"req-1","method":"executions.current","params":{}}
```

```json
{"kind":"response","id":"req-1","result":null}
```

```json
{"kind":"error","id":"req-1","code":"runner_busy","message":"a top-level workflow is already active"}
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
| `runner.shutdown` | `mode`: `cancel` (default) or `drain` | Shutdown accepted |
| `executions.current` | None | Active execution ID/status, or null |
| `executions.list` | `after`, `limit` (default 100, maximum 500) | Lexically ordered execution IDs and `next_after` |
| `executions.get` | `execution_id` | Persisted execution record and checkpoint |
| `executions.start` | `file`, `callable`, `input`, `idempotency_key` | Execution ID and `starting` status |
| `executions.resume` | `execution_id`, `idempotency_key` | Execution ID and `starting` status |
| `executions.stop` | `execution_id` | Active execution with `stopping` status |
| `history.query` | History query below | Ordered history items and `next_cursor` |
| `events.subscribe` | Optional `execution_id`, optional `kinds` list | `subscription_id` |
| `events.unsubscribe` | `subscription_id` | Null |
| `logs.list` | None | Retained log filenames/sizes and dropped/write-error counters |
| `logs.download` | `file`, `offset`, `limit`, optional `end` | Base64 bytes, `next_offset`, `end`, `eof` |

There is no workflow catalog. `file` must identify a `.py` file within the workspace after symlink resolution. The callable must be an async workflow entry point. Import/callable/execution failures are reported through execution lifecycle events after acceptance. Existing runner references remain supported internally; the protocol does not accept Python objects or module imports in place of filenames.

Start and resume receipts are persisted before launch. Repeating the same idempotency key and arguments returns the original acceptance, even after restart; different arguments produce `idempotency_conflict`. Acceptance is not proof of successful execution. A crash between receipt persistence and launch can leave an accepted ID without execution history; retries do not automatically execute it. Retention can also remove history without removing its receipt. Use a new key only for an intentional new attempt.

Stop is cooperative and addresses the specific active execution. It returns before cleanup; wait for `execution.cancelled`, `execution.completed`, or `execution.failed`. Workflows that block the event loop or suppress cancellation can delay shutdown. Forced termination is not supported.

## History and live reconciliation

Subscribe first and wait for acknowledgement, query the current execution, then fetch the desired history window. Subscriptions become active before their acknowledgement. The application merges events received before, during, and after backfill.

Step events retain the existing kinds `step.created`, `step.updated`, `step.completed`, and `step.failed`. They contain `execution_id` and a full `step` replacement snapshot, not a patch. A step's first event can already be terminal. Key records by execution/step ID and accept only newer `step.revision` values. Revisions start at 1 and persist across resume; legacy records default to revision 1. History items include a stable storage `position`; workflow predecessor links describe workflow order.

The live feed is not a durable event journal. Reconnect, resubscribe, and refresh history after disconnection. Intermediate snapshots missed while disconnected are not replayed. Subscription sequence numbers order delivery within a connection, not stored history. Both server and client event queues are bounded to 256 packets. Overflow disconnects the consumer; the client reports `resync_required` on its own queue overflow. Execution never waits for a network consumer.

Protocol-backed executions persist streaming snapshots before emitting updates. Ordinary local runner execution retains write-behind caching.

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
| `cursor` | null | Opaque continuation for the same query |

Responses contain `execution_id`, `items` (`position`, `step`), and nullable `next_cursor`. Items are always ascending by position. Tail continuation loads older records; head continuation loads newer records. Iterate until the cursor is null to download the complete selection.

Cursors freeze the insertion boundary of a selection, not mutable record values. New inserts do not shift pages; newer revisions may appear on subsequent reads. All filters except `limit` must remain unchanged when continuing. SQLite applies filtering and limits before records are decoded, so a last-100 query does not load all steps into Python. A response exceeding the frame limit fails with `frame_too_large`; reduce the page size. A single oversized step requires a future chunked-content extension.

## Related

- [Provider authentication RPCs and global configuration](../config/authentication.md)
- [Structured diagnostic logging](../config/logging.md)
- [Daemon and SSH usage](../usage/daemon.md)