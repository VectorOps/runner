# Structured diagnostic logs

Persistent runners write diagnostic JSONL to `<data_directory>/logs/`, separately
from saga history. Use these logs to investigate errors and workflow lifecycle
events, not to reconstruct workflow results or provide an audit trail.

## Configure logging

Workspace `config.toml` overrides these defaults:

```toml
[logging]
level = "INFO"
max_bytes = 8388608
max_files = 5
queue_size = 1024
max_record_bytes = 16384
```

| Setting | Meaning |
| --- | --- |
| `level` | Minimum severity: `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL` |
| `max_bytes` | Maximum bytes per file, at least 1024 |
| `max_files` | Retained files, 1-1000; oldest modified files are removed first |
| `queue_size` | Pending records, 1-100000 |
| `max_record_bytes` | Maximum encoded record size, at least 512; also bounded by `max_bytes` |

Logs use unique filenames across rotation and restart. Configuration applies at
runner/service startup; it is not part of deterministic workflow replay. History
retention does not delete logs. Files are created with mode `0600` and the log
directory with mode `0700`; existing directory permissions are not changed.

## Emit structured events

```python
from vectorops.runner import diagnostics

diagnostics.log(
    "provider.retry",
    level=diagnostics.Level.WARNING,
    fields={"attempt": 2, "delay_seconds": 1.5},
)
```

Each record contains `version` (1), UTC `timestamp`, `level`, `event`, JSON `fields`,
and nullable `execution_id`, `workflow_id`, and `step_id`. Event names should be
stable identifiers, not formatted messages. Workflow and activity tasks inherit
correlation automatically, including concurrent child workflows. Custom code can
use `diagnostics.bind(...)` to establish a narrower scope. Ordinary and streaming
activity bodies receive step correlation. Calls outside an active logger are no-ops.

The control service records runner, execution, and mailbox lifecycle events.
Both local persistent runners and the control service record workflow lifecycle
and failure events. Step successes and progress are `DEBUG`; failures are `ERROR`.
Replay can generate additional diagnostic events; logs are not exactly-once.
An in-memory-history runner does not automatically create a disk logger.

## Protect sensitive data

Runner events omit workflow inputs, outputs, prompts, mailbox contents, and
authentication packets. Passing `error=exc` records the exception type and up to
32 traceback frame locations, without exception messages, source lines, or locals.
Nested fields whose names contain password, secret, token, authorization, cookie,
API key, credential, or private key markers are redacted before enqueueing.
Bearer tokens and simple password/token/key assignments in strings are also masked.

Redaction is defense in depth, not a secret detector: arbitrary credentials in
free text or innocently named fields cannot be recognized reliably. Never log
credentials or transcripts. This API does not intercept standard Python logging
or change the host application's logging handlers.

## Download logs from a host

Authenticated control clients can call `logs.list` and `logs.download` over either
transport. All authenticated controllers can read all retained workspace logs.

```python
import base64
from vectorops.runner.diagnostics import LogDownload

async def chunks(host):
    listing = await host.logs()
    for file in listing.files:
        offset = 0
        while offset < file.size:
            chunk = await host.download_logs(LogDownload(
                file=file.name, offset=offset, end=file.size,
            ))
            yield base64.b64decode(chunk.content_base64)
            offset = chunk.next_offset
```

`logs.list` takes no parameters and returns `files` (`name`, byte `size`), `dropped`,
and `write_errors`. Counters cover the current logger lifetime. Listing flushes
pending writes. Downloads accept only generated filenames, never arbitrary paths.

`logs.download` accepts `file`, byte `offset` (default 0), byte `limit` (default
65536, maximum 262144), and optional `end`. It returns `file`, `content_base64`,
`next_offset`, `end`, and `eof`. Pass the listing's size as `end` to freeze the
download boundary while new records append. Without `end`, the current size is used.
Concatenate decoded bytes before parsing JSONL; chunks can split records.

Downloads do not pin retention. If rotation deletes a selected file, the API
returns `not_found`; refresh the listing. Invalid ranges return `invalid_argument`.

## Delivery guarantees

Emission snapshots and redacts fields synchronously, then enqueues without waiting
for capacity. Disk writes, rotation, and downloads run off the event loop. Queue
overflow, oversized/invalid records, and disk write failures increase `dropped`;
disk failures also increase `write_errors`. Write failures do not fail workflows.
Directory initialization failures prevent startup rather than silently disabling logs.

Normal shutdown drains accepted records. Abrupt process termination can lose
queued records or leave a partial final line; writes are not fsynced. Ignore an
incomplete final JSONL line when analyzing crash logs. Retention is count-based,
not age-based, and there is no guaranteed retention window.

## Related

- [Control protocol](../reference/control-protocol.md)
- [History retention](retention.md)