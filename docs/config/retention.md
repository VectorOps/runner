# Execution history retention

Retention limits remove old completed execution directories after a persistent workflow finishes successfully.

## Configuration

Set either or both limits in the project's `config.toml`:

```toml
[retention]
max_age_days = 30
max_completed_executions = 100
```

| Setting | Default | Behavior |
|---|---|---|
| `max_age_days` | Disabled | Delete completed histories older than this many days. |
| `max_completed_executions` | Disabled | Retain the newest completed histories up to this count. |

Both settings accept nonnegative integers; omit a setting to disable it.
Zero makes every unprotected completed history eligible under that limit.
Age and ordering use the execution's completion-state `updated_at` timestamp, not filesystem modification time.
A history is eligible when either configured limit is exceeded.
Histories exactly at the age boundary are retained.

## Protected histories

The execution referenced by `runner-state.json.last_execution_id` is always retained, even if a limit would remove it.
It counts toward the completed-history limit when it is completed.
Running, failed, and cancelled executions are not subject to automatic retention.
Active and reserved execution IDs are protected through startup, resume, and final cleanup, even if their persisted record is already completed. Retention and resume reservations are coordinated within the daemon so cleanup cannot remove a history being resumed. Temporary protection can exceed the configured limit until a later retention pass.
Unreadable histories, incomplete directories, and symlinked execution paths are preserved.

Cleanup runs under exclusive project writer ownership with execution-reservation coordination and removes an execution's entire directory, including child history and SQLite sidecar files.
Cleanup errors are logged without changing a successfully completed workflow's result.
Injected history providers are not scanned or cleaned automatically.