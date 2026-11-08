# Local targets, files, and processes

An execution target is the project directory used by file tools, process working
directories, and agent context. Targets currently support local POSIX execution.
SSH and Docker target backends are not provided.

## Choose a target

By default, tools target the project passed to `--project`, while runner files live
under its `.voc/` directory. Changing the workspace directory does not change the
tool target. To select another directory, configure project `config.toml`:

```toml
[environment]
kind = "local"
base_project_path = "/home/user/projects/example"
max_read_bytes = 1048576
max_write_bytes = 16777216
max_list_entries = 1000
```

Use an absolute existing directory. The runner resolves symlinks and records the
target before execution begins. Resume reopens that target, not a newly configured
default. A missing or redirected target fails instead of silently falling back.
Limits must be positive integers; the example shows defaults.

Python callers can use `runner.run(reference, base_project_path="/absolute/path")`.
The control API accepts the same override on `executions.start`. A managed
`worktree_id` is an alternative, not an additional path override. Children inherit
their parent's target unless explicitly bound to another worktree. See
[worktrees](../usage/worktrees.md).

## File tools and patching

| Tool | Use |
| --- | --- |
| `list_files` | List one directory with bounded entries and an explicit truncation flag |
| `read_file` | Read bounded bytes or UTF-8 lines, with continuation offsets |
| `apply_patch` | Add, update, move, or delete files using a supported patch format |

Standard file tools accept target-relative, absolute, and home-relative paths,
including paths outside the target root. Authorization distinguishes internal and
external paths, but **the root is not a sandbox**. Avoid broad automatic approval
when tools must not access other files.

`read_file` defaults to a 64 KiB byte range. Line mode uses one-based `start_line`
and defaults to 100 lines. Complete supported images can return native image
content; partial images and other binary files use base64. PDFs are not extracted
as text. Follow `next_offset` and `eof`, and check truncation before treating a read
or listing as complete. Concurrent writes can change content between reads.

Patching supports `v4a` and `patch` (SEARCH/REPLACE, not unified diff). Parse and
matching failures leave the affected file unchanged; other files can succeed.
Filesystem failures, including edited moves, can leave partial effects. Inspect
the result and reread failed files before retrying. There is no multi-file rollback
or protection against external writers. Reverse application is not a general undo.

For trusted Python integrations, `PatchService(root: Path)` owns patch application.
It does not require a target-side patch executable. Do not infer permissions from
using the service directly; workflow tools add their own approval boundary.

## Configure processes

These project settings show the defaults:

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

Each `exec` starts a fresh process. Argv commands run without shell expansion.
Shell scripts use `/bin/sh -c` and require `allow_shell = true`. An argv command
can still invoke a shell explicitly, so this flag is not a security boundary.

Processes inherit the runner's environment by default. Use `inherit_parent=false`
or an `allowlist` to avoid passing sensitive variables. Inheritance is filtered
by `allowlist` and `denylist`, then configured `defaults` and request `env` values
are applied. Explicit values can reintroduce a filtered name.

The model-facing tool supplies plain-output defaults such as `TERM=dumb` and
`NO_COLOR=1`. Override them when an interactive application needs a terminal.

## Execute and collect output

Workflow authors normally use the process tool factories from
`vectorops.runner.api.tools`. Application integrations can open a local environment
and use its process manager directly:

```python
from pathlib import Path

from vectorops.runner.environment.configuration import LocalEnvironmentConfig
from vectorops.runner.environment.local import LocalEnvironment
from vectorops.runner.environment.processes import models


async def run_command(root: Path):
    environment = await LocalEnvironment.open(
        LocalEnvironmentConfig(base_project_path=str(root.resolve()))
    )
    try:
        result = await environment.processes.exec(
            models.ExecRequest(command=models.ArgvCommand(argv=["git", "status", "--short"]))
        )
        chunks = list(result.chunks)
        while result.retained:
            result = await environment.processes.poll(
                models.PollRequest(
                    process_id=result.process_id,
                    cursor=result.cursor,
                    wait_timeout_ms=1000,
                )
            )
            chunks.extend(result.chunks)
        return result, chunks
    finally:
        await environment.processes.close()
```

Check the return code, `error`, `dropped_bytes`, and `output_complete`; captured
output alone does not prove success. In JSON, byte chunks use base64. Session IDs
are opaque and belong to the current attachment, not an OS PID.

| Request option | Default | Meaning |
| --- | --- | --- |
| `cwd` | `.` | Working directory within the selected target |
| `yield_timeout_ms` | 60000 | Initial wait, 0 to 600000 ms; expiry returns a running session |
| `execution_timeout_ms` | Unset | Positive runtime limit, separate from the initial wait |
| `keep_stdin_open` | `false` | Allow later input |
| `kill_tree` | `true` | Manage the POSIX process group |
| `mode` | `pipe` | `pipe` or `pty` |
| `terminal_size` | 50 rows, 160 columns in PTY mode | Optional positive dimensions, at most 65535 |

`poll` accepts a byte cursor and an optional wait of up to 30 seconds. Output is
drained continuously into a bounded buffer; dropped bytes are reported. A quick,
fully collected exit is forgotten immediately. Other sessions remain available
until final polling, retention expiry, or manager cleanup.

`max_sessions` counts starting, running, and retained sessions across the root's
default target, child attachments, and exec overrides. Exiting does not necessarily
free capacity until output is consumed. `io_timeout_seconds` bounds I/O and cleanup,
not the command's total runtime.

## Interactive terminals

Use `mode="pty"` with `keep_stdin_open=True` for interactive programs. Output is
one terminal stream. `write_stdin` sends text or control sequences; its `eof` flag
is unsupported for PTYs. Send an appropriate terminal EOF sequence, such as Ctrl-D,
when the application expects it. Pipe mode supports the explicit EOF flag.

## Cancellation, cleanup, and recovery

Cancelling an `exec`, poll, or input request does not implicitly terminate its
process. Use `kill_process` or close the owning process manager. A cancelled input
request can still have delivered bytes.

With `kill_tree=True`, kill, runtime timeout, and cleanup signal the process group
and escalate to SIGKILL. Remaining group members are also cleaned up after normal
command exit. Detached descendants can escape; confirmation does not prove every
descendant is gone. `kill_tree=False` leaves descendants unmanaged.

The runner cleans up root-owned resources after stopping activities and children.
A child with its own target closes its attachment when it finishes. Registered
cleanup failures are reported rather than silently ignored. Hard termination
cannot run cleanup, and process handles are not restored after a crash.

Completed activities replay saved results. An interrupted file mutation or command
can already have taken effect before recovery repeats it. Inspect external state
when the outcome is uncertain. Replay does not restore historical project files.

## Related

- [Tool approval](../config/tool-approval.md)
- [Activity context and idempotency](activities.md)
- [Security boundaries](../../SECURITY.md)
