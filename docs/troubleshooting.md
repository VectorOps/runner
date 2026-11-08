# Troubleshooting

Start with the command's `--help`, the execution status, and
[diagnostic logs](config/logging.md). Do not publish raw history or logs without
checking them for sensitive data.

## Installation fails

Use Python 3.13 and a current `uv`, then run `uv sync --locked` from the checkout.
Git dependencies require access to `github.com/vectorops/connect` and
`github.com/vectorops/pypika`. Native Windows is unsupported; local execution and
credential locking require POSIX.

If an older checkout still expects `../connect`, update its dependency metadata
and lockfile together. Do not work around it by installing an unrelated package
with the same distribution name.

## A workflow cannot be found

References use `path:callable`, such as `greeting:main`. Put the file at
`<project>/.voc/workflows/greeting.py` unless you selected another workspace layout.
Do not include `.py`, an absolute path, or the old `workspace:` prefix.

Check the project passed to `--project`. Python library references use
`python:package.module:callable`. Import errors in a workflow can prevent discovery.
See [workflow references](development/workflows.md#references-and-discovery).

## The workspace is busy

Only one local writer can own a workspace. Stop the existing foreground runner or
connect to its daemon rather than launching another writer. A daemon supports
multiple executions. `status` and `export` are read-only and can inspect active work.

## An agent appears to be waiting

Inspect pending input on the daemon:

```bash
uv run vops-runner client --port 8765 input list EXECUTION_ID
uv run vops-runner client --port 8765 input show EXECUTION_ID REQUEST_ID
```

The request might be a user message or tool approval. Respond with the expected
schema, not a guessed text format. Use a second client while `execution watch`
occupies the first. A foreground `start builtin:agent` has no stdin chat interface.
See [agent usage](usage/cli.md#coding-agent-and-helper).

## Authentication fails

Configure credentials before starting work. Model names must include the provider,
for example `openai/gpt-4.1`. The default adapter uses the runner's credential store,
not provider environment variables. Check the selected global config directory and
active or pinned credential profile.

Missing credentials do not create an interactive workflow wait. Import a key or
complete explicit login, then start a new execution. A completed failed activity
replays its saved failure. See [authentication](config/authentication.md).

## Configuration changes have no effect

TOML files are read at startup. Restart to apply file edits, or use runtime component
overrides. Selected LLM profiles can override shared model settings. Existing
resources and recorded configuration reads keep their previous values; only fresh
reads use updates. See [configuration precedence](config/components.md).

## A request timed out or disconnected

Do not assume a mutation failed. Start, resume, and input-send commands expose
idempotency keys; retry with the same key and arguments. Query current state before
retrying other mutations. A tool timeout can leave an external effect without a
saved result. Inspect that external state first.

## Resume reports a mismatch

Restore compatible workflow code and operation order, or start a new execution.
Changing recorded inputs, operation identities, explicit model options, or schemas
can break replay. Editing a database is not a supported migration path. See
[saved-execution compatibility](reference/compatibility.md).

## Process output is incomplete

Poll retained sessions with the returned cursor until completion. Check
`dropped_bytes`, `output_complete`, and `error`; output is bounded and can be evicted.
A stale process ID after restart cannot restore a process. Inspect orphaned processes
before repeating commands with side effects. See [process management](development/target-environments.md).

## Recover or back up data

Stop the runner and copy its complete data directory, including SQLite sidecars.
JSONL exports are for analysis, not restore. Back up global credentials separately
with their encryption key and protect that backup as secret material. Never delete
or regenerate an encryption key to repair unreadable credentials.

For server-specific failures, see [MCP troubleshooting](config/mcp.md#limits-and-troubleshooting).
