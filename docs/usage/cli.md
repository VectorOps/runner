# Workflow CLI

Use `vops-runner` to start workflows, resume interrupted executions, inspect persisted progress, and export history for external analysis.

## Quick start

Create `workflows/example.py` in your workspace:

```python
async def workflow(ctx, value: int = 0) -> int:
    return value + 1
```

Run it with JSON keyword arguments:

```bash
uv run vops-runner start example:workflow --input '{"value": 4}' --execution-id example-run
uv run vops-runner status
uv run vops-runner export --output example-run.jsonl
```

Start prints a JSON object containing `execution_id` and `result`. It runs in the foreground. Errors go to stderr with exit code 1, argument errors use exit code 2, and Ctrl-C uses exit code 130.

## Start and resume

```bash
uv run vops-runner --project /path/to/workspace start example:workflow
uv run vops-runner --project /path/to/workspace resume example-run
```

`--project` precedes the command and defaults to the current directory. Workspace paths follow `config.toml`, including workflow and data directory overrides. `start` accepts `builtin:name` for a registered built-in, an explicit `path:callable` reference relative to the workflow directory without `.py`, or `python:package.module:callable` for a library workflow. Local and library references require a callable; built-ins identify their entry point by name. There is no `--callable` option. Input defaults to `{}`, and execution IDs are generated unless `--execution-id` is supplied. Existing execution IDs are never overwritten.

`resume [EXECUTION_ID]` uses persisted configuration, input, and selected root and child heads. Omitting the ID selects the last execution. Completed operations replay their stored outcomes; interrupted activities restart with their existing step identity. Activity implementations remain responsible for external side-effect idempotency. Resuming a completed execution replays its result. Stop the original writer before resuming; the project permits only one writer.

This foreground CLI owns the workspace writer lock. For concurrent independent executions in one workspace, run a [daemon](daemon.md) and use its [execution-oriented control API](../reference/control-protocol.md#concurrent-execution-lifecycle) instead of launching competing foreground writers.

## Built-in chat

`builtin:chat` needs no workspace Python file. It requests a user message, runs
the LLM, and repeats with the same conversation history until stopped. It exposes
no tools or subagents. Text and images use the existing typed chat input contract.

Configure models in `<workspace>/config.toml`, using model IDs supported by your
provider and [configured credentials](daemon.md):

```toml
[components.workflows.defaults]
strong_model = "openai/gpt-5"
medium_model = "openai/gpt-5-mini"
weak_model = "openai/gpt-5-nano"
thinking_level = "high"
```

These fields are optional. An unset tier inherits `components.llm.model`; an unset
thinking level inherits `components.llm.reasoning`. At least the selected tier's
model or the fallback model must be configured before generation. Thinking levels
are `none`, `minimal`, `low`, `medium`, `high`, and `xhigh`; provider/model support
still applies. Shared workflow defaults are snapshotted at invocation and replayed
on resume. Other LLM options retain their existing live configuration behavior.

Start a daemon in one terminal:

```bash
uv run vops-runner --project /path/to/workspace daemon serve --port 8765
```

From another terminal, discover and start chat:

```bash
uv run vops-runner client --port 8765 workflow show builtin:chat
uv run vops-runner client --port 8765 execution start builtin:chat
```

The workflow argument `model_tier` defaults to `strong`; pass
`--input '{"model_tier":"weak"}'` to select `weak` (or `medium`). Use the returned
execution ID to list the pending request, then answer its request ID:

```bash
uv run vops-runner client --port 8765 input list EXECUTION_ID
uv run vops-runner client --port 8765 input send EXECUTION_ID REQUEST_ID --value '{"content":[{"type":"text","text":"Hello"}]}'
```

Use `execution watch EXECUTION_ID` in a separate client before sending input to
observe live response events. Each turn creates a new request ID. End the loop
with `execution stop EXECUTION_ID`. The foreground command
`uv run vops-runner --project /path/to/workspace start builtin:chat` also resolves
the workflow, but it does not provide a stdin chat interface; use the daemon/client
flow for interactive conversations.

Built-in references remain `builtin:name` in descriptors and execution history.
Only explicitly registered names are accepted; unknown names do not fall back to
files or arbitrary Python imports. `builtin:chat` is the first registered workflow.

## Connect to an existing runner

Start a daemon in one terminal, then use its reported port from another:

```bash
uv run vops-runner --project /path/to/workspace daemon serve --port 8765
uv run vops-runner client --port 8765 workflow list
uv run vops-runner client --port 8765 workflow show mass_validator:main
uv run vops-runner client --port 8765 execution start mass_validator:main --input-file args.json
uv run vops-runner client --port 8765 execution list --active
```

Client commands never acquire the workspace writer lock or start an embedded
runner. They reuse the existing authenticated control client. `--project` does not
select the remote workspace; the connected daemon owns it. Use `client info` to
verify the workspace and instance. Place connection options before the command:
`--host` defaults to `127.0.0.1`, `--port` is required, and `--token-file` or
`--token-stdin` can replace the configured token. `--config-dir` remains a global
option. Remote runners require an SSH-forwarded loopback port, not direct
unencrypted network access.

| Command after `client --port PORT` | Behavior |
| --- | --- |
| `info` | Show runner identity and workspace |
| `workflow list` | Discover built-ins and local decorated definitions with active execution, pending-input, and authentication-wait counts |
| `workflow show REFERENCE` | Show descriptor and input schema; explicit undecorated/library references are supported |
| `execution start REFERENCE --input JSON` | Start a background execution; `--input-file FILE` is an alternative |
| `execution list [--active]` | Show root reference, lifecycle status, live ownership, and pending waits |
| `execution show EXECUTION_ID` | Show one execution summary |
| `execution resume EXECUTION_ID` | Resume persisted configuration and history |
| `execution stop EXECUTION_ID` | Request cancellation, including children |
| `execution watch EXECUTION_ID` | Print current summary and live JSONL events until terminal notification or Ctrl-C |
| `input list EXECUTION_ID` | List pending requests, including child workflows; filter with `--workflow-id` or `--channel` |
| `input show EXECUTION_ID REQUEST_ID` | Show full request payload and expected response type |
| `input send EXECUTION_ID REQUEST_ID ...` | Answer an existing pending request |
| `shell` | Run repeated commands on one connection |

Lists accept `--limit` (default 100, maximum 500) and `--after`; output identifies
the next cursor when more results exist. Add `--json` before the command family
or after a leaf command for machine-readable output. Ordinary output is compact;
`show` provides full details. `workflow list` imports trusted workspace Python and
reports per-file or per-callable discovery errors without hiding valid definitions.
It rescans filenames and reuses unchanged source modules. Imported workflow aliases
and undecorated helpers are not enumerated. Definitions and executions are distinct:
starting the same reference twice creates two independent executions.

Pending counts include child mailbox requests and tool confirmations. They do not
replace lifecycle status: an execution with a waiting child can also be doing work.
Authentication waits are reported separately. Catalog counts aggregate active
executions by their root reference, not child-definition reference. Summaries are
live views, not a globally atomic snapshot across all execution databases.

### Send input

```bash
uv run vops-runner client --port 8765 input list EXECUTION_ID
uv run vops-runner client --port 8765 input send EXECUTION_ID REQUEST_ID --text-file ghidra.txt
uv run vops-runner client --port 8765 input send EXECUTION_ID REQUEST_ID --value '{"approved":true}'
```

Choose exactly one of `--text`, `--text-file`, `--value` (JSON), or `--json-file`.
Files are local to the client, read as UTF-8, and bounded by the control frame size;
JSON escaping and packet overhead can further reduce the effective payload limit.
Text files preserve newlines. JSON responses may be objects, arrays, or scalars.
The request's declared response type is checked before sending. Input is an answer
to a specific pending request, not an unsolicited message queue. Acceptance confirms
delivery, not successful workflow completion.

Start, resume, and input send accept `--idempotency-key`. A generated key is printed
to stderr before mutation. If a request times out or disconnects, its outcome may
be unknown: retry with that same key and arguments, not a newly generated key.
The client never automatically resends mutations.

### Connected shell

```bash
uv run vops-runner client --port 8765 shell
```

```text
/workflow list
/execution start mass_validator:main --input-file args.json
/execution list
/input list EXECUTION_ID
/input send EXECUTION_ID REQUEST_ID --text "Continue"
/execution watch EXECUTION_ID
/help
/exit
```

Shell commands share the one-shot parser and support shell-like quoting; the leading
slash is optional. A bare family such as `/input` shows its help, and leaf commands
accept `--help`. Invalid commands report errors without ending the session.
There is no implicit selected execution. No live events are subscribed to until an
explicit `execution watch`. Watching occupies the shell until completion; use another
client to answer inputs meanwhile. EOF, `/exit`, Ctrl-C, or disconnect closes only
the client, never the daemon or its executions. Reconnect explicitly after a lost
connection and refresh lists/history. Shell authentication uses configured credentials
or `--token-file`; `--token-stdin` is rejected because stdin carries commands.

## Inspect status and statistics

```bash
uv run vops-runner status example-run
```

`status [EXECUTION_ID]` emits JSON with `execution` metadata and `stats`: workflow count, step count, and counts by operation and outcome kind. Counts include child workflows and all retained branches, not only selected paths. Without an ID it selects the last execution.

Status and export read a consistent committed SQLite snapshot without taking the writer lock, so they work from another terminal while a workflow runs. Buffered streaming snapshots appear only after persistence. Lifecycle status is persisted: `running` can also indicate an execution interrupted by a crash, not proof of a live process. Checkpoint timestamps reflect persisted checkpoints, not the inspection time. Activity timing, token usage, and costs are not recorded or inferred.

## Export JSONL

```bash
uv run vops-runner export example-run --output analysis.jsonl
```

`export [EXECUTION_ID] --output FILE` requires a new output file and an existing parent directory. It refuses to overwrite any existing file. Without an ID it selects the last execution.

The UTF-8 file contains:

- One header entry with `kind: "execution.header"`, `version: 1`, `execution` metadata, and a `workflows` array containing every stored workflow configuration.
- One normalized workflow step per following line, preserving its `id`, `workflow_id`, `previous_step_id`, `operation`, `outcome`, and `deferred_delivery` fields.

Steps follow insertion order across the entire execution, including children and retained branches. Replacement snapshots export only the latest committed value for each step. Use predecessor links to reconstruct branches and `execution.checkpoint.heads` to identify selected paths. The entire export uses one committed snapshot; it is an analysis format, not an import or restore interface. Inputs, results, and error messages can contain sensitive data; protect the exported file accordingly.

## Related

- [Workflow authoring and replay](../development/workflows.md)