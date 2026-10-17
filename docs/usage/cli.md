# Workflow CLI

Use `vops-runner` to run workflows, connect to a running daemon, answer requests,
and inspect or export executions. `uv run python -m vectorops.runner` accepts the
same commands.

## Quick start

Create `.voc/workflows/example.py` in an existing project directory:

```python
async def workflow(ctx, value: int = 0) -> int:
    return value + 1
```

From that project directory, run:

```bash
uv run vops-runner start example:workflow --input '{"value": 4}' --execution-id example-run
uv run vops-runner status example-run
uv run vops-runner export example-run --output example-run.jsonl
```

`start` runs in the foreground and prints the execution ID and result:

```json
{"execution_id": "example-run", "result": 5}
```

Choose the mode that fits your task:

| Task | Command |
| --- | --- |
| Run a workflow in the foreground | `start REFERENCE` |
| Continue a saved execution | `resume [EXECUTION_ID]` |
| Inspect local saved progress | `status [EXECUTION_ID]` |
| Export local history | `export [EXECUTION_ID] --output FILE` |
| Run interactive or concurrent executions | `daemon serve`, then `client --port PORT ...` |

Use `--help` on any command to see its arguments.

## Workspace location

By default, commands use the current project directory and keep runner files in
its `.voc/` subdirectory:

| Path | Purpose |
| --- | --- |
| `.voc/config.toml` | Optional project configuration |
| `.voc/workflows/` | Local Python workflow files |
| `.voc/data/` | Saved executions and logs |

Place global options before the command:

```bash
uv run vops-runner --project /path/to/project status
uv run vops-runner --project /path/to/project --workspace-directory .runner status
uv run vops-runner --project /path/to/existing-workspace --workspace-directory '' status
```

| Option | Default | Meaning |
| --- | --- | --- |
| `--project PATH` | Current directory | Existing project directory |
| `--workspace-directory PATH` | `VOPS_RUNNER_WORKSPACE_DIRECTORY`, then `.voc` | Relative runner directory; an empty value uses the project directory directly |
| `--config-dir PATH` | Runner-global configuration location | Select global configuration and credentials; see [daemon setup](daemon.md) |

Startup creates missing runner directories, but does not generate `config.toml`
or move configuration from another location. Workflow and data directory overrides
are described in [component configuration](../config/components.md).

Tools use the project directory as their local target unless you configure a
different environment. Changing `--workspace-directory` does not change that target.
Commit non-secret configuration and workflow source; exclude the data directory
from version control. For a complete backup, stop the runner and copy its data
directory as a unit. Exports are for analysis, not backup restoration.

## Start and resume

```bash
uv run vops-runner start example:workflow --input '{"value": 4}'
uv run vops-runner resume example-run
```

Supported workflow references are:

| Form | Meaning |
| --- | --- |
| `example:workflow` | Callable `workflow` in `workflows/example.py` |
| `builtin:agent` | Registered built-in workflow |
| `python:package.module:workflow` | Callable in an importable Python module |

Local paths are relative to the workspace's workflow directory, without `.py`.
Local and library references must include the callable name.

`--input` accepts a JSON object and defaults to `{}`. Execution IDs are generated
unless `--execution-id` is supplied. Existing IDs are never overwritten. Starting
the same workflow twice creates two independent executions.

`resume` uses the saved configuration and input. Omitting the execution ID selects
the last execution. Completed operations reuse their saved results; interrupted
activities can run again, so check the workflow's guidance before retrying operations
with external side effects. Resuming a completed execution returns its saved result.

Only one foreground writer can own a workspace. Stop the original runner before
resuming locally. To run several executions concurrently, start a daemon and use
the client commands below.

## Connect to an existing runner

Start a daemon in one terminal:

```bash
uv run vops-runner --project /path/to/project daemon serve --port 8765
```

From another terminal:

```bash
uv run vops-runner client --port 8765 info
uv run vops-runner client --port 8765 workflow list
uv run vops-runner client --port 8765 workflow show example:workflow
uv run vops-runner client --port 8765 execution start example:workflow --input-file args.json
uv run vops-runner client --port 8765 execution list --active
```

The client connects to an existing daemon; it does not start one. `--project`
does not select the remote workspace. Use `client info` to confirm the connection.

Connection options go before the command family. `--port` is required;
`--host` defaults to `127.0.0.1`. Use configured authentication, `--token-file FILE`,
or `--token-stdin`. For credentials and SSH forwarding, see the
[daemon guide](daemon.md).

| Command after `client --port PORT` | Purpose |
| --- | --- |
| `info` | Show the connected runner and workspace |
| `workflow list` | List available workflow references |
| `workflow show REFERENCE` | Show a workflow's description and input schema |
| `execution start REFERENCE` | Start an execution; accepts `--input JSON` or `--input-file FILE` |
| `execution list [--active]` | List executions and pending waits |
| `execution show EXECUTION_ID` | Show an execution summary |
| `execution resume EXECUTION_ID` | Resume a saved execution |
| `execution stop EXECUTION_ID` | Request cancellation, including child workflows |
| `execution watch EXECUTION_ID` | Print the summary and live events until execution ends or you interrupt the client |
| `input list EXECUTION_ID` | List requests awaiting a response |
| `input show EXECUTION_ID REQUEST_ID` | Inspect a request and its expected response |
| `input send EXECUTION_ID REQUEST_ID ...` | Answer a pending request |
| `auth login PROVIDER` | Authenticate with a provider before running workflows |
| `auth profile list` | Show authentication profiles; `add`, `switch`, and `delete` take a profile name |
| `shell` | Issue repeated commands over one connection |

List commands accept `--limit` (default 100, maximum 500) and `--after CURSOR`.
Output supplies a cursor when more results are available. Add `--json` before the
command family or after a leaf command for machine-readable output:

```bash
uv run vops-runner client --port 8765 --json execution list --limit 20
```

## Send input

Inspect the pending request before choosing its response format:

```bash
uv run vops-runner client --port 8765 input list EXECUTION_ID
uv run vops-runner client --port 8765 input show EXECUTION_ID REQUEST_ID
uv run vops-runner client --port 8765 input send EXECUTION_ID REQUEST_ID --text "Continue"
```

Choose exactly one input source:

| Option | Value |
| --- | --- |
| `--text TEXT` | Plain text |
| `--text-file FILE` | UTF-8 text from a local file, preserving newlines |
| `--value JSON` | A JSON response matching the request |
| `--json-file FILE` | A JSON response from a local file |

For example, approve a tool request with:

```bash
uv run vops-runner client --port 8765 input send EXECUTION_ID REQUEST_ID --value '{"approved":true}'
```

Requests can come from child workflows as well as the main workflow. Filter the
list with `--workflow-id` or `--channel`. Sending input answers a specific request;
it does not create an unsolicited message. Acceptance does not mean the workflow
has completed successfully.

### Retry after a disconnect

Start, resume, and input-send commands accept `--idempotency-key`. When omitted,
the client generates a key and prints it to stderr before sending the request.

If a timeout or disconnect leaves the outcome uncertain, retry with **the same
key and arguments**. A new key can submit a second operation. The client does not
automatically resend these commands.

## Coding agent and helper

Use `builtin:agent` for an interactive coding conversation. Configure the models
and credentials before sending its first message. For example, in
`.voc/config.toml`:

```toml
[components.llm]
model = "openai/gpt-5-nano"

[components.llm.profiles.strong]
model = "openai/gpt-5"
```

Choose models supported by your provider. See [named LLM profiles](../config/components.md#named-llm-profiles)
for settings and [daemon setup](daemon.md) for credentials.

Against a running daemon:

```bash
uv run vops-runner client --port 8765 execution start builtin:agent
uv run vops-runner client --port 8765 input list EXECUTION_ID
uv run vops-runner client --port 8765 input send EXECUTION_ID REQUEST_ID --value '{"content":[{"type":"text","text":"Investigate the failing tests."}]}'
```

Use the execution ID returned by `start` and the request ID returned by `input list`.
Run `execution watch EXECUTION_ID` in another terminal before sending the message
to observe the response. Each turn creates a new request ID. End the conversation
with `execution stop EXECUTION_ID`.

The foreground `start builtin:agent` command does **not** provide a stdin chat
interface. Use the daemon/client flow for conversations and tool approvals.

`builtin:helper` accepts a task and returns a report without a conversational input
loop:

```bash
uv run vops-runner client --port 8765 execution start builtin:helper --input '{"task":"Locate the parser tests and report relevant paths. Do not change files."}'
```

The helper can still request tool approval or authentication. Its read-only role
is an instruction, not a sandbox: review execution commands before approving them.
For capability configuration, see [named tool collections](../config/tool-collections.md).

## Connected shell

```bash
uv run vops-runner client --port 8765 shell
```

At the prompt, enter the same commands used after `client --port PORT`, for example
`workflow list`, `execution list`, or `input list EXECUTION_ID`. Use `help` for
commands and `exit` to disconnect. Quoting works as in the one-shot examples.

There is no implicit selected execution. `execution watch` occupies the shell;
use another client to answer input while watching. Exiting or disconnecting the
shell does not stop the daemon or its executions. After a lost connection,
reconnect and check the current state before retrying a mutation.

Use configured authentication or `--token-file` for the shell. `--token-stdin`
is unavailable because stdin is used for commands.

## Inspect status and statistics

```bash
uv run vops-runner status example-run
```

`status [EXECUTION_ID]` prints JSON containing saved execution metadata and counts
of workflows and steps, including counts by operation and outcome. Counts include
children and all retained branches. Without an ID, it selects the last execution.

You can run `status` while a workflow is active. It reports saved progress, so the
latest streaming output might not yet appear. A saved `running` status can remain
after a crash; use daemon client commands to check whether an execution is live.

## Export JSONL

```bash
uv run vops-runner export example-run --output analysis.jsonl
```

The output file must not already exist, and its parent directory must exist.
Without an execution ID, the command exports the last execution.

The UTF-8 export contains an execution header followed by saved workflow steps,
including children and retained branches. You can export while an execution is
running. Export is an analysis format, not an import or restore interface.
Inputs, results, and errors can contain sensitive data; protect the file accordingly.

## Exit codes

| Code | Meaning |
| --- | --- |
| `0` | Command completed successfully |
| `1` | Runtime or client-command error; details go to stderr |
| `2` | Top-level argument parsing error |
| `130` | Interrupted with Ctrl-C |

## Related

- [Run and connect to a daemon](daemon.md)
- [Component configuration](../config/components.md)
- [Workflow authoring and replay](../development/workflows.md)
- [Control protocol for integrations](../reference/control-protocol.md)
