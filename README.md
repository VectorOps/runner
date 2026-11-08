# VectorOps Runner

Run durable Python workflows and LLM agents from your terminal or application.
VectorOps Runner records execution progress so you can resume interrupted work,
reuse completed results, and inspect what happened.

Write workflows as async Python functions. Compose them with recorded activities,
child workflows, model calls, and tools. Run a single workflow from the command
line, or use a daemon to manage concurrent executions and interactive agents.

**Status:** active development. APIs and stored execution formats can change.
Review compatibility notes before upgrading a workspace with saved executions.

## Install

You need Git, [uv](https://docs.astral.sh/uv/getting-started/installation/), and a
POSIX system such as macOS or Linux. The project requires Python 3.13; `uv` can
install it for you. Native Windows execution is not supported.

```bash
git clone https://github.com/vectorops/runner.git
cd runner
uv sync --locked
uv run vops-runner --help
```

Dependencies include [VectorOps Connect](https://github.com/vectorops/connect)
and the [VectorOps PyPika fork](https://github.com/vectorops/pypika), fetched from
GitHub. No sibling repository checkouts are required.

The commands below run from this checkout. Use `--project /path/to/project`
before a command to work in another existing project directory.

## Run your first workflow

No model account or API key is needed for this example.

Create the workflow directory:

```bash
mkdir -p .voc/workflows
```

Save this as `.voc/workflows/greeting.py`:

```python
async def main(ctx, name: str = "world") -> str:
    return f"Hello, {name}!"
```

Run it:

```bash
uv run vops-runner start greeting:main --input '{"name":"Ada"}' --execution-id hello
```

The command prints:

```json
{"execution_id": "hello", "result": "Hello, Ada!"}
```

Inspect the saved execution or retrieve its completed result:

```bash
uv run vops-runner status hello
uv run vops-runner resume hello
```

Execution IDs are unique. Use a new ID, or omit `--execution-id`, to start another
execution. `resume` reuses saved inputs and completed operations. An interrupted
activity can run again, so external side effects need idempotency or reconciliation.

Workflow source lives in `.voc/workflows/`; history and logs live in `.voc/data/`.
Keep `.voc/data/` out of version control. See the [CLI guide](docs/usage/cli.md)
for workspace options, exports, and recovery.

## Use LLM agents

The built-in coding agent can inspect files, run commands, make edits, and delegate
bounded tasks to a helper. It uses a daemon and client for conversation and tool
approvals, not a foreground stdin chat loop.

1. [Configure a model](docs/config/llm.md) and
   [import credentials or sign in](docs/config/authentication.md).
2. Follow the [coding-agent example](docs/usage/cli.md#coding-agent-and-helper).
3. Add [MCP servers](docs/config/mcp.md), [tool collections](docs/config/tool-collections.md),
   or [project skills](docs/config/skills.md) when you need them.

**Review tool requests before approving them.** Local tools run with your account's
permissions. A project directory or Git worktree is not a sandbox. See the
[security boundaries](SECURITY.md).

## Documentation

- [Documentation index](docs/index.md)
- [Write workflows and activities](docs/development/workflows.md)
- [Run a daemon and connect remotely](docs/usage/daemon.md)
- [Configure the runner](docs/config/components.md)
- [Use isolated Git worktrees](docs/usage/worktrees.md)
- [Integrate with the control API](docs/reference/control-protocol.md)
- [Troubleshooting](docs/troubleshooting.md)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, tests, and documentation
guidelines. The Python package is `vectorops.runner`; the distribution is
`vectorops-runner` and the command is `vops-runner`.

## License

[MIT](LICENSE).
