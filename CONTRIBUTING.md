# Contributing

Thanks for helping improve VectorOps Runner. Keep changes focused, preserve saved
execution contracts, and include tests or runnable examples for new behavior.

## Set up a checkout

Install Git and [uv](https://docs.astral.sh/uv/getting-started/installation/) on
macOS or Linux, then:

```bash
git clone https://github.com/vectorops/runner.git
cd runner
uv sync --locked
uv run vops-runner --help
```

The project uses Python 3.13. `uv sync` includes the development tools and fetches
the pinned Git dependencies. Do not replace them with machine-specific paths in
committed metadata.

## Run checks

```bash
uv run pytest
uv run black --check src tests benchmarks
uv run mypy src
git diff --check
```

Run focused tests while developing, for example:

```bash
uv run pytest tests/test_skills.py tests/workflows/test_agent.py
```

Format changed Python files with `uv run black PATH...` and type-check them with
`uv run mypy PATH...`. Production type checking does not cover every test module;
report any failures separately rather than treating a partial check as a full pass.
For stricter async checks, use `PYTHONASYNCIODEBUG=1 uv run pytest -W error`.

## Find the relevant code

| Directory | Responsibility |
| --- | --- |
| `src/vectorops/runner/api/` | Workflow, activity, LLM, and tool authoring APIs |
| `src/vectorops/runner/runner/` | Execution, replay, history, and workspace persistence |
| `src/vectorops/runner/control/` | Daemon, client, and control protocol |
| `src/vectorops/runner/environment/` | Local targets and managed processes |
| `src/vectorops/runner/authentication/` | Credential storage and login |
| `src/vectorops/runner/workflows/` | Built-in agent and helper |
| `tests/` | Tests and reusable fixtures |
| `benchmarks/` | Opt-in performance measurements |

See [workflow authoring](docs/development/workflows.md) before changing recorded
operation order. Renaming an operation or inserting a new activity can break
replay. Test both fresh execution and replay when changing durable behavior.

## Documentation

Write for someone who has not read the source. Explain the task, show a small
working example, and describe the result. Keep plans and implementation logs in
issues or pull requests rather than the documentation tree.

- Keep the README focused on installation, first use, and next steps.
- Link new pages from [the documentation index](docs/index.md).
- Check examples, defaults, and imports against current code.
- Preserve important security and saved-execution compatibility warnings.
- Use plain language and sentence-case headings. Do not use em dashes.
- Keep reusable project guidance in `.voc/skills/`.

[AGENTS.md](AGENTS.md) contains additional repository conventions for coding agents.

## Before submitting

Describe the user-visible change and the checks you ran. Do not include runtime
data, credentials, transcripts, or machine-specific paths. Keep unrelated fixes
out of the patch. Report potential vulnerabilities using [SECURITY.md](SECURITY.md),
not a public issue containing sensitive details.
