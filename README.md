# VectorOps Runner

> WIP: this project is under active development and the public surface is still evolving.

VectorOps Runner is the execution engine for the VectorOps workflow family.

## Quick start

This repository targets Python `>=3.13,<3.14`.

1. Install dependencies:

```bash
uv sync
```

2. Show the CLI commands:

```bash
uv run vops-runner --help
```

Use `vops-runner` to start, resume, inspect, and export executions. See the [CLI guide](docs/usage/cli.md) for a runnable workflow and recovery examples. `uv run python -m vectorops.runner` provides the same commands.

## Current scope

The design in `docs/design.md` describes three major roles:

- harness
- host
- runner

This repository is the runner-focused project. It provides:

- `uv` project metadata
- an installable `vectorops.runner` package under `src/`
- persistent workflow execution and crash recovery
- a CLI with read-only status, statistics, and full-execution JSONL export

The distribution is published independently as `vectorops-runner` while the Python import path is `vectorops.runner`.

## Development

Run the package entry point:

```bash
uv run python -m vectorops.runner
```

Run tests:

```bash
uv run pytest
```