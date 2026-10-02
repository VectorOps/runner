# VectorOps Runner

> WIP: this project is under active development and the public surface is still evolving.

VectorOps Runner is the execution engine for the VectorOps workflow family.

## Quick start

This repository targets Python `>=3.13,<3.14`.

1. Install dependencies:

```bash
uv sync
```

2. Run the package entry point:

```bash
uv run python -m vectorops.runner
```

## Current scope

The design in `docs/design.md` describes three major roles:

- harness
- host
- runner

This repository is the runner-focused project. The initial scaffold provides:

- `uv` project metadata
- an installable `vectorops.runner` package under `src/`
- a minimal module entry point for future CLI and runtime bootstrapping

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