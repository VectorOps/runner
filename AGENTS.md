# Agent guide

This repository implements durable Python workflows and LLM agents, with a CLI, daemon, and control API. It uses Pydantic v2 for schemas, strong typing, and discriminated unions.

## Stack and assumptions
- Python 3.13 is required by project metadata.
- Core libraries: Pydantic v2, VectorOps Connect, asyncio, SQLite, and MCP.
- Testing: pytest.
- Configuration: TOML. Workflow definitions: async Python functions.
- Project skills: `.voc/skills/`. Runtime data: `.voc/data/`, excluded from Git.

## High-level style and patterns
- Pydantic v2 BaseModel is the canonical schema:
  - Use discriminated unions via `Annotated[Union[...], Field(discriminator="kind")]`.
  - Prefer `str`-backed Enums for wire stability.
  - Use `default_factory` for mutable defaults.
  - Use `field_validator` and `model_validator` for coercion/invariants.
- Typing:
  - Prefer precise types (`dict[str, Any]`, `list[Message]`), `Final`, `ClassVar`, `Literal`.
  - Do not use getattr or hasattr on strongly typed objects.
  - Never use `X | None` composite type, use `Optional[X]`
- Protocols and packets:
  - Include a stable `kind` field; treat enum values and field names as contracts.
- Workflow runtime:
  - Keep external effects in recorded activities and orchestration deterministic.
  - Preserve operation identities and saved-execution compatibility.
- Commands and tools:
  - Async-first handlers; avoid blocking I/O; keep dataclass command defs minimal.

## Architecture and ownership
- Each module or service owns one cohesive responsibility. Split by ownership,
  not file size; do not scatter one behavior across helpers.
- Each model and policy has one authoritative owner for validation,
  interpretation, normalization, and state transitions.
- Orchestration calls owners through explicit contracts. It must not inspect
  private structure, reconstruct representations, or duplicate decision rules.
- Configuration owners handle defaults, merging, and projections.
  Representation owners handle serialization and envelope updates.
- Dependencies point from orchestration to owners, never the reverse.
  Pass narrow capabilities instead of entire runtime containers.
- Adapters translate and delegate. Compatibility facades re-export;
  neither contains a second implementation.
- Keep registration in dedicated adapters loaded by explicit bootstrap.
  Importing reusable APIs must not register unrelated capabilities.
- Preserve public contracts and deterministic replay during refactoring.
  Test owner invariants and boundary guarantees.
- If one policy change requires edits across unrelated modules, fix ownership
  rather than adding another coordinated patch.

## What to change vs never touch (boundaries)
- Never commit secrets or credentials; use environment variables.
- Do not break wire contracts:
  - Do not rename or remove `kind` values, enum values, or packet field names.
  - Additive fields are OK; removals/renames require migration.
- Avoid blocking I/O in async code (executors, UI RPC, process backend).
- Do not vendor large deps or modify generated template files without justification.
- Tests define behavior; update tests only to cover intentional changes.

## Testing
- Tests live in `tests/` and mirror package modules.
- Add tests for:
  - New models or validators (invalid/valid cases).
  - Packet shape changes and discriminators.
  - Executor behavior, cancellation, and interrupted recovery.
  - Fresh execution and replay without repeated completed effects.

## Python best practices for this repo
- You must never add any comments unless explicitly requested. If comment already exists - do not remove it.
- Complex functions should include concise docstrings that explain what the function does and any important design decisions, without being overly chatty.
- Do not use getattr or hasattr on strongly typed objects, such as pydantic BaseModel
- Validation:
  - Normalize with `@model_validator(mode="before")`.
  - Cross-field checks in `@model_validator(mode="after")`.
  - Use `@field_validator(..., mode="after")` for derived checks (e.g., uniqueness).
- Serialization:
  - JSON-safe types; `str` enums; explicit `Optional[...] = None`.
  - When changing runtime state in `src/vectorops/runner/runner/`, check the corresponding SQLite storage, summary projections, and replay tests.
- API stability:
  - New fields should be backward compatible and optional.
  - Keep `kind` and enum values stable.
- Typing and constants:
  - Use `Final` for constants; `ClassVar` for registries.
  - Avoid `Any`; prefer exact models or TypedDict for structured config.
- Comments:
  - Do not add unnecessary comments that do not add value
- Async:
  - Use asyncio primitives; propagate timeouts and cancellation.
- Use `black` to format files after changing them.
- Run `mypy` together with `black` after changing Python files.
- Do not import a single symbol per line from a single module when multiple symbols are imported. This is not allowed:
```python
from vectorops.runner.runner import state
from vectorops.runner.runner import state_store
from vectorops.runner.runner import workspace_database
```

Instead do:
```python
from vectorops.runner.runner import state, state_store, workspace_database
```

- Never import more than three symbols from one module. This is allowed:
```python
from vectorops.runner.control.protocol import Start, Resume, Execution
```

This is not allowed:

```python
from vectorops.runner.control.protocol import (
    Start,
    Resume,
    Execution,
    ExecutionList,
)
```

Instead, import a module and reference symbols via the module name or alias.

## Documentation

- Follow `.voc/skills/documentation-standards/SKILL.md`.
- Write for users and contributors, not as an implementation log.
- Keep README focused on installation, first use, and documentation links.
- Use sentence-case headings and do not use em dashes.

## Shell tools
- If you need to run python-based tools, always run them via `uv run`.
- Format changed files by running `uv run black ...`.
- Type-check Python changes by running `uv run mypy ...`.
