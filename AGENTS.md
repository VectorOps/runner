# # Agents Guide

This repository implements a configurable, graph-driven workflow runner with LLM-backed executors, tools, and a terminal UI. It uses Pydantic v2 for schema, strong typing, and discriminated unions.

## Stack and assumptions
- Python 3.11+ recommended.
- Core libs: pydantic v2, litellm, PyYAML, json5, asyncio.
- Testing: pytest.
- Config: YAML with includes and variable interpolation.

## High-level style and patterns
- Pydantic v2 BaseModel is the canonical schema:
  - Use discriminated unions via `Annotated[Union[...], Field(discriminator="kind")]`.
  - Prefer `str`-backed Enums for wire stability.
  - Use `default_factory` for mutable defaults.
  - Use `field_validator` and `model_validator` for coercion/invariants.
- Typing:
  - Prefer precise types (`dict[str, Any]`, `list[Message]`), `Final`, `ClassVar`, `Literal`.
  - Do not use getattr or hasattr, prefer strong typing and
  - Never use `X | None` composite type, use `Optional[X]`
- Protocols and packets:
  - Include a stable `kind` field; treat enum values and field names as contracts.
- Graph runtime:
  - Separate config-time models (Node/Edge/Graph) from runtime wrappers.
  - Use type-name registration for node dispatch.
- Commands and tools:
  - Async-first handlers; avoid blocking I/O; keep dataclass command defs minimal.
- Design and ownership:
  - Follow single responsibility principle. Each module or service should own one clear area of behavior.
  - Keep ownership boundaries explicit. The type or subsystem that owns a model is responsible for interpreting it.
  - Dependency direction must follow ownership. Higher-level orchestration may call into an owner, but it must not inspect or reconstruct the owner's private model shape.
  - If a model changes, prefer updating its owner rather than spreading shape knowledge across unrelated modules.

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
  - Executor behaviors and preprocessors.
  - Graph validation and traversal.

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
  - When changing runtime state models in `src/vocode/state.py`, update persistence DTOs and conversion logic in `src/vocode/persistence/` accordingly.
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
```
from vectorops.runner.runner import state_replay
from vectorops.runner.runner import state_runtime
from vectorops.runner.runner import state_tracker
```

Instead do:
```
from vectorops.runner.runner import state_replay, state_runtime, state_tracker
```

- Never import more than a three symbols. This is allowed:
```
from .proto import UIPacket, UIPacketEnvelope, UIPacketRunEvent
```

This is not allowed:

```
from .proto import (
    UIPacket,
    UIPacketEnvelope,
    UIPacketRunEvent,
    UIPacketUIReset,
    UIPacketStatus,
    UIPacketRunInput,
)
```

Instead, import a module and reference symbols via the module name or alias.

## Shell tools
- If you need to run python-based tools, always run them via `uv run`.
- Format changed files by running `uv run black ...`.
- Type-check Python changes by running `uv run mypy ...`.
