---
name: tui-command-standards
description: Use when designing or reviewing terminal command semantics, help, output, errors, and interactive shell behavior.
---

# Terminal command standards

Keep command behavior predictable and help easy to scan. The runner exposes a CLI
and connected shell, not a full-screen TUI. The shell accepts an optional leading
slash; do not invent command families that the runner does not implement.
Check the parser and tests for supported syntax before documenting examples.

## Names and behavior

- Group related actions under a noun, such as `execution list` or `auth profile add`.
- Reuse established verbs: `list`, `show`, `start`, `resume`, `stop`, `add`, `delete`.
- A bare command family shows help, not a mutation or an inferred target.
- Keep required arguments minimal and ordered consistently.
- Support ordinary quoting rather than command-specific escaping rules.
- Read commands must not silently change state.
- State when a mutation is only accepted, not yet complete.
- Report no-op changes honestly and make destructive scope explicit.

## Help and examples

Show a short description, exact syntax, arguments, defaults, and one useful example.
Use required placeholders such as `EXECUTION_ID` and optional arguments in brackets.
Global options precede the command; client connection options precede its family.

```text
vops-runner client --port 8765 execution list
vops-runner client --port 8765 execution show EXECUTION_ID
```

Connected-shell examples should use the same family syntax without repeating the
executable and connection arguments. Explain any difference from one-shot commands.

## Output and errors

- Put the result or required action first.
- Keep human output concise; preserve machine-readable JSON contracts separately.
- Give explicit empty states and mark truncated or paginated results.
- Errors identify the problem and offer a concrete next step.
- Do not expose tokens, raw authentication payloads, or internal tracebacks.
- A timeout or lost response is not proof that a mutation failed. Explain safe retry.
- Color is optional emphasis, never the only indicator of success or failure.
- Use sentence case, stable labels, and layouts readable in narrow terminals.
- Do not use em dashes.

## Verification

Test help, invalid arguments, empty results, no-op mutations, errors, and JSON output.
For asynchronous operations, test acceptance separately from completion. Preserve
documented exit codes and existing command meanings when adding subcommands.
