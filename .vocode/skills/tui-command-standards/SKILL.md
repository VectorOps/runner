---
name: tui-command-standards
description: Use this skill when designing, reviewing, or implementing TUI slash commands so command semantics, help, formatting, output, and color usage stay consistent.
---

# TUI Command Standards

This document defines the user-facing standard for TUI slash commands. It is the reference for command naming, semantics, help behavior, output structure, formatting, and color usage.

The goals are consistent command behavior, easy command discovery, and a command surface that scales cleanly as new subcommands are added.

## Scope

These standards apply to all interactive slash commands exposed through the TUI command manager, including top-level commands and subcommands.

They cover:

- command semantics and lifecycle
- naming and subcommand structure
- help and discovery behavior
- argument conventions
- success, warning, and error output
- formatting, truncation, and pagination
- color usage
- extensibility rules for future commands

## Core principles

Every command should follow these principles:

- Predictable: similar commands behave the same way.
- Discoverable: users can find commands from help output and command-specific guidance.
- Composable: command families should scale to additional subcommands without breaking mental models.
- Concise: default output should be useful without being noisy.
- Recoverable: failures should explain what went wrong and what the user can do next.
- Stable: command names, parameter meaning, and output structure should not change casually.

## Command model

### Invocation style

All interactive commands are slash commands.

Canonical form:

```text
/command [subcommand] [arguments...]
```

Examples:

```text
/help
/repo list
/repo add my-repo /workspace/my-repo
/var set api_base https://example.test
```

### Command families and subcommands

Prefer command families for related actions instead of creating many unrelated top-level verbs.

Use this pattern:

```text
/noun action ...
```

Preferred examples:

```text
/repo list
/repo add <name> <path>
/repo refresh <name>
/var list
/var set <name> <value>
```

Avoid this pattern unless the command is truly global and standalone:

```text
/list-repos
/add-repo
/refresh-repo
```

Guidelines:

- Top-level command names should usually be nouns or domains.
- Subcommands should usually be verbs.
- A command family should present a complete, internally consistent action set.
- Adding a new subcommand must not require changing the meaning of existing subcommands.

### When a top-level command is acceptable

Use a top-level verb only when the action is globally meaningful and not part of a larger domain model.

Examples:

```text
/help
/continue
/reset
```

## Naming standards

### Command names

- Use short, lowercase names.
- Use a single stable word when possible.
- Prefer full words over abbreviations.
- Avoid aliases unless they are clearly necessary.
- Avoid punctuation in command names.

Preferred:

```text
/help
/repo
/workflow
/auth
```

Avoid:

```text
/repos-manager
/wf
/Auth
```

### Subcommand names

- Use lowercase verbs.
- Reuse common verbs across command families.
- Prefer consistent action names such as `list`, `show`, `get`, `set`, `add`, `remove`, `delete`, `refresh`, `status`, `login`, `logout`, `cancel`.

Verb intent should be consistent everywhere:

- `list`: compact overview of multiple items
- `show`: detailed view of one item
- `status`: current runtime or connection state
- `add`: create or register a new item
- `set`: replace or assign a value
- `remove` or `delete`: remove an item
- `refresh`: re-read or re-sync external state
- `cancel`: stop an in-progress operation

Do not use multiple verbs for the same action in different command families unless there is a clear semantic difference.

## Semantic standards

### Principle of least surprise

Commands should do what their names imply with no hidden side effects.

- Read-style commands must not mutate state.
- Mutating commands must be explicit.
- Destructive commands must never be ambiguous.

### Default behavior

Each command should have a useful default action only when the default is obvious.

Acceptable:

```text
/help
/auth status
```

If the command family contains multiple distinct actions, invoking only the parent command should show focused help for that family instead of guessing.

Preferred:

```text
/repo
```

Should return family help such as available subcommands, purpose, and common examples.

### Idempotence expectations

When possible:

- `list`, `show`, `status`, and `help` should be idempotent.
- `set` should leave the system in the same final state when repeated with the same value.
- `add` and `delete` should clearly report if the target already exists or is missing.

### Side-effect transparency

Commands that trigger expensive, asynchronous, or external work should say so in output.

Examples:

- refreshing repositories
- starting a workflow
- authenticating with an external service
- canceling an active process

The user should always understand whether the command:

- completed immediately
- started a background operation
- is waiting for external input

## Argument and parsing standards

### Syntax and tokenization

Commands are tokenized as shell-like arguments. Quoted values should be supported for multi-word input.

Examples:

```text
/var set greeting "hello world"
/repo add app "/workspace/My App"
```

Guidelines:

- Design commands so quoted arguments are enough for ordinary usage.
- Do not invent custom escaping rules per command.
- If a command accepts free-form text, consume the remainder as a variadic tail instead of forcing awkward syntax.

### Positional arguments

Positional arguments are the default. They must appear in a stable order.

Rules:

- Keep required positional arguments minimal.
- Order arguments from most important to least optional.
- Do not overload positions with different meanings.
- Do not make users infer whether an argument is an identifier, a name, or a path.

Use descriptive parameter names in help output:

```text
/repo add <name> <path>
/var set <name> <value>
/run <workflow>
```

### Variadic arguments

Use a variadic tail only when the command genuinely needs a flexible remainder.

Examples:

```text
/help <command...>
/queue add <message...>
```

Rules:

- At most one variadic tail.
- Variadic input must come last.
- Help text must make it obvious that multiple values are accepted.

### Optional arguments

If an argument is optional, the command should still remain easy to understand from help output.

Preferred notation:

```text
/branch switch [name]
/repo refresh [name]
```

Only add optional arguments when they are genuinely useful. If optional positional arguments make semantics unclear, split the behavior into explicit subcommands instead.

## Help and discovery standards

### Global help

`/help` is the main discovery surface for slash commands.

Global help should:

- list every non-hidden command
- show a one-line description for each command
- show the canonical usage signature
- prefer stable ordering
- remain concise enough to scan quickly

Recommended structure:

```text
Commands:
  /auth status - Show authentication status
  /repo list - List configured repositories
  /repo add <name> <path> - Add a repository
```

Descriptions should be short, action-oriented, and parallel in tone.

### Family help

When a command family is invoked without a required subcommand, return family-specific help instead of a generic error.

Family help should include:

- a one-line summary of the command family
- the available subcommands
- a short description for each subcommand
- one or two representative examples when helpful

Recommended structure:

```text
/repo commands:
  /repo list - List configured repositories
  /repo add <name> <path> - Add a repository
  /repo refresh [name] - Refresh one repository or all repositories
```

### Error-driven discovery

When a user enters an unknown subcommand or invalid syntax, the response should help them recover.

Preferred behavior:

- identify the exact problem
- show the expected usage
- point to `/help` or family help when appropriate

Preferred examples:

```text
Command error: Missing value for 'path' (position 2).
Usage: /repo add <name> <path>
```

```text
Unknown command: /rep
Try: /help
```

## Output standards

### General rules

Command output should be structured, compact, and easy to scan in a terminal.

Rules:

- Put the most important information first.
- Avoid walls of text.
- Use one message per logical result when possible.
- Prefer stable wording across commands.
- Prefer plain language over internal implementation terms.

### Success output

Success responses should confirm the action and include only the details the user is likely to need next.

Preferred:

```text
Added repository 'app' at /workspace/app.
```

```text
Variable 'api_base' updated.
```

Avoid vague output:

```text
Done.
Success.
Operation completed.
```

### Read-only output

For `list`, `show`, and `status` commands:

- use labels and ordering consistently
- keep summary views compact
- make empty states explicit
- do not force users to parse prose to extract facts

Preferred empty state examples:

```text
No repositories configured.
No queued inputs.
No active workflow.
```

### Multi-item lists

List output should be optimized for scanning.

Rules:

- one item per line by default
- use stable ordering
- show the most identifying field first
- include compact secondary details only if they help users choose the next command

Preferred:

```text
Repositories:
  1. app - /workspace/app
  2. docs - /workspace/docs
```

### Detailed views

Detailed views may span multiple lines, but should still be clearly structured.

Preferred:

```text
Repository: app
Path: /workspace/app
Status: indexed
Last refresh: 2026-04-20 15:32 UTC
```

### Truncation and pagination

When output can grow large:

- prefer truncation for default views
- mention when truncation occurred
- provide a clear way to inspect more detail via another command or explicit subcommand

Examples:

- show the first N items in a default list
- trim multi-line values in list output
- use `show` for full details and `list` for summaries

If truncation happens, state it clearly:

```text
Showing first 10 queued items.
```

### Progress and long-running operations

For long-running commands, show progress in phases:

- acknowledgement that work started
- intermediate status when meaningful
- final success or failure result

Preferred pattern:

```text
Refreshing repositories...
Refreshed 3 repositories.
```

### Output tone

Use direct, neutral, helpful language.

Preferred tone:

- factual
- concise
- specific

Avoid:

- conversational filler
- celebratory language
- blameful phrasing
- internal stack trace details in normal user-facing output

## Error handling standards

### Error message format

User-facing command errors should follow a consistent pattern:

```text
Command error: <clear explanation>
```

The explanation should:

- identify the invalid argument, missing value, or failed precondition
- name the relevant entity when possible
- avoid exposing internal exception details unless they are already user-meaningful

### Usage errors

Usage errors should say what is wrong with the input and, when possible, show the correct usage.

Examples:

```text
Command error: Missing value for 'name' (position 1).
Usage: /repo add <name> <path>
```

```text
Command error: Expected 1 argument(s), got 2.
Usage: /continue
```

### State errors

If the command cannot run because the system is not in the required state, say so directly.

Examples:

```text
Command error: No active workflow to continue.
Command error: Authentication is required before verification.
```

### Unknown commands and subcommands

Unknown commands should explicitly mention the command name and offer a recovery path.

Preferred:

```text
Unknown command: /unknown
Try: /help
```

For unknown subcommands, prefer family guidance:

```text
Command error: Unknown subcommand 'sync' for /repo.
Try: /repo
```

## Formatting standards

### Layout

- Use sentence case for headings and labels in output.
- Use indentation to show hierarchy.
- Use blank lines sparingly and only to separate distinct sections.
- Avoid decorative separators unless they add clear value.

### Signatures and placeholders

Use these conventions in help text:

- required positional argument: `<name>`
- optional positional argument: `[name]`
- choice set: `<list|set|delete>`
- variadic tail: `<items...>`

Examples:

```text
/repo add <name> <path>
/repo refresh [name]
/var <list|set> ...
/help <command...>
```

### Capitalization and punctuation

- Commands themselves are lowercase.
- Sentences in output should use normal capitalization.
- End full-sentence status lines with punctuation.
- Keep list labels and item descriptions consistent.

### Tables versus plain text

Use simple aligned layouts only when they improve scanability. Do not require complex table rendering for ordinary command output.

Prefer plain text lists unless:

- there are multiple comparable fields
- alignment materially improves readability
- the result remains legible in narrow terminals

## Color standards

Color should reinforce meaning, not carry meaning alone.

Every command must remain understandable in monochrome output.

### Semantic palette

Recommended semantic usage:

- default text: normal foreground
- command names and usage tokens: high-contrast accent or plain text
- success state: green
- warning or caution: yellow
- error state: red
- metadata, hints, or secondary context: dim
- selected emphasis or section titles: strong weight rather than relying only on color

### Color rules

- Use the same color semantics across all commands.
- Never encode critical distinctions by color alone.
- Avoid using more than one semantic accent in a single short line unless necessary.
- Prefer dim styling for hints, examples, and low-priority metadata.
- Reserve red for errors and failed states.
- Reserve yellow for caution, partial completion, or user action needed.
- Reserve green for successful completion or active key hints where already established by the TUI.

### Color in help output

Help screens should use color lightly.

Recommended:

- highlight the command or key token
- keep descriptions in normal text
- dim secondary hints

Help output should still be easy to read if color is unavailable.

## Command family design

### Recommended family shape

When designing a command family, aim for this progression:

- `list` for overview
- `show` or `status` for detail
- `add` or `set` for mutation
- `remove` or `delete` for removal
- `refresh`, `login`, `logout`, `cancel`, or domain-specific verbs where needed

Not every family needs every action, but the set should feel internally complete.

### Family-level defaults

A bare family command should do one of the following:

- show family help
- show status, if status is the obvious primary purpose of the family

Do not make a bare family command perform a mutating action.

### Extensibility rules

New subcommands should be additive and predictable.

Before adding a subcommand, verify:

- the action does not already fit an existing verb
- the family remains discoverable in help output
- the new usage signature is distinct and unambiguous
- the output style matches existing siblings in the family

## Behavioral standards for mutating commands

### Confirmation model

Mutating commands should confirm what changed.

Preferred:

```text
Removed repository 'app'.
```

If the effect is deferred or asynchronous, say so:

```text
Started refresh for repository 'app'.
```

### Destructive actions

If an action is irreversible or high impact, the wording must be explicit. If the TUI later adds confirmation flows, destructive commands should integrate with them consistently.

Until explicit confirmation support exists, destructive commands should:

- use unmistakable verbs like `delete` or `clear`
- clearly identify the target in the confirmation output
- avoid silent broad-scope behavior

### No-op mutations

If a mutating command changes nothing, report that clearly instead of pretending success.

Examples:

```text
Repository 'app' is already configured.
Variable 'mode' already has that value.
```

## Discoverability checklist for new commands

Every new command or subcommand should answer yes to these questions:

- Is the name short and obvious?
- Is it grouped under the right command family?
- Does `/help` or family help reveal it?
- Is the usage signature self-explanatory?
- Does the default output tell the user what happened?
- Are failure cases actionable?
- Does it follow the same color and formatting semantics as other commands?

## Recommended documentation pattern for each command family

When introducing a new family, document it in this shape:

### Purpose

One sentence describing what the family manages.

### Commands

```text
/family list
/family show <name>
/family add <name> <value>
/family remove <name>
```

### Notes

- expected empty state behavior
- whether actions are immediate or asynchronous
- any important output or truncation behavior

## Examples of standardized command UX

### Help

```text
Commands:
  /auth status - Show authentication status
  /repo list - List configured repositories
  /repo add <name> <path> - Add a repository
  /var set <name> <value> - Set a variable
```

### Family help

```text
/repo commands:
  /repo list - List configured repositories
  /repo add <name> <path> - Add a repository
  /repo refresh [name] - Refresh one repository or all repositories
```

### Success

```text
Added repository 'app' at /workspace/app.
```

### Empty state

```text
No repositories configured.
```

### Usage error

```text
Command error: Missing value for 'path' (position 2).
Usage: /repo add <name> <path>
```

### State error

```text
Command error: No active workflow to continue.
```

### Long-running action

```text
Refreshing repositories...
Refreshed 3 repositories.
```

## Implementation guidance

These standards are user-facing, but they should also shape command implementation:

- register commands with a clear description and canonical parameter names
- preserve stable ordering and naming in help entries
- parse arguments consistently using the shared command manager behavior
- use family help instead of ambiguous defaults
- keep output templates consistent across sibling commands
- keep hidden commands out of ordinary discovery paths

## Non-goals

This document does not require:

- a specific internal rendering component for every command
- complex rich formatting for ordinary output
- aliases for every command
- verbose prose in command responses

The standard is optimized for clarity, consistency, and growth of the command surface over time.