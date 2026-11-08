---
name: documentation-standards
description: Use when writing or reviewing project documentation, examples, navigation, and reference pages.
---

# Documentation standards

Write for people using or extending the runner, not for the implementation tracker.
Check relevant code and tests before documenting behavior.

## Organize by reader task

- `README.md`: what the project does, installation, first run, and documentation links.
- `docs/index.md`: navigation grouped by user goal.
- `docs/usage/`: step-by-step tasks and operational guidance.
- `docs/config/`: settings, defaults, precedence, and examples.
- `docs/development/`: authoring APIs and extension guides.
- `docs/reference/`: protocol fields, metrics, and compatibility contracts.
- `CONTRIBUTING.md`: development setup and checks.

Keep proposals, implementation logs, and delivery checklists out of these pages.
Preserve compatibility and security warnings that affect current users.

## Write a useful page

1. Explain what the reader can do and when it is useful.
2. State prerequisites and show the smallest complete example.
3. Explain expected results and the next step.
4. Put detailed options and edge cases after the example.
5. Link related pages rather than repeating their reference material.

Use one title, sentence-case headings, short paragraphs, and fenced code blocks
with language labels. Use tables for fields, defaults, and allowed values. Add a
contents list to long references. Do not use em dashes, marketing language, or
unexplained internal terminology.

## Verify examples and contracts

- Run examples where practical. Identify prerequisites and illustrative placeholders.
- Use Python workflows and TOML configuration, not the removed YAML graph format.
- Distinguish the project, runner workspace, execution target, and user configuration.
- Use `.voc/skills/` for project skills. Runtime data belongs in `.voc/data/`.
- State whether an operation is immediate, asynchronous, replayed, or repeatable.
- Document interrupted side effects without promising exactly-once execution.
- Keep `kind` values and wire field names exact, even when historical names are awkward.
- Explain permissions and containment separately. A target path is not a sandbox.
- Never include secrets or local developer paths.

## Review before finishing

Check local links and anchors, imports, command syntax, defaults, and example output.
Update the index when adding or moving pages. Search for old paths and contradictory
claims. Report checks that could not be run. For terminal command guidance, use
`.voc/skills/tui-command-standards/SKILL.md`.
