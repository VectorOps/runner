---
name: documentation-standards
description: Use this skill when writing, reviewing, reorganizing, or expanding project documentation so page structure, tone, navigation, examples, and reference sections stay consistent.
---

# Documentation Standards

This skill defines the project standard for documentation. It covers page structure, tone, information architecture, examples, reference formatting, and project-specific rules for documenting configuration, protocols, workflows, persistence, and terminal behavior.

The goals are documentation that is practical, easy to scan, easy to navigate, and reliable as a source of truth for users and contributors.

## Scope

These standards apply to repository documentation written in Markdown, including:

- top-level docs under `docs/`
- feature or subsystem documentation
- reference pages for settings, packets, schemas, and workflows
- how-to guides and getting-started content
- local feature guidance such as `SKILL.md` where appropriate

They cover:

- documentation architecture and sectioning
- page taxonomy and templates
- tone and writing style
- examples, tables, and code blocks
- reference and task-oriented documentation
- project-specific documentation needs
- navigation, sequencing, and cross-linking

## Core principles

Every documentation page should follow these principles:

- Task-oriented: start from what the reader wants to do.
- Concrete: prefer examples over abstract explanation.
- Reference-friendly: make contracts, defaults, and invariants easy to find.
- Predictable: similar pages should use similar structure.
- Concise: include what is needed, not long introductions or filler.
- Stable: treat documented config, packet fields, command behavior, and schemas as public interfaces.
- Navigable: organize the docs tree so readers can predict where information lives.

## Documentation architecture

The full documentation set should be organized as a coherent system, not just a collection of pages.

A good reference model is a docs site that separates onboarding, usage, configuration, and development concerns into clear top-level sections.

For this repository, prefer a structure close to:

- overview and getting started
- usage and operations
- configuration and customization
- development and integration
- troubleshooting and platform notes

Recommended top-level pages or sections:

- `docs/index.md` for landing and entry points
- `docs/getting-started.md` for first-run setup and core concepts
- `docs/usage/` for user workflows and TUI or CLI behavior
- `docs/config/` for settings, config loading, and examples
- `docs/reference/` for schemas, packets, node kinds, and persistence contracts
- `docs/development/` for local development, testing, and extension points
- `docs/troubleshooting.md` for common failures and diagnostics

The exact filenames can evolve, but the separation of concerns should stay stable.

## Navigation and discoverability

- The docs landing page should link readers into the main paths they are likely to need next.
- Group pages by reader goal, not by internal module names alone.
- Long reference pages should include a short table of contents near the top.
- Each page should make it obvious what comes before and after it.
- Prefer a small number of stable top-level categories over many shallow one-off pages.

If this repo later adopts a docs site generator, preserve these categories in sidebar navigation.

## Documentation taxonomy

Each page should have a clear type. Do not mix page types without structure.

Preferred page types:

- landing pages: overviews and links into subsections
- getting started pages: installation, setup, first successful run
- how-to pages: specific tasks in step order
- reference pages: exact fields, options, packet shapes, and defaults
- concept pages: explain a subsystem or model at a high level
- troubleshooting pages: symptoms, likely causes, and concrete fixes

When a topic grows too large, split concept, how-to, and reference content into separate pages instead of combining everything into one long document.

## Tone and voice

- Use plain, direct language.
- Write in present tense.
- Prefer short paragraphs and short sentences.
- Explain what something does before explaining edge cases.
- Avoid marketing language, filler, and unnecessary background.
- Use consistent terms for the same concept across pages.

Good:

```md
## Settings

Project settings override global settings.
```

Avoid:

```md
## Settings

This section aims to provide a comprehensive overview of the various settings-related capabilities available in the system.
```

## Page structure

Most pages should follow this shape:

```md
# Title

One or two sentences describing what the feature is and when to use it.

## Quick start

Minimal working example.

## Main concepts or usage

Task-focused sections in the order a user would need them.

## Reference

Tables, field descriptions, packet shapes, or option lists.

## Examples

Realistic examples for common cases.

## Related

Links to adjacent docs when useful.
```

Use only the sections that improve the page. Do not add empty structure for its own sake.

### Page templates by type

Use lighter structure for short pages and stronger templates for important reference areas.

Landing page template:

```md
# Title

One paragraph explaining the scope.

## Start here

- Link to the first pages most users need.

## Main sections

- Group links by user intent.
```

How-to template:

```md
# Title

One paragraph describing the goal.

## Prerequisites

Only if needed.

## Steps

Ordered procedure.

## Verify

What success looks like.

## Related

Next useful links.
```

Reference template:

```md
# Title

One paragraph describing what this page defines.

## Quick reference

Most-used example or summary table.

## Details

Fields, options, variants, defaults, invariants.

## Examples

Valid common cases and edge cases.
```

## Headings

- Use sentence-style headings.
- Keep headings concrete and task-oriented.
- Prefer `How to run a workflow` over `Workflow execution functionality`.
- Use a table of contents near the top for long reference pages.
- Use headings that mirror a predictable scan pattern: overview, setup, usage, reference, troubleshooting.

Recommended heading levels:

- `#` page title
- `##` major tasks or topic areas
- `###` subtopics within a task or reference section

## Openings

Every page should begin with a short description that answers:

- What is this?
- When should I use it?

Example:

```md
# Workflow settings

Workflow settings control how graphs are loaded, validated, and executed. Use them when you need to change runner behavior without changing code.
```

## Quick starts and examples

- Put the smallest useful example near the top.
- Make examples runnable or directly copyable.
- Prefer realistic examples over placeholders.
- Use separate examples for common variants instead of one overloaded example.
- Briefly explain the result or intent after non-obvious examples.
- For getting-started pages, lead the reader to a first successful outcome before detailing all options.

For this repository, examples should usually use:

- YAML for configuration
- Python for library or model examples
- JSON for packets, DTOs, or wire payloads
- Bash for CLI usage

## Lists and tables

- Use bullets for short sets of related points.
- Use numbered lists for ordered procedures or resolution order.
- Use tables for settings, fields, packet properties, enum values, and compatibility notes.
- Keep table descriptions short and concrete.

Good uses of tables in this repo:

- settings keys, types, defaults, and meaning
- protocol packet fields
- node kinds and required fields
- persistence DTO mapping notes

## Code blocks

- Always use fenced code blocks with a language.
- Keep blocks focused on the point being documented.
- Do not mix multiple unrelated examples in one block.
- Prefer one complete minimal example over many fragments.

Examples:

```bash
uv run pytest tests/test_settings_loader.py
```

```yaml
graph:
  nodes:
    - kind: llm
      id: summarize
```

```python
class WorkflowSettings(BaseModel):
    retries: int = 3
```

## Reference documentation

Reference pages should be optimized for lookup.

- Put the most-used information first.
- Start with a short summary or quick reference before deep detail.
- Use tables for fields and options.
- Document defaults explicitly.
- State precedence and merge rules explicitly.
- Call out invariants and validation behavior.
- Separate configuration-time concepts from runtime concepts.
- Break long references into stable subsections so a generated table of contents remains useful.

When documenting models, settings, packets, or DTOs, include:

- field name
- type
- default or required status
- allowed values when constrained
- meaning in runtime behavior
- compatibility notes when relevant

## Task-oriented documentation

How-to pages should be optimized for action.

- Start with the goal.
- List prerequisites only when necessary.
- Present steps in execution order.
- End with expected results, verification, or next steps.
- Keep procedural pages focused on one outcome whenever possible.

Examples of good page types for this project:

- how to define a workflow graph
- how to add a new executor
- how to document a new protocol packet
- how to add a tool implementation
- how to persist new runtime state safely

## Project-specific guidance

This repository has several areas where docs must be precise.

### Settings and config

- Document load order and override behavior.
- Show complete YAML snippets for common configurations.
- Make interpolation, includes, and path resolution rules explicit.
- State whether a field is optional, required, or inferred.
- Prefer a dedicated config reference area over scattering settings across unrelated pages.

### Pydantic models and unions

- Document discriminators such as `kind` as stable contracts.
- List allowed variants and what each variant is used for.
- Show valid and invalid examples when validation rules are subtle.

### Protocols and packets

- Treat packet shape as public API.
- Document wire fields exactly.
- Note backward-compatible additions separately from breaking changes.
- Include small JSON examples for each packet family.
- Keep protocol overview pages separate from packet-by-packet reference when the surface grows.

### Runner and graph behavior

- Distinguish graph definition from runtime execution behavior.
- Document traversal, validation, retries, cancellation, and failure handling explicitly.
- Explain lifecycle in the order events happen.

### TUI and commands

- Document user-facing behavior, not internal widget details.
- Show exact command syntax and common examples.
- Keep output examples short and representative.
- When documenting slash commands, follow the command consistency rules in `.vocode/skills/tui-command-standards/SKILL.md`.
- Prefer one command index page plus separate pages for larger command families if the command set expands.

## Page sequencing

Readers should be able to move through the docs in a natural order.

Recommended progression:

1. what the project is
2. how to get it running
3. how to perform common tasks
4. how to configure it
5. exact reference details
6. how to extend or develop it
7. how to debug problems

Landing pages and section index pages should reinforce this flow.

## Cross-linking

- Link to related pages where a user would naturally need them next.
- Keep links in the body when they help the current task.
- Avoid dumping large unrelated link lists.
- Prefer relative links for project docs.

## Formatting rules

- Use one sentence per line only if a future docs tool requires it. Otherwise use normal Markdown paragraphs.
- Use backticks for commands, file paths, environment variables, field names, enum values, and packet kinds.
- Keep line length reasonable for GitHub reading.
- Avoid deep nesting.
- Avoid raw HTML unless Markdown cannot express the content.

## What to avoid

- Long conceptual introductions before the first useful example
- Repeating the same explanation across multiple pages
- Mixing tutorial, reference, and design rationale without structure
- Large code dumps when a smaller example is enough
- Undocumented defaults or hidden precedence rules
- Vague wording such as `may`, `typically`, or `basically` when behavior is deterministic

## Suggested document set for this repo

As the documentation grows, prefer a top-level `docs/` directory with focused pages such as:

- `docs/index.md` for an overview and entry points
- `docs/getting-started.md` for setup and first workflow
- `docs/usage/` for runner, workflow, and terminal usage
- `docs/config/settings.md` for configuration reference
- `docs/reference/protocol.md` for manager and UI packet contracts
- `docs/reference/workflows.md` for graph, node, and traversal contracts
- `docs/reference/persistence.md` for state and DTO behavior
- `docs/development/index.md` for local development and testing
- `docs/troubleshooting.md` for common failures and diagnostics

Keep feature-specific operational notes close to the feature when they are only relevant there, such as `SKILL.md` files under `.vocode/skills/`.

## What OpenCode gets right structurally

Useful structural patterns to adopt:

- a strong landing page that gets users to a first successful workflow quickly
- docs grouped by user intent instead of code package boundaries alone
- long reference pages with a clear internal outline
- consistent templates across config, CLI, and SDK pages
- distinct areas for usage, configuration, and development APIs

This project should adopt those structural patterns while keeping content aligned to its own architecture and terminology.

## Documentation checklist

Before merging documentation, check that it:

- explains the feature in the first paragraph
- includes at least one concrete example when the feature is user-facing
- documents defaults, precedence, and allowed values when relevant
- uses headings that match user tasks
- uses consistent project terminology
- links to related pages when a follow-up step is likely
- does not contradict tests, schemas, or command behavior
- fits clearly into the docs taxonomy and section structure
- avoids duplicating content that belongs in an existing reference page

## Recommended use of this skill

Use this skill when:

- creating a new documentation page
- reviewing docs for consistency
- reorganizing the docs tree
- splitting a large page into concept, how-to, and reference pages
- documenting settings, packets, workflows, or persistence behavior

If the documentation includes TUI slash commands, also use `.vocode/skills/tui-command-standards/SKILL.md`.

## Style summary

Write docs that are concise, practical, reference-friendly, and example-driven. Organize them into a stable structure that helps readers move from onboarding to usage to reference without digging through source code or guessing where information lives.