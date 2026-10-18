# Skills

Skills are on-demand Markdown instructions advertised to the built-in agent and helper. Use project skills for local conventions, configured directories for shared guidance, and runner-provided skills for runner APIs.

## Quick start

Create `.vocode/skills/review/SKILL.md` under the attached target project:

```markdown
---
name: review
description: Use when reviewing code changes for correctness and test coverage.
---

# Review

Inspect changed code and relevant tests before reporting findings.
```

Only the frontmatter name, description, and file path enter the prompt. The agent reads the body with `read_file` when the skill matches the task.

## Shared and runner-provided skills

Configure extra skill roots in the runner workspace's `config.toml`:

```toml
[components.skills]
directories = ["~/.vocode/skills", "/opt/team/skills", "../shared-skills"]
include_builtin = true
```

Each root contains direct child directories with a `SKILL.md`. Relative roots resolve against the attached target project, **not** the runner workspace or configuration file. Absolute paths, home-relative paths, and symlinked roots or skill directories are supported. Home directories are not scanned unless configured.

`directories` defaults to `[]` and accepts at most 32 nonblank paths. `include_builtin` defaults to `true`. Runner packages include `workflow-authoring`, which covers workflow creation, APIs, deterministic replay, versioning, and tests; no project copy is required.

Discovery uses this precedence, with the first valid skill for each frontmatter `name` winning:

1. Target project `.vocode/skills`.
2. Configured `directories`, in declaration order.
3. Runner-provided skills installed with the package.

Within a source, directories are selected in filename order. Invalid skills do not shadow valid lower-priority skills. Paths inside the target are target-relative; external skills use absolute paths readable by the normal file tools. Discovery does not grant tool permissions or bypass approval. Treat skills as instructions from their source, not as permission to expand a task.

## Limits and replay

- At most 32 candidate directories per source and 32 unique skills in the merged catalog. A truncated catalog is marked in the prompt. Project and configured skills can fill the catalog before runner defaults.
- Directory selection scans names but retains a bounded candidate list; it does not recursively traverse sources.
- Frontmatter reads are limited to 64 KiB per file and require a complete YAML header. The body can be longer and is read on demand.
- `name` is 1–64 lowercase letters, digits, hyphens, or underscores. `description` is a nonempty string of at most 1,024 characters. Extra metadata is ignored.
- Missing sources are ignored; invalid or unreadable skills are skipped with warnings. Special files are rejected without blocking.
- Discovery and selected settings are consumed inside the recorded `builtin.context` activity. Completed replay reuses its rendered context without rediscovering files or reading current skill settings. Recorded `read_file` results are also reused. Changes take effect for a new context activity, not retroactively in an existing execution.

## Related

- [Workflow composition](../development/workflows.md)
- [Activity execution context](../development/activities.md)
- [Component configuration](components.md)
