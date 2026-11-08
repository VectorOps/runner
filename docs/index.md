# VectorOps Runner documentation

VectorOps Runner executes durable Python workflows and LLM agents. Start with a
small local workflow, then add model calls, tools, or a daemon as needed.

## Start here

- [Install and run your first workflow](../README.md#install)
- [CLI usage](usage/cli.md): start, resume, inspect, export, and answer requests.
- [Write a workflow](development/workflows.md): compose async functions and children.
- [Write an activity](development/activities.md): record external work safely.
- [Troubleshooting](troubleshooting.md): setup, authentication, waits, and recovery.

## Run and operate

- [Daemons and remote connections](usage/daemon.md)
- [Git worktrees](usage/worktrees.md)
- [Fetch web content](usage/web-fetch.md)
- [Diagnostic logs](config/logging.md)
- [History retention](config/retention.md)

## Configure and customize

- [Configuration files and overrides](config/components.md)
- [LLM models, profiles, and retries](config/llm.md)
- [Conversation compaction](config/compaction.md)
- [Provider credentials and login](config/authentication.md)
- [Tool approval](config/tool-approval.md)
- [Tool registration and collections](config/tool-collections.md)
- [MCP servers](config/mcp.md)
- [Skills](config/skills.md)

## Develop and integrate

- [Contributor setup](../CONTRIBUTING.md)
- [Configuration reads in workflows](development/configuration.md)
- [Local targets, file tools, and processes](development/target-environments.md)
- [Workspace file search](development/file-search.md)
- [Control protocol and Python client](reference/control-protocol.md)
- [Usage accounting](reference/usage.md)
- [LLM metrics](reference/llm-metrics.md)
- [Saved-execution compatibility](reference/compatibility.md)
- [Security boundaries](../SECURITY.md)

## Project and workspace

The **project** is the directory you work on. The **workspace** stores runner
configuration and workflow source under `<project>/.voc/` by default. Its `data/`
directory holds execution history, logs, and managed worktrees. A **target** is the
directory a particular execution operates on; it can differ from the project.

An **execution** is one root workflow and its children. An **activity** records a
unit of work and its result. During replay, completed activities return their
saved outcomes instead of running again. Interrupted activities can repeat effects;
recording results does not guarantee exactly-once external operations.
