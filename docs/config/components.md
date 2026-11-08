# Configure the runner

Use TOML files for shared defaults and project settings. Use runtime overrides to
change component settings without restarting a daemon. Keep credentials in the
[credential store](authentication.md), not in these files.

## Quick start

Create `~/.config/vops-runner/config.toml` to select a model for all projects:

```toml
[components.llm]
model = "openai/gpt-4.1"
```

This is an example model, not a built-in default. Choose a model available through
your provider and [configure its credentials](authentication.md).

To override it for one project, create `<project>/.voc/config.toml`:

```toml
[components.llm.profiles.strong]
model = "openai/o3"
```

The coding agent selects the `strong` profile. Omitted `strong`, `medium`, and
`weak` profiles inherit shared LLM settings. See [LLM configuration](llm.md).

## Configuration files

| Location | Purpose |
| --- | --- |
| `~/.config/vops-runner/config.toml` | User component defaults |
| `<project>/.voc/config.toml` | Project settings and component overrides |
| `<project>/.voc/data/workspace.sqlite3` | Runtime state, including permanent overrides |

Neither TOML file is required or generated automatically. The runner reads the
selected files at startup; it does not search parent directories, interpolate
environment variables in values, or watch files for changes. Restart after edits.

Select the user configuration directory with `--config-dir PATH`, then
`VOPS_RUNNER_CONFIG_DIR`, then the default above. Select the project and workspace
with the [CLI global options](../usage/cli.md#workspace-location).

User configuration accepts only the `components` table. Project configuration
also accepts startup settings:

| Project setting | Default or guide |
| --- | --- |
| `workflows_directory` | `"workflows"`, relative to the workspace |
| `data_directory` | `"data"`, relative to the workspace |
| `environment` | [Local execution target](../development/target-environments.md) |
| `logging` | [Diagnostic logs](logging.md) |
| `retention` | [Execution history retention](retention.md) |
| `worktrees` | [Git worktrees](../usage/worktrees.md#limits-and-backend-configuration) |
| `mcp` | [MCP servers](mcp.md); project shorthand for `components.mcp` |
| `components` | Workflow and tool settings |

Directory overrides must remain inside the workspace. Changing `data_directory`
selects a different data location; it does not move existing history or overrides.
Commit non-secret configuration and workflow source. Ignore the entire data directory.

## User-level defaults and precedence

Settings apply from lowest to highest priority:

1. Consumer defaults.
2. User component settings.
3. Project component settings.
4. Workspace runtime overrides.

User and project tables merge recursively. Scalars and lists replace lower-priority
values, including `false`, `0`, and empty lists. An empty table does not clear
inherited keys. Component owners validate their settings when they read them.
Malformed TOML or unsupported startup settings fail startup.

LLMs additionally apply the selected profile and explicit Python options. A project
shared model does not replace a model in an inherited selected profile; override
that profile's `model` to change it. See [LLM precedence](llm.md#precedence).

## Override component settings

From a connected `RunnerClient`, set or inspect a value:

```python
async def choose_model(client):
    name = "components.llm.model"
    await client.set_configuration(name, "openai/gpt-4.1", permanent=True)
    current = await client.get_configuration(name)
    await client.reset_configuration(name)
    return current
```

Only `components` and its descendants are editable at runtime. Directory, target,
logging, retention, and other startup settings require a restart.

Names are dotted strings. `llm.model` is accepted as shorthand for
`components.llm.model`. Within a segment, `~1` represents a literal dot and `~0`
a tilde. Arrays are read or replaced as whole values.

| Operation | Behavior |
| --- | --- |
| Set with `permanent=False` | Apply until restart; this is the default |
| Set with `permanent=True` | Persist while the corresponding startup TOML value remains unchanged |
| Reset | Remove overlapping overrides and restore the merged startup value |
| Get with no name | Return the effective configuration, including read-only startup settings |
| Suggest | Return advisory model or enum values; suggestions do not restrict writes |

There is one override per key. A temporary write replaces a permanent write; it
does not hide a permanent layer that will return later. Setting or resetting a
parent removes overlapping child overrides, and vice versa. Unrelated keys remain.

At startup, changes to a permanent override's underlying TOML value invalidate that
override. Formatting changes and unrelated edits do not. Runtime operations never
write either TOML file. Use the running daemon's API, not a second workspace writer.

All clients share a workspace's overrides. A successful write affects fresh reads,
not cached resources, in-flight calls, or recorded workflow reads. Invalid component
values can be stored and inspected; validation occurs when the component uses them.

## Replay and restart

Workflow configuration reads are recorded activities. Replay returns the saved
value, even after configuration changes. New reads use current effective settings.
An interrupted generation reuses its already-recorded settings read; a later
generation can pick up a changed model.

For custom settings and validation examples, see
[configuration reads in workflows](../development/configuration.md). For control
packet fields, see [configuration RPCs](../reference/control-protocol.md#configuration-reads-and-overrides).

## Related

- [Models and LLM profiles](llm.md)
- [Compaction](compaction.md)
- [Tool approval](tool-approval.md)
- [Tool collections](tool-collections.md)
- [MCP servers](mcp.md)
- [Skills](skills.md)
