# Component configuration

Set workspace defaults for LLMs and reusable workflow components in `config.toml`.
Workflows read arbitrary nested values with `await configuration.get(name, default)`.
Each read is an activity: replay returns its recorded value, while a new read loads
the current workspace file. There is no central component schema or global digest.

## Quick start

```toml
[components.llm]
model = "gpt-4.1"
provider = "openai"
max_output_tokens = 4096
timeout = 120
max_rounds = 20
```

```python
from vectorops.runner.api import llm

async def main(ctx):
    agent = llm.LLM("assistant")
    agent.add_user_message("Explain deterministic workflow replay.")
    response = await agent.run()
    return response.model_dump(mode="json")
```

The model above is an example, not a built-in default. Configure a model or supply
one explicitly. Authentication remains the provider adapter's runtime concern;
do not put credentials in component settings.

For models that support reasoning, thinking level uses the existing provider
contract:

```toml
[components.llm.reasoning]
effort = "high"
summary = "auto"
```

Model/provider support for reasoning options is checked by the provider, not by
the configuration manager.

## Read a configuration value

```python
from vectorops.runner.api import configuration

async def main(ctx):
    language = await configuration.get("report.language", "English")
    options = await configuration.get("report", {})
    return {"language": language, "options": options}
```

Paths are relative to `[components]`: `llm.reasoning.effort` selects
`[components.llm.reasoning]`'s `effort` value. Paths must have nonempty segments;
literal dots in keys and list indexing are not supported. A missing file, missing
key, or non-mapping intermediate value returns the supplied default (None when
omitted). False, zero, empty lists, and empty mappings remain actual values.
The getter returns the selected value as-is; it does not merge mappings or infer
types from the default. Consumers own type and value validation.

Each fresh read opens `config.toml` off the event loop. There is no process cache,
environment interpolation, or additional configuration search path. Selected
values and defaults must be finite JSON-compatible values. Parsing or I/O errors
other than a missing file become recorded activity failures, not silent defaults.
Replace the file atomically when editing to avoid reading a partially written file.

The activity contract is `kind="configuration.get"`, `name="get"`, version 1,
with `name` and `default` inputs and the selected value as its result. Returned
containers are detached so caller mutation cannot alter recorded history.
Reads require workflow context and cannot be nested inside an activity.

Configuration values and defaults are persisted in saga history and can appear in
history exports and step events. Never read credentials or secrets through this
API. A subtree read records the entire selected subtree, not just fields used later.

## LLM options

`LLM` accepts an optional `Settings` or `SettingsOverrides` positional argument,
plus generation-option keywords. Keywords override the positional model's
explicit fields; workspace values supply omitted fields.

The constructor does not read configuration. Each `generate()` and each tool-loop
round records one `configuration.get("llm", {})` snapshot, then validates and
merges it with explicit overrides. Changes to model, thinking level, or other
defaults take effect at the next fresh round, even on an existing LLM object.
Compaction decisions and generation within one round share that snapshot.
The round limit is resolved once at the start of `run()` and remains fixed for
that invocation; the next `run()` can adopt a changed limit.

LLM precedence is built-in defaults, workspace values, then explicit options.
Its owner merges nested mappings recursively; scalars and lists replace lower
layers. Explicit `None` clears optional settings rather than inheriting them.
Pydantic option models contribute only explicitly supplied fields. Pydantic here
validates LLM-specific options, not the shape of general workspace configuration.

```python
import connect
from vectorops.runner.api import llm

async def main(ctx):
    agent = llm.LLM(
        "reviewer",
        llm.SettingsOverrides(temperature=0),
        model="o3",
        reasoning=connect.ReasoningConfig(effort="low"),
        compaction=None,
    )
    agent.add_user_message("Review this design.")
    return (await agent.run(max_rounds=5)).model_dump(mode="json")
```

`Settings` still requires a model. Use `SettingsOverrides` for a partial override
that inherits the workspace model. `reasoning=None` disables workspace reasoning;
`compaction=None` disables workspace compaction. Omitted compaction inherits it.
`LLM.settings` returns a detached snapshot of the last resolved generation options.
Before the first generation it raises `RuntimeError`; construction is synchronous
and cannot perform a recorded read.

Workflow-side helpers can call `await llm.resolve_settings(...)` with the same optional
settings and generation keywords. This requires active workflow context and does
not work inside an activity; resolve before entering its recorded callback.

| Workspace field | Built-in default | Validation/meaning |
| --- | --- | --- |
| `model` | Required after merging | Nonblank model name |
| `provider` | None | Optional provider name |
| `credential_profile` | None (`default` profile) | Non-secret runner credential profile reference |
| `temperature` | None | Number from 0 through 2 |
| `max_output_tokens` | None | Positive integer |
| `timeout` | None | Positive timeout in seconds |
| `stream_read_timeout` | None | Positive stream-read timeout in seconds |
| `reasoning` | None | Provider reasoning configuration |
| `reasoning.effort` | None | `none`, `minimal`, `low`, `medium`, `high`, `xhigh` |
| `reasoning.summary` | None | `auto`, `concise`, `detailed` |
| `reasoning.enabled` | None | Optional boolean |
| `reasoning.max_tokens` | None | Optional provider reasoning token budget |
| `max_rounds` | 100 | Positive integer; `run(max_rounds=...)` overrides it |
| `compaction` | None | Optional compaction policy below |

Unknown LLM and reasoning fields are rejected during resolution. No model,
temperature, or reasoning capability is inferred from a model name.

```toml
[components.llm.compaction]
max_context_characters = 100000
keep_recent_messages = 4
```

Compaction requires a positive `max_context_characters`. `keep_recent_messages`
defaults to 4 and must be positive. Optional `instruction` overrides the existing
summary prompt. Generation and compaction both use the resolved model, reasoning,
output budget, temperature, and timeouts.

## Reusable workflow components

```python
from vectorops.runner.api import configuration

async def main(ctx):
    count = await configuration.get("report.max_sections", 5)
    if isinstance(count, bool) or not isinstance(count, int) or count < 1:
        raise ValueError("report.max_sections must be a positive integer")
    return {"max_sections": count}
```

```toml
[components.report]
language = "French"
max_sections = 8
```

The example returns eight sections. No registration or Pydantic schema is required.
The same recorded reads can safely drive branching, loops, and other workflow logic.

## Replay and restart

Completed configuration reads return their original values without reopening the
file. New reads use current settings. For example, a workflow can read model A,
wait for host input, then read model B after the file is edited. On restart it
reconstructs A from history and reads B only when reaching the new read.

No global configuration digest blocks edits. Completed LLM generations replay
their results, using their preceding recorded configuration snapshots to rebuild
the original request. An interrupted generation also reuses its completed settings
read, even if the workspace model has changed. The following generation gets a
new read. An interrupted configuration read with no committed outcome reads again;
there is no historical value until the activity completes durably.

Changing a read's path or default in workflow code can produce the normal activity
input replay mismatch. Explicit LLM overrides remain part of the generation
request digest, so changing code-level overrides can also mismatch old requests.
Workspace edits do not change those inputs. Keep workflow code and activity order
stable across replay.

LLM histories created before recorded configuration reads were introduced lack
these new activities. Start new executions when adopting this version for those
workflows; inserting the reads is not an automatic history migration. Runner
directory, logging, and retention settings still use their existing startup rules.

## Related

- [Provider authentication and global secrets](authentication.md)
- [Workflow authoring](../development/workflows.md)
- [Structured logging](logging.md)