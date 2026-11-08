# Configure LLMs

Choose a model, define reusable profiles, and set limits for generation. Provider
credentials are configured separately through [authentication](authentication.md).

## Quick start

In user or project `config.toml`:

```toml
[components.llm]
model = "openai/gpt-4.1"
```

Use a qualified `<provider>/<model>` name. The prefix selects both the provider and
its credentials. Model names can contain additional slashes, for example
`openrouter/anthropic/claude-sonnet-4`. There is no separate `provider` setting.
Choose a model your account can access; no model is selected automatically.

Save a workflow under `.voc/workflows/explain.py`:

```python
from vectorops.runner.api import llm


async def main(ctx):
    conversation = llm.LLM("assistant")
    conversation.add_user_message("Explain deterministic workflow replay.")
    return (await conversation.run()).model_dump(mode="json")
```

Run it with `uv run vops-runner start explain:main` from the runner checkout, or use
`--project PATH` to select another project. Model calls can incur provider charges.

## Named LLM profiles

Profiles select partial settings without duplicating every option:

```toml
[components.llm]
model = "openai/gpt-4.1"
max_output_tokens = 4096

[components.llm.profiles.strong]
model = "openai/o3"
reasoning = { effort = "high" }

[components.llm.profiles.weak]
model = "openai/gpt-4.1-mini"
```

Use `llm.LLM("reviewer", profile="strong")` to select a profile. The coding agent
uses `strong`. The standard `strong`, `medium`, and `weak` names inherit shared
settings when omitted. Custom names must be configured. Profiles cannot select a
parent profile or contain another profile catalog.

`credential_profile` is different: it names an account in the credential store.
When omitted, a live request uses the active credential profile. Pin it explicitly
when switching the active account must not change subsequent requests.

## Precedence

LLM settings apply in this order:

1. Built-in defaults.
2. Effective [component settings](components.md#user-level-defaults-and-precedence).
3. The selected LLM profile.
4. Explicit Python options.

Nested mappings merge recursively; scalars and lists replace lower layers.
Explicit `None` clears optional settings in Python or runtime overrides. TOML
has no null value. A new `model` replaces the full provider/model selection.

```python
import connect
from vectorops.runner.api import llm


async def main(ctx):
    conversation = llm.LLM(
        "reviewer",
        llm.SettingsOverrides(temperature=0),
        model="openai/o3",
        reasoning=connect.ReasoningConfig(effort="low"),
        compaction=None,
    )
    conversation.add_user_message("Review the proposed API.")
    return (await conversation.run(max_rounds=5)).model_dump(mode="json")
```

Generation keywords override explicitly supplied fields in a positional settings
model. `Settings` requires a model; `SettingsOverrides` can inherit it.
`set_settings(SettingsOverrides(...))` replaces explicit generation options
without clearing conversation history. Omitted fields return to inheritance.
Call it between generations, not during an in-flight mutation.

## Options

| Field | Default | Meaning |
| --- | --- | --- |
| `model` | Required after merging | Qualified provider/model reference |
| `credential_profile` | Active profile | Non-secret account selection |
| `temperature` | Unset | Number from 0 to 2 |
| `max_output_tokens` | Unset | Positive generation output limit |
| `reasoning` | Unset | Provider-supported reasoning settings |
| `reasoning.effort` | Unset | `none`, `minimal`, `low`, `medium`, `high`, `xhigh` |
| `reasoning.summary` | Unset | `auto`, `concise`, `detailed` |
| `reasoning.enabled` | Unset | Optional boolean |
| `reasoning.max_tokens` | Unset | Provider reasoning token budget |
| `timeout` | Unset | Total HTTP request timeout in seconds |
| `stream_read_timeout` | Unset | Socket-read timeout in seconds |
| `first_token_timeout` | 180 | Provider wait before meaningful output |
| `stream_stall_timeout` | 120 | Provider wait without further meaningful output |
| `max_rounds` | Unlimited | Positive tool-loop round limit |
| `retry` | Three retries | Transient-failure policy below |
| `compaction` | Unset for general LLMs | [Conversation compaction](compaction.md) |

Unknown settings are rejected. Provider support for reasoning and sampling options
depends on the selected model; it is not inferred from its name.

The built-in helper inherits the round limit unless
`components.workflows.helper.max_rounds` sets a positive limit. The coding agent
enables compaction by default for new executions; general LLMs and helpers opt in.

## Stalled calls and retries

Healthy streams have no total duration limit by default. Text, reasoning text, and
tool-argument changes count as progress; metadata and repeated snapshots do not.
Output deadlines must be positive finite seconds. Transport timeouts are optional
and can terminate an otherwise healthy long response.

Transient failures retry within the same recorded activity:

```toml
[components.llm.retry]
max_retries = 3
initial_delay = 1
max_delay = 30
```

The default delays are 1, 2, and 4 seconds. `max_retries` accepts 0 to 20; zero
disables retries. Delays accept finite values from 0 to 300 seconds, with
`max_delay >= initial_delay`.

Timeouts, connection failures, rate limits, and HTTP 408/429/5xx errors are
retryable. Authentication, TLS configuration, invalid responses, and permanent
provider errors are not. Context-length failures use the separate
[compaction recovery](compaction.md#overflow-and-failures) path.

Failed attempts do not update conversation state or usage totals, even if the
provider charges for them. Tool side effects are not automatically retried by this
policy. An interrupted generation starts a fresh bounded retry budget on recovery;
this is not an exactly-once provider guarantee. Cancellation propagates normally.

## Live changes and replay

Each generation and tool-loop round records a configuration snapshot. A runtime
override affects the next fresh round, not an in-flight request. The round limit
is fixed at the start of each `run()` invocation. Compaction and generation within
a round share settings.

`LLM.settings` returns a detached snapshot of the last resolved generation settings
and raises before the first resolution. Construction itself does not read configuration.
Workflow helpers can use `await llm.resolve_settings(...)` when they need a
recorded resolution outside `run()`, but not from inside an activity.

Replay uses recorded settings and results, not current credentials or model prices.
See [saved-execution compatibility](../reference/compatibility.md) before changing
workflow code or upgrading older histories.

## Related

- [Compaction](compaction.md)
- [Usage accounting](../reference/usage.md)
- [LLM metrics](../reference/llm-metrics.md)
- [Tool errors and approval](tool-approval.md)
