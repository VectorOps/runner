# Component configuration

Set user-level defaults for LLMs and reusable workflow components in
`~/.config/vops-runner/config.toml`. Override them per project in
`<project>/.voc/config.toml`; the
[workspace directory](../usage/cli.md#workspace-location) is configurable at startup.
Workflows read arbitrary nested values with `await get_config(ctx, name, default)`.
Each workflow read is an activity: replay returns its recorded value, while a new
read uses the workspace's shared in-memory configuration. The runner parses both
optional files once at startup. There is no central component schema or global digest.

## Quick start

Create `~/.config/vops-runner/config.toml` with:

```toml
[components.llm]
model = "openai/gpt-4.1"
```

This is enough for the built-in coding agent. Its `strong`, `medium`, and `weak`
profiles inherit these shared settings unless explicitly overridden; no profile
tables are required. Other LLM options use their built-in defaults.

Select models with one `<provider>/<model>` value. The provider prefix selects
both the generation backend and its credentials. A separate `provider` setting is
not supported. The model portion can contain additional slashes, for example
`openrouter/anthropic/claude-sonnet-4`. Credential profiles remain optional account
selectors, independent of model selection.

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

## User-level defaults and precedence

User defaults apply to every project opened with the same global configuration
directory. Select that directory in this order:

1. Explicit `--config-dir PATH` (or Python `config_dir=Path(...)`).
2. `VOPS_RUNNER_CONFIG_DIR`.
3. `~/.config/vops-runner`.

The runner reads `config.toml` in that directory, separately from `secrets.json`
and the authentication store. Only the top-level `components` table is accepted;
project-specific directory, environment, logging, retention, and worktree settings
remain in project configuration. For user-level MCP defaults, use
`[components.mcp]`, not the project-only `[mcp]` shorthand.

Configuration precedence, from lowest to highest, is:

1. Built-in defaults.
2. User component defaults.
3. Project component settings.
4. Workspace runtime overrides.

User and project tables merge recursively. Scalars and lists in project settings
replace user values, including `false`, `0`, and empty lists. An empty table does
not clear inherited keys. For example, a project can override only the strong
model while retaining the user's shared model and other profiles:

```toml
[components.llm.profiles.strong]
model = "openai/o3"
```

The LLM then applies its selected profile and explicit call options as described
below. A project shared model does not override a model in an inherited selected
profile; override that profile's model when needed.

Neither file is required or generated automatically. With user LLM defaults and
[provider credentials](authentication.md) configured, a fresh project needs no
local `config.toml`. Startup still creates `.voc` runtime directories. It does
not search parent directories for configuration.

Invalid user TOML or unsupported top-level settings fail startup. Component
schemas are validated by their consumers. User defaults are read-only at runtime:
configuration set/reset operations affect only the current workspace and never
write either TOML file. Restart to pick up file edits; recorded workflow reads
retain their original values during replay.

## Read a configuration value

```python
from vectorops.runner.api.configuration import get_config

async def main(ctx):
    model = await get_config(ctx, "llm.model", "fallback")
    language = await get_config(ctx, "report.language", "English")
    options = await get_config(ctx, "report", {})
    return {"model": model, "language": language, "options": options}
```

`ctx` is the `WorkflowExecutionContext` supplied to the workflow entry point.
The function submits one recorded activity through that context; it does not
look up ambient workflow context. `get_config` is the canonical workflow getter;
there is no ambient-context `configuration.get` adapter.

All configuration APIs use dotted string names. The canonical name
`components.llm.reasoning.effort` selects `[components.llm.reasoning]`'s `effort`
value; `llm.reasoning.effort` remains accepted as component-relative shorthand.
The configuration owner normalizes shorthand identically for workflow reads,
internal access, and control RPCs. Names must have nonempty segments. Within a
segment, `~1` represents a literal dot and `~0` a literal tilde; lists are accessed
as whole values rather than indexed. A missing file, missing
key, or non-mapping intermediate value returns the supplied default (None when
omitted). False, zero, empty lists, and empty mappings remain actual values.
Without `model=`, the getter returns the selected value as-is; it does not merge
mappings or infer types from the default. Pass a Pydantic model to validate and
normalize the selected value before persistence.

Getters access memory only. There is no environment interpolation or configuration
search beyond the selected user and project files. Selected values and defaults must be finite
JSON-compatible values. File parsing and I/O errors fail startup;
invalid selected values become recorded activity failures.

Without a model, the activity contract is `kind="configuration.get"`, `name="get"`, version 1,
with `name` and `default` inputs and the selected value as its result. Returned
containers are detached so caller mutation cannot alter recorded history.
Reads require workflow context and cannot be nested inside an activity.

Configuration values and defaults are persisted in saga history and can appear in
history exports and step events. Never read credentials or secrets through this
API. A subtree read records the entire selected subtree, not just fields used later.

## Internal access and startup

Each running workspace owns one `RunnerProject.configuration` instance, shared
by its workflows, MCP management, and tool approval. Internal getters are
synchronous and do not depend on workflow context or history:

```python
project = await runner.project()
language = project.configuration.get("report.language", "English")
```

`get_validated(name, Model)` returns an instance of the supplied Pydantic model.
Missing values validate as `{}`, allowing model defaults. Returned values are
detached from shared state. Workflow code instead uses the activity APIs above;
the activity machinery owns persistence and replay.

TOML is parsed only at runner startup, off the event loop. There is no file watcher,
polling, or runtime reload API. Getters never stat or read the file. TOML edits
take effect after restart; a missing file contributes no component settings.
Component-specific validation still happens at consumption. Runner-level directory,
logging, retention, and environment settings also require restart. Use runtime
overrides below to change component settings without restarting.

## Override component settings

Hosts can change workflow settings without editing TOML or restarting the worker:

```python
config = (await runner.project()).configuration
await config.set("llm.model", "openai/gpt-4.1", permanent=True)
await config.set("llm.model", "openai/o3")
current = config.inspect("llm.model")
await config.reset("llm.model")
```

Names use the same canonical dotted paths or component shorthand as reads.
These APIs cannot reconfigure runner-level
directories, logging, retention, or environment settings. Values must be finite
JSON-compatible data; component owners still validate their own schemas on read.

There is one override per key, not separate temporary and permanent layers.
The latest write replaces the previous override. In the example, the temporary
`openai/o3` override removes the persisted `openai/gpt-4.1` override; after restart the TOML value
applies. Temporary overrides ignore TOML edits and disappear at worker restart.
Setting or resetting a subtree also removes overlapping ancestor/descendant
overrides. Setting `llm.model` after overriding `llm` removes the whole `llm`
override; unrelated sibling keys remain unaffected.

Permanent overrides are stored in the `configuration` document in
`data/workspace.sqlite3`, following the configured `data_directory`.
Each override includes a fingerprint of the
merged user/project TOML value for its key. At startup,
a changed value, type, presence, or selected subtree invalidates that override.
Formatting-only edits and changes to unrelated keys do not. Invalidated overrides
are removed from disk and do not reappear if TOML later reverts. Invalid TOML edits
do not affect the running worker but fail the next startup. Do not store credentials in overrides or commit
this workspace-local state file as configuration.

Commit non-secret `config.toml` settings and workflow source; ignore the configured
data directory, which contains overrides, execution history, logs, and locks.
Workspace-root override files are no longer read or migrated. Changing
`data_directory` selects the overrides in that directory rather than moving them.

The authenticated control protocol exposes the same operations over embedded and
TCP connections:

```python
name = "components.llm.model"
await client.set_configuration(name, "openai/gpt-4.1", permanent=True)
current = await client.get_configuration(name)
snapshot = await client.get_configuration()
suggestions = await client.suggest_configuration(name, prefix="openai/")
await client.reset_configuration(name)
```

| RPC method | Parameters | Result |
| --- | --- | --- |
| `configuration.get` | Optional dotted string `name` (default `""`) | Canonical `name`, `exists`, `value` |
| `configuration.set` | String `name`, required `value`, optional `permanent` (false) | Null acknowledgment |
| `configuration.reset` | String `name` | Null acknowledgment |
| `configuration.suggest` | String `name`, optional `prefix` (empty string) | Advisory `values` array |

RPC targeting is identical to internal and workflow targeting: canonical names
start at the TOML document root, such as `components.llm.model`. Existing
`name: "tools.approval.whitelist"` requests select
`components.tools.approval.whitelist`; path arrays are not accepted. Omit `name`
or pass `""` to read
the complete effective snapshot, including read-only startup settings and their
runner defaults. `exists` distinguishes a missing key from an explicit null.
Consumer-specific defaults and resolved LLM profiles are not materialized.

Only `components` and its descendants are editable. Root replacement/reset and
edits to startup directories, logging, retention, environment, or other root keys
return `read_only`. Component values can be arbitrary finite JSON, including
invalid or incomplete settings; validation happens when consumers read them.
Reset removes runtime overrides and restores merged startup TOML, not necessarily shared
inheritance. If absent there, the consumer supplies its own default.

Model suggestions use qualified `<provider>/<model>` references, including for
LLM profiles, compaction summarizers, and workflow tier settings. Suggestions also
include enum values from known LLM
and built-in workflow schemas. Unknown paths return an empty inventory. Suggestions
are advisory and do not restrict writes or require valid current settings.

All clients of the same worker share these overrides. A successful write response
means new reads see the value immediately. Cached values, existing resources,
in-flight generations, and replay snapshots are unchanged; no resource or runner
is reinitialized. Requests are independent, without batches or revisions.
Use the running worker's API rather than a second configuration writer.

Workflow code continues to call `get_config(ctx, ...)`. New reads see the current
effective settings; replayed reads retain their recorded values. Updating a model
does not change an already-running generation's settings snapshot.

## Validated component reads and MCP

Owners that need validation before persistence can use
`await get_config(ctx, name, {}, model=Model)`. Validation happens inside the
activity, which records the model's normalized JSON result, not the raw selected
value. The return value is JSON-compatible data, not a model instance. Invalid
settings produce payload-free errors.

```python
from pydantic import BaseModel, Field
from vectorops.runner.api.configuration import get_config

class ReportSettings(BaseModel):
    max_sections: int = Field(default=5, ge=1)

async def main(ctx):
    return await get_config(ctx, "report", {}, model=ReportSettings)
```

Missing settings use the supplied default, which is then validated. Pass `{}` to
apply model field defaults; an omitted default remains `None` and must be accepted
by the model. An existing value is never merged with the fallback.

Model-backed reads retain the `configuration.validated` activity contract and
record a schema digest. `contract_digest=` can provide an owner-managed contract
identifier and requires `model=`. Nonempty or non-mapping fallbacks are fingerprinted
so changing them is detected during replay without persisting their raw payload.
It does not scan accepted fields for secrets; owners must design non-secret schemas.
Ordinary `get_config(ctx, name, default)` retains its schema-free behavior.

MCP settings use this path through root `[mcp.servers]`, normalized to the
legacy `[components.mcp.servers]` namespace. Do not configure both forms. See
[MCP servers and workflow tools](mcp.md) for stdio/HTTP configuration, credentials,
prompt/resource access, and `LLM.run(tool_set=...)` dynamic discovery. MCP selection
and discovery do not replace the tool approval policy below.

## LLM options

`LLM` accepts an optional `Settings` or `SettingsOverrides` positional argument,
plus generation-option keywords. Keywords override the positional model's
explicit fields; workspace values supply omitted fields.

The constructor does not read configuration. Each `generate()` and each tool-loop
round records one `get_config(ctx, "llm", {})` snapshot, then validates and
merges it with explicit overrides. Changes to model, thinking level, or other
runtime overrides take effect at the next fresh round, even on an existing LLM object.
Compaction decisions and generation within one round share that snapshot.
The round limit is resolved once at the start of `run()` and remains fixed for
that invocation; the next `run()` can adopt a changed limit.

LLM precedence is built-in defaults, workspace values, the selected profile,
then explicit options.
Its owner merges nested mappings recursively; scalars and lists replace lower
layers. Explicit `None` clears optional settings rather than inheriting them.
Overriding `model` replaces the complete provider/model selection.
Pydantic option models contribute only explicitly supplied fields. Pydantic here
validates LLM-specific options, not the shape of general workspace configuration.

```python
import connect
from vectorops.runner.api import llm

async def main(ctx):
    agent = llm.LLM(
        "reviewer",
        llm.SettingsOverrides(temperature=0),
        model="openai/o3",
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

`LLM.set_settings(SettingsOverrides(...))` replaces explicit generation options
without clearing messages, usage, or compaction history. Omitted fields revert to
profile and workspace inheritance rather than retaining old overrides. Profile
selection and explicit compaction and retry policies are preserved. Call it
between generations; overlapping mutations are rejected. `LLM.settings` still
reports the last resolved generation
until the next call.

The built-in chat workflow reads `workflows.defaults` directly with
`get_config(ctx, ..., model=Settings)` after each user message and applies the selected
tier with `set_settings`. Overrides to a tier model or shared thinking level take
effect on the next turn. Clearing those fields restores `components.llm` inheritance.
The coding agent always selects the `strong` LLM profile instead;
see [Named LLM profiles](#named-llm-profiles) below.
The helper reads shared defaults and `workflows.helper` directly for each task.
Prompt and tool selection remain invocation-scoped; no settings-loader wrappers
or legacy workflow-settings schema digests are used.

Workflow-side helpers can call `await llm.resolve_settings(...)` with the same optional
settings, `profile`, and generation keywords. This requires active workflow context and does
not work inside an activity; resolve before entering its recorded callback.

| Workspace field | Built-in default | Validation/meaning |
| --- | --- | --- |
| `model` | Required after merging | Model reference in `<provider>/<model>` form; custom injected backends can use their own identifiers |
| `credential_profile` | None (`default` profile) | Non-secret runner credential profile reference |
| `temperature` | None | Number from 0 through 2 |
| `max_output_tokens` | None | Positive integer |
| `timeout` | None | Optional positive total HTTP request timeout in seconds; can limit healthy streams |
| `stream_read_timeout` | None | Optional positive socket-read timeout in seconds; network traffic resets it |
| `first_token_timeout` | 180 | Positive finite seconds of provider waiting before first meaningful output |
| `stream_stall_timeout` | 120 | Positive finite seconds of provider waiting without further output after streaming starts |
| `reasoning` | None | Provider reasoning configuration |
| `reasoning.effort` | None | `none`, `minimal`, `low`, `medium`, `high`, `xhigh` |
| `reasoning.summary` | None | `auto`, `concise`, `detailed` |
| `reasoning.enabled` | None | Optional boolean |
| `reasoning.max_tokens` | None | Optional provider reasoning token budget |
| `max_rounds` | None (unlimited) | Optional positive integer; `run(max_rounds=...)` overrides it |
| `compaction` | None | Optional compaction policy below |

Unknown LLM and reasoning fields are rejected during resolution. No model,
temperature, or reasoning capability is inferred from a model name.

Omit `max_rounds` for unlimited rounds by default. The built-in helper also has no
round cap of its own: it inherits the LLM configuration unless
`components.workflows.helper.max_rounds` sets an explicit positive limit.

```toml
[components.llm.compaction]
trigger_threshold_ratio = 0.8
keep_recent_ratio = 0.25
summary_target_ratio = 0.1
```

### Named LLM profiles

Use `LLM("agent", profile="strong")` to select a partial configuration under
`components.llm.profiles`. Shared fields remain directly under `components.llm`:

```toml
[components.llm]
model = "openai/gpt-4.1"
max_output_tokens = 4096
reasoning = {effort = "high"}

[components.llm.profiles.strong]
model = "openai/o3"

[components.llm.profiles.weak]
model = "openai/gpt-4.1-mini"
reasoning = {effort = "low"}
```

Each profile can override generation settings, `compaction`, `retry`, and
`max_rounds`. Nested mappings merge recursively before the resolved configuration
is validated. The built-in `strong`, `medium`, and `weak` profiles inherit all shared
settings when omitted, just like an empty profile. Define only the profiles you
want to override; `medium` in the example uses the shared model and reasoning.
Unknown custom profile names,
non-mapping catalogs or profile entries, and invalid resolved settings fail before
generation. Profiles cannot contain another profile catalog or select a parent
profile. Without `profile=`, the LLM uses only shared settings and explicit options.

Profile selection is separate from `credential_profile`, which references runtime
credentials. A settings profile can select a credential profile by name, but must
not contain secrets. The entire `llm` subtree, including the catalog, is recorded
in the existing per-round configuration snapshot. Runtime overrides such as
`await config.set("llm.profiles.strong.model", "openai/o3", permanent=True)` use the same
storage and lifetime rules as other component settings. Fresh rounds see updates;
replayed or interrupted generations use their recorded snapshot.

### Migrate the coding agent

The coding agent selects `profile="strong"` and accepts no workflow input parameters.
Remove `model_tier` from invocation inputs. Configure only
`components.llm.model = "openai/gpt-4.1"` to use a shared model; no profile tables
are required. To override the coding agent's model, move
`workflows.defaults.strong_model` to `llm.profiles.strong.model`. Move its shared
`thinking_level` to `llm.reasoning.effort`, or set reasoning in individual profiles.
Update stored overrides to target the new keys as well. Chat and helper workflows
still use `workflows.defaults`; retain their settings when those workflows need them.

The agent no longer reads or translates workflow defaults after every user message.
Its LLM refreshes the selected profile before each generation, including tool rounds,
without clearing conversation history. Prompt and tool settings remain workflow-owned.

For legacy split settings, replace `model = "gpt-4.1"` and `provider = "openai"`
with `model = "openai/gpt-4.1"`, then remove `provider`. Apply this to profile and
summary settings as well. Separate provider settings, including explicit nulls,
are rejected rather than translated or ignored. Start new executions after this
migration: recorded configurations containing `provider` are no longer accepted,
and removing the field changes generation request digests and compaction signatures.
Existing histories are not rewritten or automatically migrated.

This changes the agent's recorded activity sequence, so `builtin.agent` now uses
workflow contract version 2. Start a new agent execution after migrating settings.
Version-1 histories require the previous implementation; they are not automatically
migrated and replay with the new contract is rejected. Profile-less LLM workflows
retain their existing configuration-read and generation contracts.

### Measured compaction

Compaction is opt-in. An empty compaction table or `llm.Compaction()` enables the
token policy. After a successful response with final provider usage, the runner
compares measured input tokens (including normalized cache categories) with the
configured fraction of the model context window. For new workflows, the default
is 80%. The trigger also respects the generation output reservation: it uses the
lower of the configured fraction and the context window minus the output budget.
It does not predict the size of the next user message or tool result; provider
overflow recovery handles those additions.

| Compaction field | Default | Meaning |
| --- | --- | --- |
| `trigger_threshold_ratio` | 0.8 | Trigger fraction, greater than 0 and at most 1 |
| `context_window` | Connect catalog | Optional positive override; unknown limits disable the normal trigger |
| `keep_recent_messages` | 8 | Minimum recent messages, expanded to preserve complete tool exchanges |
| `keep_recent_tokens` | None | Positive recent-tail token target; takes precedence over the ratio |
| `keep_recent_ratio` | 0.25 | Recent-tail target as a fraction of context; below the trigger when no absolute target is set |
| `summary_target_ratio` | 0.1 | Soft summary length target relative to the projected older conversation, greater than 0 and below 1 |
| `summary_max_output_tokens` | None | Optional positive technical output cap, independent of generation's output cap |
| `summary_settings` | Inherit generation | Partial generation settings, including qualified model, reasoning, temperature, and timeouts |
| `additional_instructions` | None | Optional preservation guidance appended to the built-in rules |
| `instruction` | Built-in continuation instructions | Existing customization field; new workflows always include the core preservation rules |

For a dedicated summarizer, add `[components.llm.compaction.summary_settings]`
with `model = "openai/gpt-4.1-mini"`. A qualified summary model replaces the
generation provider selection. Register its context and output limits in Connect
to check capacity before the call.
When using the generation model, an explicit `context_window` also applies to the
summarizer. Set `keep_recent_tokens = 16000` to retain a fixed token target rather
than a fraction. Both the message minimum and token target must be satisfied.
`summary_max_output_tokens` is authoritative; `summary_settings.max_output_tokens`
does not override it in new workflows. Compaction remains opt-in:

```python
conversation = llm.LLM(
    "worker",
    system_prompt="Complete the user's request without weakening its constraints.",
    compaction=llm.Compaction(
        trigger_threshold_ratio=0.8,
        keep_recent_messages=8,
        summary_target_ratio=0.1,
    ),
)
```

The successful response's activity ID anchors compaction. Input usage measures the
prefix before that response. The response itself, complete recent tool exchanges,
and everything appended after the anchor remain unchanged. Selection walks backward
through measured checkpoints and stops at the previous summary. Retention is a
target, not a hard cap: whole exchanges and measured boundaries can retain more.
If there is no eligible boundary that satisfies the target, compaction is skipped.
The latest response is always retained, including pending tool calls. Text
compaction does not consume images: selection stops before the first image so its
exchange remains intact. This can prevent further compaction; an image is never
silently replaced with a placeholder or serialized as base64 prose.

The summarizer receives readable message roles, text, tool calls, and results,
plus the original system instructions and tool definitions as reference context.
It has no executable tools. Opaque reasoning signatures, encrypted payloads,
provider metadata, usage counters, and transport identifiers are excluded from
summary input, not removed from original messages. Tool arguments and results
retain their business fields, even fields named `signature` or `usage`.

Built-in instructions prioritize user requests, constraints, corrections, and
unresolved questions. They preserve attribution and distinguish proposed work,
reported outcomes, and verified evidence. User inputs can be condensed, but exact
requirements and identifiers should survive. Summary fidelity is best-effort, not
a guarantee that every historical detail survives.

One rolling summary replaces the selected prefix as the first user message, with
a fixed envelope identifying generated historical context rather than a new user
request. The system prompt, current tools, and retained messages remain unchanged.
Recent readable messages are supplied as optional continuity context; this excerpt
is omitted when it does not fit or contains images. The actual retained suffix is
never truncated. Previous summaries are separate from newly evicted messages.

New compactions discard derived token coordinates after replacement and rebuild
them from subsequent provider measurements. Original usage is never rewritten.
Missing/partial usage and changed model/system/tool/reasoning signatures are not
bridged with character estimates.

On a provider context-length error, the runner forces the same operation at the
last measured successful response and retries generation once. New input and tool
results survive unchanged; tools are never reexecuted. No eligible prefix, an
already-compacted anchor, summary failure, or another generation overflow surfaces an error.
Recovery does not consume another tool-loop round. Transient retries remain bounded
within each generation or summary activity; there is no measurement roundtrip.

Each compaction sends one projected payload in one recorded summarization call.
`summary_target_ratio` supplies prompt guidance, not an API output limit. The
technical output limit uses the model's catalog ceiling and estimated remaining
context, optionally constrained by `summary_max_output_tokens`; it does not inherit
the generation's output cap or impose a fixed 4,096-token default. Unknown limits
are left to the provider. An input that exceeds the known summarizer window raises
`CompactionCapacityError`; a provider overflow surfaces without chunking or another
summary call. Neither failure replaces history.

Summary validation precedes history replacement. Empty, tool-calling, truncated,
over-cap, or failed summaries leave the active history intact. The final framed
summary must reduce the selected conversational prefix under consistent
before/after estimates; token counts from different models are not compared.
These checks are not proof that the rebuilt request fits the provider tokenizer.
The protected tail is never silently shrunk.

New workflows persist `compaction_version=1`. Generation results record the
output budget. Summaries use `llm.compact` activity version 3 with name `_compact`.
Completed summaries are reused without consulting the current catalog;
interrupted summaries restart under their existing identity. History is replaced
only after the summary succeeds. Request inputs remain digests, not transcripts.

Workflows created before this marker keep the previous measured flow, defaults
(70% trigger and no measured message-count floor), summary framing, and version-2
operation sequence. They do not automatically gain these changes on resume.
Explicit thresholds such as 80% remain explicit. Start a new workflow to use the
new flow; no history migration or automatic restart is performed.

Existing policies with `max_context_characters` retain version-1 pre-request
character triggering and `keep_recent_messages` behavior for replay compatibility.
They continue to use generation settings and do not gain overflow recovery.
Remove `max_context_characters` to opt into measured compaction for new executions;
do not change explicit policy or activity order while replaying existing history.

## Stalled LLM calls

Generation and compaction allow three minutes for the first meaningful output,
then two minutes without further output. Healthy streams have no total duration
limit by default. Configure these deadlines in `[components.llm]` or override them
with `LLM(..., first_token_timeout=180, stream_stall_timeout=120)`.
Both settings require positive finite seconds; zero and `None` are rejected.

Text, reasoning text, and tool-argument changes count as progress. Response metadata,
usage updates, empty text, and repeated snapshots do not reset the deadline.
A final-only response also succeeds without an earlier token. A provider that hangs
after its final response is still subject to the stall deadline while exhausting
the stream.

The runner enforces deadlines around provider iteration, including custom backends.
Snapshot persistence and downstream processing do not consume the waiting budget.
The Connect adapter excludes credential acquisition and transport authentication
resolve/refresh waits, including interactive host login. Custom providers that
perform interactive authentication can use `deadlines.suspend()` from
`vectorops.runner.api.llm` around that wait. This does not suspend explicitly
configured transport timeouts.

Expiry cancels and closes the attempt, emits `llm.request_timeout` with `phase`
(`first_token` or `stream_stall`) and `timeout_seconds`, and raises a retryable
`StreamTimeoutError`. Each retry starts with a fresh first-output budget; the retry
policy below still bounds the number of attempts, not the entire call's duration.
Cancellation remains cancellation rather than becoming a retryable timeout.
Providers must cooperate with asyncio cancellation; blocking event-loop code or
suppressed cancellation cannot be forcibly stopped by an asyncio deadline.

`timeout` and `stream_read_timeout` remain optional lower-level transport controls.
Leave `timeout` unset to avoid terminating a healthy long-running response.
Socket reads alone cannot detect a model stalled behind heartbeats; the output
deadlines operate independently of those reads.

## Transient provider retries

LLM generation and compaction retry transient provider failures within the same
recorded activity. Defaults allow three retries after the initial attempt, with
exponential delays of 1, 2, and 4 seconds, capped at 30 seconds.

```toml
[components.llm.retry]
max_retries = 3
initial_delay = 1
max_delay = 30
```

`max_retries` is an integer from 0 through 20; zero disables retries. Delays are
finite seconds from 0 through 300, and `max_delay` must be at least `initial_delay`.
Explicit `LLM(..., retry=llm.RetryPolicy(max_retries=5))` overrides supplied policy
fields. Workspace policy is read through the same recorded settings snapshot as
generation, so runtime overrides apply to fresh rounds rather than in-flight attempts.

Retryable failures include timeouts, connection resets/disconnects, truncated
transport payloads, Connect rate limits, transient provider errors, retryable
Connect errors, and HTTP 408/429/5xx errors. Authentication, certificate/TLS
configuration, context-length, permanent provider, and invalid response errors
are not retried by the transient retry loop. Token compaction separately handles
one context-length recovery as described above. Custom providers participate by raising the same exception types.
Direct `ConnectProvider.stream()` calls remain single-attempt; `LLM` owns retries.

Every attempt closes its stream, uses detached copies of the original request,
and retains the activity step ID (the Connect idempotency key). Before waiting for
a retry, an empty content snapshot replaces partial output. Only a validated final
response from an exhausted, successfully closed stream updates conversation state
and usage. Failed-attempt usage is not included, even if the provider charges it.

Cancellation propagates during streaming or backoff. Replay of terminal activities
does not call the provider or sleep. Restart of an unfinished activity starts a new
bounded retry budget with its recorded settings and existing identity. This is not
an exactly-once provider guarantee. Storage failures are outside the retry boundary.
Exhaustion propagates as the ordinary recorded `ActivityError`.

## LLM observability

Generation and compaction activities attach a versioned `metrics.llm` object to
their existing step snapshots. Live `step.updated`, `step.completed`, and
`step.failed` events carry the same metrics as persisted history. Inspect them
through `RunnerClient.history()` (`history.query`) or the existing JSONL export:

```bash
vops-runner --project ./workspace export execution-id --output execution.jsonl
jq -c 'select(.metrics.llm != null) | {id, kind: .operation.kind, metrics: .metrics.llm}' execution.jsonl
```

Metrics are separate from activity results and inputs. They contain numeric
observations, fixed labels, and resolved catalog model/provider names, not prompts,
tool arguments, transcripts, credential profiles, headers, URLs, or provider
metadata. The original response remains in the activity result (inside a checkpoint
envelope for token-mode generation); metrics do not add
another transcript. Generic activity metrics are a trusted implementation surface;
custom activity authors must not put secrets in them.

| Field | Meaning |
| --- | --- |
| `version` | Metric schema version, currently 1 |
| `catalog_model`, `catalog_provider` | Connect catalog identity; null for unknown or ambiguous models |
| `token_estimation` | `json_characters_div_4`: rounded-up compact JSON character count divided by four, not a tokenizer |
| `input_tokens_estimate` | Request estimate including message content, system prompt, tool schemas, and structural overhead |
| `output_tokens_estimate` | Current attempt's text, reasoning text, and tool-call content estimate |
| `context_window`, `model_max_output_tokens` | Limits from the resolved Connect model catalog, or null |
| `output_budget` | Minimum of configured output limit and catalog output limit when available; null if neither is known |
| `input_budget` | Context window minus output budget, clamped to zero; null if either limit is unknown |
| `context_remaining_estimate` | Context window minus estimated input, clamped to zero |
| `context_overflow_estimate` | Estimated input plus output reservation exceeding the context window, clamped to zero |
| `remaining_output_headroom_estimate` | Tightest known output/context capacity minus current output, clamped to zero; uses provider output tokens when usage is available |
| `provider_usage` | Allowlisted provider token counters and their `partial`/`final` completeness, or null when unavailable; never filled with local estimates |
| `cost_estimate_usd` | Decimal string using Connect catalog prices and accounting, or null when the model/pricing or a used token category's rate is unknown |
| `cost_basis` | `provider_usage` or `token_estimate`; null when no cost estimate is available |
| `attempt`, `retries` | One-based current attempt and preceding retries in this invocation |
| `compaction` | Final summary's before/after input token estimates and signed `saved_tokens_estimate`; null on ordinary generations |

Cost is always an estimate, even when calculated from final provider-reported
usage. It is not a provider invoice or a total billed execution cost. Partial
usage stays partial; usage with completeness `none` is unavailable. Missing usage
falls back to local input/output estimates when possible. Pricing uses Connect's
normalized input/output/cache categories; reasoning counters are not charged again
on top of output tokens. Subscription plans, custom routing/pricing, and provider
charges for earlier failed attempts are not accounted for. The requested catalog
model supplies pricing; custom providers must use a matching catalog identity for
meaningful estimates.

Generation estimates do not enforce context limits, change output settings, or trigger
compaction. The summarizer uses estimates to check its projected input and leave
output headroom. Provider framing, tokenization, and hidden reasoning can differ from
the heuristic. Images make input/output estimates for that content unknown rather
than treating base64 length as token usage. Headroom based only on an output cap is
an upper bound when context capacity is unknown. Null means unavailable, not zero.
Compaction savings compare the full continuation request before and after summary
replacement, including unchanged system/tools/recent messages. Legacy summaries
can report negative savings. New compactions reject a final summary that does not
reduce the selected conversational prefix. Ordinary generations have null
`compaction` metrics.

Each retry clears attempt-local usage, cost, and output estimates while retaining
the activity identity. The latest metrics remain attached on failure or
cancellation, even if no provider output arrived. Attempts count entries into the
provider attempt lifecycle, including authentication waits, not confirmed billable
requests. Unfinished recovery starts again at attempt 1 with its fresh bounded
retry budget. Terminal replay preserves stored metrics without consulting today's
catalog or recomputing prices. Old steps without metrics remain valid and are not
backfilled. Events are advisory; reconnecting hosts should query durable history.

## Tool approval

New workflows require host confirmation for tools unless their stable names are
whitelisted or approve-all is enabled in workspace `config.toml`:

```toml
[components.tools.approval]
approve_all = false
whitelist = ["read_file", "list_files"]
```

`approve_all` defaults to false and accepts only a boolean. `whitelist` defaults
to an empty list and accepts unique, nonempty, unpadded tool names. Names match
exactly; there are no wildcard or argument-pattern grants in configuration.
Set `approve_all = true` to bypass confirmation for every valid, permitted tool.
Unknown settings and invalid configuration fail closed as tool errors.

Argument validation always precedes authorization. Mandatory registered checks
can deny calls even when approve-all is enabled; otherwise workspace grants,
runtime name grants, and registered matchers can permit automatic execution.
Without a grant, the tool waits on the existing `tool.approval` mailbox channel.
`ToolApproval(channel=...)` still selects a custom channel, but its presence or
absence no longer determines authorization in new workflows.

Hosts use `mailbox.list`, `mailbox.get`, and `mailbox.respond`, supplemented by
`mailbox.requested` and `mailbox.answered` notifications. Request payloads retain
`instance`, `call`, `tool`, and `tool_version`. The JSON response contains strict
`approved: bool`, optional `reason: str`, and optional `scope: "call" | "workflow"`;
scope defaults to `call`, and unknown fields are rejected.
Rejection and malformed answers become tool errors, just like implementation
exceptions. Missing mailbox support and mailbox/storage failures propagate.

To approve the current call and future calls of the same tool in this workflow,
respond with `{"approved": true, "scope": "workflow"}`. Workflow scope requires
approval; it cannot express a persistent rejection. The grant matches the exact
tool name regardless of arguments, version, or schema, and applies only to the
request's workflow instance within its execution. Children, siblings, and other
root executions do not inherit it. Hosts identify the scope through the request's
`execution_id` and `workflow_id`; they cannot redirect a grant by supplying a tool
name or workflow ID in the response. `mailbox.list` supports workflow filtering.

The winning answer and grant commit atomically in the execution database, so grants
survive disconnects, crashes, and resume, even if the approved call subsequently
fails. Grants also remain in force on continuations of the same workflow instance;
they are not branch-local and have no revocation API yet. History cleanup removes
them with the execution. This stores an explicit host permission, not a policy
evaluation or activity. Mandatory checks still run before future granted calls.
Existing pending confirmations must be answered individually and are not bulk-approved.
Legacy binding-local workflows retain single-call behavior and do not create grants.

The gate executes inside `llm.tool` version 3. There is no policy activity,
configuration-read activity, or recorded policy decision. Completed tool outcomes,
including rejections, replay without validation, policy evaluation, or invocation.
Fresh or interrupted calls without an existing confirmation request read the shared
in-memory settings. Existing requests and accepted host answers remain
durable mailbox records attached to the tool step and are reused on recovery;
enabling approve-all does not bypass an outstanding confirmation or rejection.
An interrupted tool can execute again after approval: this is not an exactly-once
side-effect guarantee, and implementations retain responsibility for idempotency.

`RunnerProject.tool_policy` owns an isolated `ToolPolicy` registry. Trusted Python
code can use `allow(name)`, `remove(name)`, `register_check(name, callback)`, and
`register_matcher(name, callback)` without changing tool bindings. Async checks
return a denial reason or None; async matchers return whether to grant approval.
Each receives a detached `ToolCall` with name, version, arguments, definition
digest, and execution/workflow/step identities. Checks run before grants;
exceptions fail closed. Configure extensions before calls begin. Runtime grants
are shared with children of that project attachment, not persisted or shared
between independently opened projects. Workspace grants apply to every execution
using that workspace. Neither mechanism sandboxes trusted workflow Python code.

Existing workflow records without `tool_policy_version` retain the old binding-local
approval and activity sequence. New workflows persist format marker 1; children
inherit their parent's format. This marker selects compatibility behavior, not
a policy snapshot. Start a new execution to adopt centralized approval for a
legacy workflow. No history is silently rewritten.

## Tool error responses

By default, tool validation and implementation exceptions become recorded tool
responses with `is_error=True` and the exact `str(exception)` text. The LLM receives
these alongside successful tool results in original call order and can recover or
choose another action. Tool implementations are not automatically retried: they
can have non-idempotent side effects. Unknown tools, rejected approval, and malformed
approval responses also return errors without executing the tool.

```python
from vectorops.runner.api import llm

async def main(ctx):
    agent = llm.LLM("assistant", raise_tool_errors=True)
    agent.set_raise_tool_errors(False)
    agent.add_user_message("Continue despite tool failures.")
    return (await agent.run()).model_dump(mode="json")
```

`raise_tool_errors=True` (or `set_raise_tool_errors(True)`) opts into exception
handling for tool exceptions, including explicit `ToolExecutionError`. Exceptions
cross the recorded activity boundary as `ActivityError`, with their original type
and message retained. Parallel calls settle before propagation; no partial round
is appended. Standalone `invoke_tool(..., raise_errors=True)` offers the same mode.
Explicitly returned `ToolResponse(is_error=True)` remains a response in either mode.
Changing strict mode for an already recorded tool invocation causes a replay mismatch.

Cancellation, replay mismatches, history/storage failures, and failures obtaining
approval from the mailbox remain runner errors, not model-visible tool errors.
Exception messages sent to the LLM are also stored in history: tool authors must
avoid putting credentials or other sensitive data in exception messages.

Retries emit `llm.request_retry` warnings; terminal provider failures and tool errors
emit `llm.request_failed`, `llm.tool_failed`, or `llm.tool_error` errors. Structured
logs include correlation IDs, attempt/delay fields, and exception type/frame
metadata without raw exception messages, request contents, or credentials.

## Reusable workflow components

```python
from vectorops.runner.api.configuration import get_config

async def main(ctx):
    count = await get_config(ctx, "report.max_sections", 5)
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
file. New reads use current in-memory settings. For example, a workflow can read model A,
wait for host input, then read model B after a runtime override. On restart it
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

Recorded configuration activities retain the caller's original string name.
Existing component-relative reads therefore replay unchanged. Use canonical names
for new code, but do not rewrite names in workflows being resumed from history.

LLM histories created before recorded configuration reads were introduced lack
these new activities. Start new executions when adopting this version for those
workflows; inserting the reads is not an automatic history migration. Runner
directory, logging, and retention settings still use their existing startup rules.

## Related

- [MCP servers and workflow tools](mcp.md)
- [Provider authentication and global secrets](authentication.md)
- [Workflow authoring](../development/workflows.md)
- [Structured logging](logging.md)