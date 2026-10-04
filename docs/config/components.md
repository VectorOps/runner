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
generation, so edits apply to fresh rounds rather than in-flight attempts.

Retryable failures include timeouts, connection resets/disconnects, truncated
transport payloads, Connect rate limits, transient provider errors, retryable
Connect errors, and HTTP 408/429/5xx errors. Authentication, certificate/TLS
configuration, context-length, permanent provider, and invalid response errors
are not retried. Custom providers participate by raising the same exception types.
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
`approved: bool` and optional `reason: str`; unknown fields are rejected.
Rejection and malformed answers become tool errors, just like implementation
exceptions. Missing mailbox support and mailbox/storage failures propagate.

The gate executes inside `llm.tool` version 3. There is no policy activity,
configuration-read activity, or recorded policy decision. Completed tool outcomes,
including rejections, replay without validation, policy evaluation, or invocation.
Fresh or interrupted calls without an existing confirmation request read current
settings off the event loop. Existing requests and accepted host answers remain
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