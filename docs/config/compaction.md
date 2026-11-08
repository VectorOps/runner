# Conversation compaction

Compaction summarizes older conversation history to make room for later work.
It preserves recent complete exchanges, but summary fidelity is best-effort.
It does not guarantee that the next request fits the provider's context window.

## Enable compaction

New built-in coding-agent executions enable compaction at an 80% threshold.
Helpers and general `llm.LLM` instances opt in. Configure a shared policy in TOML:

```toml
[components.llm.compaction]
trigger_threshold_ratio = 0.8
keep_recent_ratio = 0.25
summary_target_ratio = 0.1
```

An empty compaction table uses defaults. In Python, pass
`compaction=llm.Compaction()` to enable it or `compaction=None` to disable it.
`default_compaction=llm.Compaction()` supplies a fallback only when neither shared
settings nor the selected profile specifies a policy. Explicit null overrides
that fallback.

## Settings

| Field | Default | Meaning |
| --- | --- | --- |
| `trigger_threshold_ratio` | 0.8 | Fraction of the context window, greater than 0 and at most 1 |
| `context_window` | Model catalog | Optional positive token limit; unknown limits disable the normal trigger |
| `keep_recent_messages` | 8 | Minimum recent messages, expanded to keep complete tool exchanges |
| `keep_recent_tokens` | Unset | Positive recent-tail token target; takes precedence over the ratio |
| `keep_recent_ratio` | 0.25 | Recent-tail fraction; below the trigger when no absolute target is set |
| `summary_target_ratio` | 0.1 | Desired summary fraction, greater than 0 and below 1 |
| `summary_max_output_tokens` | Unset | Optional positive technical output cap |
| `summary_settings` | Generation settings | Partial model, reasoning, sampling, and timeout overrides |
| `additional_instructions` | Unset | Extra preservation guidance |
| `instruction` | Built-in instructions | Customization field; new workflows still include core preservation rules |

To use a separate summarizer:

```toml
[components.llm.compaction.summary_settings]
model = "openai/gpt-4.1-mini"
```

The summarizer must support the selected history's content types, including images.
`summary_max_output_tokens`, not `summary_settings.max_output_tokens`, controls
the explicit summary cap for new executions. The summary ratio is guidance, not
an API output limit.

## What is preserved

After a successful response, the runner compares finalized provider input usage
with the lower of the configured threshold and the context window minus the output
budget. It selects a measured prefix while keeping the latest response and recent
complete exchanges. If no suitable boundary exists, it skips compaction.

The summary request includes the selected messages, tool calls and results, errors,
and native image blocks. Reasoning blocks and opaque provider metadata are excluded
from the summary request, not deleted from original history. No tools execute as
part of summarization. Original system instructions and tool definitions are
reference material for the summarizer.

One rolling summary replaces the selected prefix. System instructions, current
tools, and the retained suffix remain unchanged. Historical target files are not
snapshotted or restored. Inspect the original history when exact details matter.

## Overflow and failures

On a context-length error, the runner can force one compaction and retry generation
once. Newly added user input and tool results are retained; tools do not rerun.
No eligible prefix, a failed summary, or another overflow surfaces an error.

Empty, tool-calling, truncated, over-cap, or failed summaries do not replace history.
For content whose size can be estimated, the summary must reduce the selected
prefix. Image-bearing estimates can be unknown; base64 length is not a token count.
A known oversized summarizer request raises `CompactionCapacityError` rather than
splitting the work into extra summarization calls.

## Replay

Completed summaries replay without contacting the provider or consulting today's
model catalog. Interrupted summaries can run again with their recorded settings.
Older executions keep their recorded compaction behavior, including earlier defaults
and image-selection rules. Start a new execution to adopt a new policy rather than
changing recorded activity order. See [compatibility](../reference/compatibility.md).

## Related

- [LLM configuration](llm.md)
- [Compaction diagnostics and metrics](../reference/llm-metrics.md)
