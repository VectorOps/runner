# LLM metrics

Generation and compaction steps expose `metrics.llm` in history and step events.
Use these fields to diagnose limits, retries, and estimated cost. Use
[usage accounting](usage.md) for durable aggregate consumption, not sums of
streaming snapshots.

## Inspect metrics

Export an execution from the runner checkout:

```bash
uv run vops-runner export EXECUTION_ID --output execution.jsonl
jq -c 'select(.metrics.llm != null) | {id, kind: .operation.kind, metrics: .metrics.llm}' execution.jsonl
```

The `jq` command requires [jq](https://jqlang.github.io/jq/). Python integrations
can use `RunnerClient.history()` instead. Step events carry the same metrics as
saved snapshots, but events are advisory; refresh history after reconnecting.

## Generation and compaction fields

| Field | Meaning |
| --- | --- |
| `version` | Metrics schema version, currently 1 |
| `catalog_model`, `catalog_provider` | Catalog identity, or null when unknown or ambiguous |
| `token_estimation` | `json_characters_div_4`, a heuristic rather than a tokenizer |
| `input_tokens_estimate` | Request estimate including messages, instructions, and tools |
| `output_tokens_estimate` | Current attempt's text, reasoning, and tool-call estimate |
| `context_window`, `model_max_output_tokens` | Catalog limits, or null |
| `output_budget` | Minimum of configured and known catalog output limits |
| `input_budget` | Context window minus output budget, clamped to zero |
| `context_remaining_estimate` | Context window minus estimated input, clamped to zero |
| `context_overflow_estimate` | Estimated input plus reserved output exceeding the window |
| `remaining_output_headroom_estimate` | Tightest known context/output capacity remaining |
| `provider_usage` | Provider token counters and completeness, never filled from estimates |
| `cost_estimate_usd` | Decimal-string estimate, or null when required pricing is unknown |
| `cost_basis` | `provider_usage`, `token_estimate`, or null |
| `attempt`, `retries` | One-based attempt and preceding retry count for this invocation |
| `compaction` | Before/after estimates and signed `saved_tokens_estimate`; null for generation |

Null means unavailable, not zero. Image content can make size estimates unknown;
base64 length is not token usage. Headroom based only on an output cap is an upper
bound when context capacity is unknown. Generation estimates do not enforce context
limits or trigger compaction. Summarization uses estimates to check input capacity.

Cost is not an invoice. It excludes subscription terms, custom pricing, and earlier
failed attempts. Provider usage can be partial; local estimates are clearly separate.
Reasoning counters are not charged again on top of output tokens. Custom providers
need a matching catalog identity for meaningful price estimates.

## Compaction diagnostics

`operation.input.compaction` is recorded before the summary call, so it can be
available even when that call fails:

| Field | Meaning |
| --- | --- |
| `anchor_id`, `trigger` | Successful generation anchoring the cut; trigger is `threshold` or `overflow` |
| `measured_prefix_tokens` | Original checkpoint measurement, not projected summary-request size |
| `selected_messages`, `retained_messages`, `packet_messages` | Prefix, suffix, and summarizer message counts |
| `image_blocks`, `tool_calls`, `reasoning_blocks_removed` | Selected content counts |
| `source_tokens_estimate`, `packet_tokens_estimate` | History and full request estimates; null with images |
| `summary_target_ratio`, `summary_target_tokens` | Soft compression target; token count can be null |

These diagnostics omit message content, tool arguments, paths, image bytes, and
opaque provider data. They describe an attempted compaction, not a skipped check.

## Retry and replay behavior

Retries clear attempt-local usage, cost, and output estimates. Failed or cancelled
steps retain their latest metrics. Attempts include authentication waits, not just
confirmed billable requests. Unfinished recovery restarts at attempt 1 with a fresh
bounded retry budget. Completed replay preserves saved metrics without applying
current prices. Older steps are not backfilled.

Metrics do not add another transcript, but original activity inputs and results
can contain sensitive content. Custom activity authors must keep credentials and
private payloads out of diagnostic fields.
