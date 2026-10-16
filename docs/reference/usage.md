# Usage accounting

Usage statistics expose recorded LLM token counts and estimated USD cost independently of the displayed conversation.
Use the control client to read durable workspace counters or reset their baseline.

```python
totals = await client.runner_usage()
summary = await client.execution_summary(execution_id)
workflow = await client.workflow_state(execution_id, workflow_id)
reset = await client.reset_runner_usage()
```

## Control methods

| Method | Parameters | Result |
| --- | --- | --- |
| `runner.usage` | `{}` | `RunnerUsage` |
| `runner.usage.reset` | `{}` | Zeroed `RunnerUsage` with a new `generation` and `reset_at` |
| `workflows.state` | `execution_id`, `workflow_id` | Persisted workflow, including `usage` |
| `executions.summary` | `execution_id` | Execution summary, including optional `usage` |

Execution summary lists expose the same usage contract. Starting summaries can have `usage: null`.
New workflow usage fields are optional for compatibility with older records.

## Scopes

- Runner counters persist in workspace storage across executions and restarts, including after execution history is removed.
- Reset zeros only runner counters. It preserves workflow statistics and step metrics. Subsequent streaming snapshots contribute changes since the reset baseline, not their entire pre-reset usage.
- Workflow `usage.own` contains that instance's tokens, calls, cost, and coverage. `usage.subtree` contains only cost and coverage, including descendants recursively. Parent cost includes child cost even before a join completes; token counts are not duplicated across the tree.
- Execution `usage.totals` counts each recorded LLM step once across all retained branches and child workflows. Do not sum workflow subtree totals together: that would count descendants multiple times.
- Execution `usage.root_context` describes the latest generation request on the authoritative root path. It is not a sum of turns or a prediction of the next request. Child requests do not replace it.

## Current context size

`root_context.input_tokens_estimate` retains its wire name but uses final provider
input usage when available, including cache-read and cache-write tokens. These
normalized cache categories occupy context even though totals report them
separately. `input_tokens_source` distinguishes `provider_usage` from
`token_estimate`; null means the count is unknown. Final measured usage takes
precedence even when a text estimate exists. Remaining context and output
headroom are projected consistently from the measured count.

Without final usage, the local text estimate remains the fallback. Requests with
images have no local estimate and remain unknown until final usage arrives.
An in-flight request never borrows the previous request's count. This projection
also applies to existing stored histories without rewriting their metrics,
provider usage, replay outcomes, or usage totals.

## Accuracy and reset behavior

Provider token usage takes precedence over fallback estimates. USD costs reuse the per-step estimate and serialize as decimal strings.
`estimated_calls` counts contributions using fallback token or cost estimates. `incomplete_calls` counts contributions with missing values, partial provider usage, or unrecorded retry attempts. Both default to zero and are replaced along with streaming metrics, so a later complete response clears its incomplete contribution. A zero known sum does not prove free usage.
Reasoning and cache token fields are reported separately and are not added to input/output counts.
Earlier retry attempt usage is not retained in the step snapshot. These totals are not a billing ledger.
Earlier detailed coverage counters are accepted and folded conservatively into these two counters; migrated coverage can overcount, while token and USD sums are preserved.

Streaming updates replace earlier contributions. Replaying an existing step does not charge it again.
Reset snapshots the current durable contributions, so cached, not-yet-persisted updates count when committed. Downward corrections cannot make reset counters negative.
Reset does not have an idempotency key; do not automatically retry it after an uncertain transport result, since another reset would move the baseline again.

Retained histories are reconciled once at service startup, or on first usage access/execution for a library runner. Later writes update counters incrementally; reads, resets, and new runs do not rescan historical executions. Restart reconciles any history committed before a workspace write failed. Work deleted before accounting was introduced cannot be reconstructed.

## Related

- [Control protocol](control-protocol.md)