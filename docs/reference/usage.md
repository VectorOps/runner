# Usage accounting

Usage statistics expose recorded LLM token counts and estimated USD cost independently of the displayed conversation.
Use a connected `RunnerClient` to read durable workspace counters or reset their
baseline. See [client setup](control-protocol.md#quick-start).

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
Token and call totals start at zero; cost uses the decimal-string format below.

## Scopes

- Runner counters persist in workspace storage across executions and restarts, including after execution history is removed.
- Reset zeros only runner counters. It preserves workflow statistics and step metrics. Requests still in flight contribute their finalized usage when it arrives after reset.
- Workflow `usage.own` contains that instance's tokens, calls, cost, and coverage. `usage.subtree` contains only cost and coverage, including descendants recursively. Parent cost includes child cost even before a join completes; token counts are not duplicated across the tree.
- Execution `usage.totals` counts each recorded LLM step once across all retained branches and child workflows. Do not sum workflow subtree totals together: that would count descendants multiple times.
- Execution `usage.root_context` describes the last measured generation request on the authoritative root path. It is not a sum of turns or a prediction of the next request. Child requests do not replace it.

## Current context size

Despite its historical name, `root_context.input_tokens_estimate` contains
finalized provider input usage, including cache-read and cache-write tokens.
`input_tokens_source` is `provider_usage`; `output_tokens_estimate` contains the
measured output of that request. `step_id` identifies the measurement. Before the
first measurement, `root_context` is null.

Starting another request does not replace the last measurement with an estimate.
The measurement does not include newly added user input or tool results. A newer,
smaller measurement is accepted, including after compaction. Changing branches
selects the last measurement on that branch, never an abandoned branch's value.
Character-based estimates remain in per-step observability metrics only; they are
not exposed as context measurements or included in cumulative usage.

## Accuracy and reset behavior

New contributions use only finalized provider usage for tokens and cost. Partial
snapshots and local estimates contribute no consumption. `cost_estimate_usd`
retains its decimal-string format and zero default. Zero alone does not establish
that all calls were free or priced.
`incomplete_calls` counts contributions with missing usage or pricing, or
unrecorded retry attempts. Finalization clears the corresponding pending coverage.
`estimated_calls` remains available for previously stored estimated contributions;
new contributions do not count estimates.
`input_tokens` is uncached input. `cache_read_tokens` and `cache_write_tokens` are
separate; sum these three fields when a cache-inclusive total is needed.
Reasoning tokens are reported separately and must not be added again to output.
Earlier retry attempt usage is not retained in the step snapshot. These totals are not a billing ledger.

Streaming updates replace earlier contributions. Replaying an existing step does not charge it again.
Reset snapshots the current durable contributions. An in-flight request has no confirmed consumption to subtract, so its full final measurement counts after reset. Reset does not clamp cumulative totals to a historical maximum.
Reset does not have an idempotency key; do not automatically retry it after an uncertain transport result, since another reset would move the baseline again.

Retained histories are reconciled once at service startup, or on first usage access/execution for a library runner. Later writes update counters incrementally; reads, resets, and new runs do not rescan historical executions. Restart reconciles any history committed before a workspace write failed. Work deleted before accounting was introduced cannot be reconstructed.

Existing stored aggregates and reset baselines are retained as recorded. There is
no usage schema migration, historical recalculation, or replacement workspace
ledger. The corrected calculation applies to new contributions; historical totals
can therefore still include estimates recorded by earlier versions.

## Related

- [Control protocol](control-protocol.md)
- [Per-step LLM metrics](llm-metrics.md)
- [Compaction](../config/compaction.md)