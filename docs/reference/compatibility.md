# Saved-execution compatibility

Resume replays recorded workflow operations. It is not a migration mechanism and
does not rerun completed failures with repaired settings. Before upgrading, back up
the stopped workspace's data directory and keep compatible workflow code available.

## Workflow and activity changes

Workflow `kind`, operation order, inputs, and explicit model options participate
in replay. Gate incompatible orchestration changes on the persisted `ctx.version`,
or start a new execution. A version bump does not rewrite history. See
[workflow contracts](../development/workflows.md#workflow-contracts) and
[activity contracts](../development/activities.md#payload-contracts).

## Older built-in executions

| Area | Compatibility behavior |
| --- | --- |
| Coding-agent settings | New agents use the `strong` LLM profile and accept no workflow inputs. Older version-1 sequences require their compatible implementation. |
| Model names | Use qualified `provider/model` names. Recorded settings with a separate `provider` field are not automatically migrated. |
| MCP collections | Version-2 agent executions retain their original static collections. New executions honor the configured static/dynamic mode. |
| Web fetching | Agent executions before version 4 retain earlier tool defaults. Completed calls replay saved results. |
| Default compaction | Agent version 5 enables it by default. Versions 1 through 4 retain opt-in behavior. |
| Measured compaction | `compaction_version=2` supports image-independent prefix selection. Marker 1 retains its image barrier; absent markers retain earlier defaults and operation sequence. |
| Character compaction | Policies with `max_context_characters` keep the original pre-request behavior. Change policy only for new executions. |
| Tool policy | Records without `tool_policy_version` keep binding-local approval. New workflows persist marker 1; children inherit it. |
| Steering | Executions without `inbox_version: 1` skip steering reads to preserve activity order. |
| Child cancellation | New instances store `child_cancellation_version=1`. Older decorated child calls retain wait-only cancellation. |
| Recorded configuration | Histories predating recorded configuration reads cannot gain those activities during replay. Start new executions. |

Compatibility notes describe retained behavior, not a promise that every private
development history can resume on every future release. Test important saved runs
before adopting an upgrade.

## Project skills

Project discovery now uses `.voc/skills/`. Move each skill directory there from
the former `.vocode/skills/`. Shared directories remain explicitly configurable.
Completed context activities replay their recorded skill listings; new context
activities discover the new location. Saved file-read results remain unchanged.

## Storage and control clients

Workspace coordination and permanent overrides live in `data/workspace.sqlite3`.
Old standalone workspace state and override files are not migrated automatically.
Execution databases, global credentials, and logs remain separate.

The control protocol uses version 2 bearer authentication. Version-1 handshake
packets are rejected; upgrade clients and daemons together. Global configuration
uses `~/.config/vops-runner`, not `~/.vectorops-runner`.

Usage totals retain previously recorded estimates. New contributions use finalized
provider usage; upgrading does not recalculate historical billing. See
[usage accounting](usage.md).
