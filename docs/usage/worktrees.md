# Git worktrees

Use managed worktrees to run workflows against separate Git checkouts without
changing the workspace's default target. Humans and LLMs use the same workspace
manager; provenance differs, but permissions and retention rules do not.

## Create and use a worktree

The configured `environment.base_project_path` must point into a Git repository
with a commit. Worktree APIs fail if it does not. Worktrees are stored inside the
runner workspace, normally `<project>/.voc/worktrees/<uuid>/`, even when the
configured repository is elsewhere.

With a connected `RunnerClient`:

```python
from vectorops.runner.control import protocol
from vectorops.runner.worktrees import models

worktree = await connection.create_worktree(
    models.CreateRequest(name="Parser cleanup", idempotency_key="parser-cleanup-1")
)
execution = await connection.start(
    protocol.Start(
        reference="review:main",
        input={},
        idempotency_key="parser-review-1",
        worktree_id=worktree.id,
    )
)
```

The UUID is the stable identity in SQLite, paths, and requests. Names are descriptive
labels and need not be unique. An optional `branch` creates a new named branch;
omitting it generates `runner/worktree/<uuid>`. Existing branches are not attached
by this API. `base_ref` defaults to `HEAD` of the configured repository and is
resolved to a commit before creation. Uncommitted changes are not copied.

## Limits and backend configuration

These are startup settings in the runner workspace's `config.toml`:

```toml
[worktrees]
max_worktrees = 20
git_executable = "git"
timeout_seconds = 120
```

The limit counts all non-deleted reservations, including incomplete and failed
operations. At capacity, creation fails without deleting another worktree. Set
`max_worktrees = 0` to explicitly disable the count limit. This is not a byte quota:
build output in a single checkout can still exhaust disk space.

The POSIX Git adapter uses asynchronous subprocess argument lists, bounded output,
and per-command timeouts; it never constructs a shell command. Configure a different
Git executable if needed. Custom integrations can implement `WorktreeBackend` from
`vectorops.runner.worktrees.backend` and inject it into `Manager` or the
`RunnerProject.worktree_backend` constructor field. Reusable models do not invoke Git.

## Workflow and exec binding

Python root execution accepts `runner.run(reference, worktree_id=uuid)`.
`worktree_id` and `base_project_path` are mutually exclusive. The selected UUID and
resolved path are persisted in the execution target; resume never substitutes a
different checkout. A missing or deleted target fails attachment.

A raw `base_project_path` inside a registered worktree also acquires that worktree's
usage lease. Selecting a subdirectory preserves that subdirectory as the target;
the lease protects the whole checkout from deletion.

For child workflows:

```python
handle = await ctx.spawn_workflow("review:main", worktree_id=worktree_id)
outcomes = await ctx.join_workflows([handle])
```

The LLM `run_agent`/`subworkflow` tool accepts the same optional `worktree_id`.
Selection is part of the recorded spawn contract. Children without an override
inherit their immediate parent's effective target. A child selecting another target
gets a separate attachment and cleanup registry; its completion runs its cleanup
callbacks and closes that attachment's processes but does not delete the worktree.
Completed child outcomes can replay without reopening
their checkout.

`exec` also accepts `worktree_id`. It overrides the target for that command only;
`cwd` stays relative to the selected worktree. `poll_process`, `write_stdin`, and
`kill_process` route by the returned process ID. Completed output stays available
under the normal process retention rules. The override does not change file tools
or prompts in the calling workflow; bind a child workflow to change those together.

`environment.processes.max_sessions` is shared across the root execution's default
target, child attachments, and exec overrides. Starting, running, and retained
sessions count toward the limit. Capacity is freed when a session is consumed,
expires, or its process manager closes, not merely when the process exits.

Bindings keep existing approval policies. Sharing a worktree does not serialize
writers, and a checkout directory is not a sandbox.

## Opt-in LLM tools

The factories `worktree_create_tool`, `worktree_list_tool`, and
`worktree_delete_tool` are available through `vectorops.runner.api.tools`. Their
registered collection names are `worktree_create`, `worktree_list`, and
`worktree_delete`. None is included in the default helper or agent collections.

An explicit collection can select them:

```toml
[components.tools.collections.isolated]
tools = ["worktree_create", "worktree_list", "worktree_delete", "exec", "poll_process"]
```

Load that collection with `await tools.load_collection("isolated")` when building
a workflow's bindings. Existing tool approval rules still apply. The create tool
requires `name`; provenance and retry identity come from activity context, not
LLM-supplied execution IDs. A retry of the same activity cannot create a duplicate.

## CRUD protocol

| Method | Parameters | Result |
| --- | --- | --- |
| `worktrees.create` | Required `name`, `idempotency_key`; optional `description`, `branch`, `base_ref` (default `HEAD`) | Worktree record |
| `worktrees.list` | Optional `after` UUID, `limit` (default 100, maximum 500), `include_deleted` (false), `refresh` (false) | `worktrees`, `next_after` |
| `worktrees.get` | `worktree_id`, optional `refresh` (false) | Worktree record |
| `worktrees.update` | `worktree_id`, required `name`, optional `description` | Updated record; no path/branch changes |
| `worktrees.delete` | `worktree_id`, optional `force` (false) | Deleted record |

Records include repository identity, path, branch, resolved base commit, origin
(`user` or `workflow` with execution/workflow/step IDs), lifecycle status,
`created_at`, `updated_at`, `last_used_at`, cached inspection, and last error.
`updated_at` describes manager metadata changes, not arbitrary filesystem edits.
Inspection contains HEAD, current branch, dirty status, and `inspected_at`.
Use `refresh` for a fresh Git observation; it is not a filesystem snapshot.

Creation receipts are durable. Reusing a key with different arguments fails;
retrying a deleted reservation returns its tombstone rather than recreating it.
Use a new key for an intentional new worktree. Domain failures use the control
error code `worktree_error`; malformed parameters use `invalid_argument`.

## Retention, deletion, and recovery

Worktrees survive workflow success, failure, cancellation, and service restart.
Keeping one means not deleting it; a later workflow can reuse any ready record,
including one created by an LLM. Humans can manage all workspace records. There
are no ownership-based permission restrictions or automatic age-based eviction.

Deletion retains the Git branch and a database tombstone. It refuses live usage
leases even with `force`. Dirty, untracked, or ignored files require `force`, which
discards those files. External editors/processes are not tracked by the harness;
operators must coordinate them separately.

Git and SQLite are not one transaction. The manager reserves `creating`/`deleting`
state before effects. Retrying an interrupted operation reconciles the same UUID
and path rather than allocating another checkout. Unregistered directories are
never recursively deleted as a fallback.

Usage leases combine SQLite records with OS file locks. After a crash or unconfirmed
process cleanup, an abandoned lease blocks normal deletion. Inspect and stop any
orphaned processes before using force to acknowledge that lease. The harness does
not recover process handles after a crash. Worktree creation does not implement
filesystem snapshots, automatic merges, cherry-picks, or branch deletion.

## Related

- [Control protocol](../reference/control-protocol.md)
- [Target environments](../development/target-environments.md)
