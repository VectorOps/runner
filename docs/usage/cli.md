# Workflow CLI

Use `vops-runner` to start workflows, resume interrupted executions, inspect persisted progress, and export history for external analysis.

## Quick start

Create `workflows/example.py` in your workspace:

```python
async def workflow(ctx, value: int = 0) -> int:
    return value + 1
```

Run it with JSON keyword arguments:

```bash
uv run vops-runner start example:workflow --input '{"value": 4}' --execution-id example-run
uv run vops-runner status
uv run vops-runner export --output example-run.jsonl
```

Start prints a JSON object containing `execution_id` and `result`. It runs in the foreground. Errors go to stderr with exit code 1, argument errors use exit code 2, and Ctrl-C uses exit code 130.

## Start and resume

```bash
uv run vops-runner --project /path/to/workspace start example:workflow
uv run vops-runner --project /path/to/workspace resume example-run
```

`--project` precedes the command and defaults to the current directory. Workspace paths follow `config.toml`, including workflow and data directory overrides. `start` takes an explicit `path:callable` reference relative to the workflow directory, without `.py`, or `python:package.module:callable` for a library workflow. There is no callable default or `--callable` option. Input defaults to `{}`, and execution IDs are generated unless `--execution-id` is supplied. Existing execution IDs are never overwritten.

`resume [EXECUTION_ID]` uses persisted configuration, input, and selected root and child heads. Omitting the ID selects the last execution. Completed operations replay their stored outcomes; interrupted activities restart with their existing step identity. Activity implementations remain responsible for external side-effect idempotency. Resuming a completed execution replays its result. Stop the original writer before resuming; the project permits only one writer.

This foreground CLI owns the workspace writer lock. For concurrent independent executions in one workspace, run a [daemon](daemon.md) and use its [execution-oriented control API](../reference/control-protocol.md#concurrent-execution-lifecycle) instead of launching competing foreground writers.

## Inspect status and statistics

```bash
uv run vops-runner status example-run
```

`status [EXECUTION_ID]` emits JSON with `execution` metadata and `stats`: workflow count, step count, and counts by operation and outcome kind. Counts include child workflows and all retained branches, not only selected paths. Without an ID it selects the last execution.

Status and export read a consistent committed SQLite snapshot without taking the writer lock, so they work from another terminal while a workflow runs. Buffered streaming snapshots appear only after persistence. Lifecycle status is persisted: `running` can also indicate an execution interrupted by a crash, not proof of a live process. Checkpoint timestamps reflect persisted checkpoints, not the inspection time. Activity timing, token usage, and costs are not recorded or inferred.

## Export JSONL

```bash
uv run vops-runner export example-run --output analysis.jsonl
```

`export [EXECUTION_ID] --output FILE` requires a new output file and an existing parent directory. It refuses to overwrite any existing file. Without an ID it selects the last execution.

The UTF-8 file contains:

- One header entry with `kind: "execution.header"`, `version: 1`, `execution` metadata, and a `workflows` array containing every stored workflow configuration.
- One normalized workflow step per following line, preserving its `id`, `workflow_id`, `previous_step_id`, `operation`, `outcome`, and `deferred_delivery` fields.

Steps follow insertion order across the entire execution, including children and retained branches. Replacement snapshots export only the latest committed value for each step. Use predecessor links to reconstruct branches and `execution.checkpoint.heads` to identify selected paths. The entire export uses one committed snapshot; it is an analysis format, not an import or restore interface. Inputs, results, and error messages can contain sensitive data; protect the exported file accordingly.

## Related

- [Workflow authoring and replay](../development/workflows.md)