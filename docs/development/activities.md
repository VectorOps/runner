# Activity execution context

Activity implementations receive a runtime context as their first positional parameter.
Use its stable step identity when your external service supports idempotency keys.

## Ordinary activities

```python
from vectorops.runner.api.composition import activity
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.context import WorkflowExecutionContext


@activity()
async def prepare(ctx: ActivityExecutionContext, value: str) -> dict[str, str]:
    return {"value": value, "operation_id": ctx.step_id}


async def workflow(ctx: WorkflowExecutionContext) -> dict[str, str]:
    return await prepare("example")
```

The caller supplies only business arguments.
The executor injects the context, and recorded inputs exclude it.
Implementations without a first positional context parameter are rejected.

## Context fields

| Field | Meaning |
|---|---|
| `execution_id` | Persistent execution identity, or `None` when using an injected history without execution metadata. |
| `workflow_id` | Identity of the owning root or child workflow invocation. |
| `step_id` | Stable identity of this operation attempt. |

Root and child activities share the persistent execution identity but have distinct workflow and step identities.
The activity context does not expose workflow scheduling or history mutation APIs.

## Interruption and idempotency

The executor persists an ordinary activity's `pending` step before invoking it.
Success or failure updates that same step and is persisted before returning the outcome.
If execution is interrupted, recovery invokes the activity again with the same `step_id`.
Completed replay returns the recorded outcome without invoking the implementation.
New operations created after a fork receive new IDs.

An activity implementation can pass `ctx.step_id` as an external idempotency key.
The implementation owns deduplication or reconciliation; the runner does not guarantee exactly-once external effects.
Initial identities and terminal outcomes write through immediately, even when streaming snapshots are buffered.

## Streaming activities

```python
from collections.abc import AsyncGenerator

from vectorops.runner.api import composition
from vectorops.runner.runner.activity_context import ActivityExecutionContext


@composition.streamable_activity()
async def build(
    ctx: ActivityExecutionContext, value: str
) -> AsyncGenerator[composition.ActivityDelta | composition.ActivityFinal[str], None]:
    yield composition.delta({"operation_id": ctx.step_id, "state": "building"})
    yield composition.final(value)
```

Streaming implementations receive the same explicit context contract.
All snapshots retain one step identity, and interrupted recovery restarts the generator with that identity.