# Activity execution context

Use activities to record external work such as a network request, filesystem access,
or a timestamp. Implementations receive a runtime context as their first parameter.
Completed replay returns the saved result; interrupted work can run again.

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

## Payload contracts

Both `activity` and `streamable_activity` accept `kind`, `version`, and `namespace`.
The operation's `kind` selects its input, streaming state, and final result contract.
Its positive integer `version` defaults to `1` and is recorded before execution, including failed operations.
The `name` identifies the implementation, not the payload schema or invocation.

```python
@activity(kind="customer.acme.prepare", version=2)
async def prepare(ctx: ActivityExecutionContext, value: str) -> dict[str, str]:
    return {"value": value}
```

Use explicit stable kinds for published contracts; internal code changes do not require a contract version change.
Alternatively, `namespace="customer.acme"` generates `customer.acme.<module>.<callable>`.
Workspace module names use the path relative to the workflows directory, without `.py`, with dots replacing path separators.
Generated kinds require module-level callables and remain stable across loader instances, but change when files or functions are renamed.
Explicit `kind` and `namespace` are mutually exclusive.
Namespace components must be identifiers separated by dots.
Namespace owners allocate contract names; generation alone cannot guarantee uniqueness across organizations.
Use your organization's namespace for custom contracts; do not rename runner-owned
kinds. Without either option, the generic kind is `activity`.
Older operation records without a version load as version `1`.
Activity replay matches kind, name, and input; operation versions are informational
and do not reject incompatible recorded payloads. Kinds beginning with `llm.` also
ignore input during replay matching. Preserve compatibility with old results or
use a workflow-version-gated legacy branch and a distinct new operation identity.
Reusing a completed child result does not execute or validate that child's internal activities.

## Context fields

| Field | Meaning |
|---|---|
| `execution_id` | Persistent execution identity, or `None` when using an injected history without execution metadata. |
| `workflow_id` | Identity of the owning root or child workflow invocation. |
| `step_id` | Stable identity of this operation attempt. |

Activities also receive optional runtime services, including the local environment,
mailbox, and cleanup registry. These can be absent in injected-history tests. Access
them through their owning adapters, and do not serialize live services in results.

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
Each `delta()` replaces the full streaming state; it is not an append or patch.
An LLM activity must accumulate provider fragments before yielding a snapshot.

## Related

- [Workflow composition](workflows.md)
- [Local processes and cleanup](target-environments.md)
- [Saved-execution compatibility](../reference/compatibility.md)