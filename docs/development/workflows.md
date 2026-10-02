# Workflow composition

Await decorated async functions to execute durable child workflows and receive their final results.
Use ordinary async helpers when execution should remain inside the caller's workflow scope.

## Direct child calls

```python
from vectorops.runner.api.composition import workflow
from vectorops.runner.runner.context import WorkflowExecutionContext


@workflow()
async def child(ctx: WorkflowExecutionContext, text: str) -> str:
    return text.upper()


@workflow()
async def parent(ctx: WorkflowExecutionContext, text: str) -> str:
    return await child(text)
```

The executor injects the first context parameter and records spawn and join operations internally.
Business arguments are bound by name, default values are applied, and JSON-compatible inputs are persisted.
An explicit decorator name is optional; the default comes from the function's module or workspace filename and function name.
Persistence uses the source file or importable module and function name, not a serialized callable.
Module-level functions with named business parameters are supported; closures, positional-only business arguments, and variadic inputs are rejected.

Workspace roots can remain undecorated async functions or use the same decorator.
Importable library functions are resolved by the executor through `python:<module>:<function>` references.
Raw async helpers remain ordinary Python calls and do not create child histories.

## Concurrent children

```python
from vectorops.runner.api import composition
from pydantic import JsonValue


async def parent(ctx: WorkflowExecutionContext) -> list[JsonValue]:
    first = await child.start("alpha")
    second = await child.start("beta")
    return await composition.gather_workflows([first, second])
```

Starts are ordered in parent history while child execution runs concurrently.
Gather returns results in invocation order and propagates recorded child failures as `ActivityError`.
For outcome collection, retain the lower-level `ctx.join_workflows(..., failure_policy="collect")` API.
Concurrent `asyncio.gather(child(...), child(...))` is not supported because parent operation submissions must remain sequential.

## Streaming step events

Pass an optional synchronous `event_sink` callback to `WorkflowRunner` to observe executor step snapshots.
Events include execution identity and a complete step record, with kinds `step.created`, `step.updated`, `step.completed`, and `step.failed`.
Activity creation and updates reference the same step ID; updates are not additional history steps.
Nested activity events reach the same execution sink.

The executor emits events after history accepts a write.
Buffered streaming snapshots are immediately observable but are not necessarily committed to SQLite yet.
Terminal outcomes are persisted before their events are delivered.
Sink exceptions are logged without changing workflow execution; callbacks should remain nonblocking.

Workflows await only final results, not progress events.
Completed replay does not re-emit historical updates.
There is no subscription queue or progress iterator in the workflow authoring API.