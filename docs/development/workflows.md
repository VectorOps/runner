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

## Concurrent activities and children

```python
import asyncio


async def parent(ctx: WorkflowExecutionContext) -> list[str]:
    return list(await asyncio.gather(child("alpha"), child("beta")))
```

Use standard `asyncio.gather`, `create_task`, `wait`, and `TaskGroup` to coordinate activities, streaming activities, and child calls, including mixed calls.
Child workflows can fan out recursively; the single root workflow per execution does not limit concurrent descendants.
Requests reserve durable invocation records in submission order, then execute independently without blocking acceptance of other requests.
Completion updates the invocation record without moving the history head backward.
Before returning a result or raising a recorded error, the dispatcher appends a `workflow.resume` record referencing the invocation.
Replay uses these records to deliver results in the original order, so concurrent branches can schedule further work after each await.
Asyncio owns result aggregation and exception handling, including `gather(..., return_exceptions=True)`.
Parent shutdown cancels owned activities, children, and pending waits; unfinished invocations remain recoverable in history.
Cancelling a child-call waiter cancels that wait, not the underlying child; the child remains owned by the parent until parent shutdown.
There are no custom start or gather helpers; decorated workflow calls are ordinary awaitables.
Keep external I/O, nondeterministic values, and wall-clock timing inside activities.
Only engine-managed invocation results have durable delivery ordering; raw asyncio timers, external events, and arbitrary background tasks are not durable workflow primitives.
Interrupted activities may execute again with the same step identity, so side effects must use implementation-owned idempotency.

One persistent dispatcher owns request acceptance and replay consumption.
Deferred activities and joins resolve independently, while a session supervisor treats storage and replay failures as fatal rather than catchable operation outcomes.
History remains streamed during replay, with only active invocations and one lookahead record retained in memory.
Older records without `deferred_delivery` retain their original immediate-delivery replay behavior; new records set it to `true` for activities and joins.

## Streaming step events

Pass an optional synchronous `event_sink` callback to `WorkflowRunner` to observe executor step snapshots.
Events include execution identity and a complete step record, with kinds `step.created`, `step.updated`, `step.completed`, and `step.failed`.
Activity creation and updates reference the same step ID; progress updates are not additional history steps.
Result delivery adds a separate `workflow.resume` step without duplicating the result payload.
Nested activity events reach the same execution sink.

The executor emits events after history accepts a write.
Buffered streaming snapshots are immediately observable but are not necessarily committed to SQLite yet.
Terminal outcomes are persisted before their events are delivered.
Sink exceptions are logged without changing workflow execution; callbacks should remain nonblocking.

Workflows await only final results, not progress events.
Completed replay does not re-emit historical updates.
There is no subscription queue or progress iterator in the workflow authoring API.