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

## References and discovery

All runner entry points use one explicit reference: `foo/bar:buzz` resolves
`workflows/foo/bar.py` and its `buzz` callable. Paths are relative to the configured
workflows directory, extensionless, and slash-separated. The callable is required.
Absolute paths, traversal, escaping symlinks, `.py` suffixes, and the former
`workspace:` form are rejected. Library references use `python:package.module:main`.
The runner, child spawning, CLI, and control protocol use the same format; split
file/callable arguments and implicit callable defaults are not supported.

```python
from pathlib import Path
from vectorops.runner.runner.workflow import WorkflowRunner

async def run():
    runner = WorkflowRunner(Path("/path/to/workspace"))
    project = await runner.project()
    descriptor = await project.resolver.describe("foo/bar:buzz")
    return await runner.run(descriptor.reference, input={"text": "hello"})
```

Add `description="..."` to `@workflow` to explain its purpose. Discovery returns
a `WorkflowDescriptor` with `reference`, display `name`, optional `description`,
contract `kind` and `version`, and `input_schema`. Inputs exclude the injected
context parameter. Undecorated async entry points are discoverable too.

Discovery resolves references on demand, without a registration step or directory
scan. Newly generated files are available immediately. A runner shares a resolver
across executions. Each source file is imported once until its filesystem
fingerprint changes; unchanged files are not reread or parsed. Changed files are
hashed before reimporting, so timestamp-only changes reuse the loaded definition.
Concurrent resolutions share the cache. Missing files are not negatively cached,
and broken updates fail rather than using a stale definition.

Use `await project.resolver.invalidate(reference)` to evict one file, or
`await project.resolver.invalidate()` to clear all workspace entries. This cache
does not track transitive imports; library and helper imports follow normal Python
import caching. Imports execute trusted Python code, not sandboxed metadata parsing.
Already selected callables remain stable while running; subsequent resolutions see
updates. Arbitrary source changes are not made replay-safe by caching.

## LLM tools

Import `llm` for conversations and `tools` for tool factories and contracts:

```python
from vectorops.runner.api import llm, tools
```

The `tools` module re-exports the existing implementations; importing it does not
create environments, register tools, or grant permissions. Pass selected bindings
explicitly to `LLM.run(tools=[...])`. Existing imports through `api.llm` and the
individual implementation modules remain supported.

| Entry point | Purpose |
| --- | --- |
| `tools.apply_patch_tool(service, format="v4a", approval=None)` | Synchronous factory for patch application; accepts `v4a` or `patch` (SEARCH/REPLACE). |
| `await tools.subworkflow_tool(references, name="subworkflow", approval=None)` | Async factory exposing an explicit set of child workflows. |
| `tools.Tool` | Construct a custom tool from a Pydantic argument model and async callback. |
| `tools.ToolBinding`, `tools.WorkflowToolBinding` | Contracts for activity-backed and workflow-composed tools. |
| `tools.ToolResponse`, `tools.ToolExecutionError` | Return tool results or report execution errors. |
| `tools.ToolApproval`, `tools.ToolApprovalResponse` | Approval configuration and response schema. |
| `tools.invoke_tool` | Invoke a binding through the existing recorded approval/execution machinery. |

Patch tools require a `PatchService` from `vectorops.runner.patch`, constructed
with an existing target environment. The facade does not own its lifecycle; see
[target environments](target-environments.md). Given an existing service and
conversation, select the patch binding explicitly:

```python
from vectorops.runner.api import llm, tools
from vectorops.runner.patch import PatchService


async def edit(conversation: llm.LLM, service: PatchService):
    return await conversation.run(tools=[tools.apply_patch_tool(service)])
```

### Subworkflow tools

```python
from vectorops.runner.api import llm, tools

async def agent(ctx, prompt: str):
    tool = await tools.subworkflow_tool(["research:main", "reports:summary"])
    conversation = llm.LLM("agent")
    conversation.add_user_message(prompt)
    answer = await conversation.run(tools=[tool])
    return answer.model_dump(mode="json")
```

The async factory returns a ready binding, with available workflow descriptions
and input schemas in its tool definition. `LLM.run` continues to accept bindings,
not bare implementation callables or unawaited factories. Ordinary `Tool`
construction remains synchronous.

The tool accepts `reference` and an `input` object (default `{}`). The supplied
list is a strict, nonempty allowlist with no duplicates. JSON Schema validation
checks inputs before policy evaluation or host approval. Creating a file does not
grant permission: call the factory again with an updated list to expose it.
Descriptor snapshots are recorded as `workflow.describe` activities, so replay
does not rediscover historical metadata from the current filesystem.

Subworkflow calls use a recorded policy/approval gate followed by durable
`workflow.spawn` and `workflow.join` operations. Child execution is not hidden
inside a tool activity. Results are JSON text; child failures are tool errors.
Cancellation, storage failures, and replay mismatches propagate. A completed
replay reuses both the approval gate and child result without running them again.
Custom workflow-side tools can implement `tools.WorkflowToolBinding`; their
`execute_workflow` method must keep nondeterministic work inside activities.

## Answer with a workflow choice

Use `LLM.run_with_choice` on an existing conversation to return a `ChoiceResult`
with `answer` and `choice`. Choices map stable names to descriptions. Like
`LLM.run`, this loop executes in the caller's persistent workflow context and
records individual generations and tool executions; it is not a catalog entry
or a separate child workflow.

```python
from vectorops.runner.api import llm


async def review(conversation: llm.LLM, task: str) -> llm.ChoiceResult:
    conversation.add_user_message(task)
    return await conversation.run_with_choice(
        choices={"ready": "Ready to proceed", "blocked": "Needs more information"},
        max_retries=2,
    )
```

The method reuses the LLM's history, settings, provider, compaction, usage
accounting, and inbox. Pass ordinary tool bindings with `tools` and optional
dynamic bindings with `tool_set`, just as with `run`. Tool execution retains
the existing approval, concurrency, and error handling behavior.

The loop temporarily appends completion instructions to the system prompt and requires one
`__finish_choice__` call containing a nonblank answer and an allowed choice.
That tool name is reserved. Mixed finish/work calls and multiple finish calls are
rejected without executing any of those calls. The original system prompt is
restored on success, failure, or cancellation. Accepted and rejected control calls
receive tool results in conversation history, allowing later calls on the same LLM.

`max_rounds` inherits the LLM configuration when omitted and limits all loop rounds,
including repairs, just as with `run`.
`max_retries` defaults to 2 and allows 0 through 20 completion repairs after the
first missing or invalid finish. During repairs only the finish tool is offered;
ordinary tool calls are rejected without execution. Exhaustion raises
`llm.ChoiceLimitError` or `RoundLimitError`. Provider retries remain independently
configured. Choice names must contain only letters, digits, underscores, or hyphens.

Partial response content is withheld for this call, but progress heartbeats are retained.
Raw completed responses, including control calls and rejected attempts, remain
available in activity history and diagnostic events; they are not private data.
The caller receives only the validated answer and choice, without a control-tool
envelope. Replay reconstructs the loop from recorded generations and tool results
rather than repeating completed work. Use `result.model_dump(mode="json")` when
returning the result across a workflow boundary.

## Workflow contracts

The workflow decorator accepts the same `kind`, `namespace`, and positive `version` options as [activity contracts](activities.md#payload-contracts).
Root and child configurations persist their contract metadata before executing.
The default workflow kind is `workflow`, and the default version is `1`, including older records without these fields.
For example, `@workflow(kind="customer.acme.chat.turn", version=2)` declares the contract for the workflow input and final result.
Changing a recorded workflow's kind is rejected when that workflow executes.
New executions use the declared version, while `ctx.version` retains the persisted
version on replay. Gate incompatible orchestration changes on `ctx.version` and
keep the legacy branch available for existing executions. A version bump does not
migrate history or force completed work to execute again.
Structural operation kinds remain `workflow.spawn`, `workflow.join`, `workflow.complete`, and `workflow.resume`.
A spawn references the independently stored child configuration containing its kind and version.

An LLM turn can be a child workflow containing model invocation and tool execution activities, returning the last assistant message.
A chat workflow can repeatedly accept input and call that turn workflow.
Clients can flatten child activity histories for chat rendering while retaining workflow identity for grouping.
Render the returned final message only once, not again for join or completion bookkeeping.
Select specialized renderers by operation kind and version; unknown contracts can display the operation name, input, current state or result, and structured error.
Refreshing a step snapshot updates its existing entry rather than appending another message.

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