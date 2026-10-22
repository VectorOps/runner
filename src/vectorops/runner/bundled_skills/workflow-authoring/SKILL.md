---
name: workflow-authoring
description: Use when creating or changing VectorOps Runner workflows, activities, LLM agents, or tools. Covers authoring APIs, deterministic replay, side effects, contract versioning, and verification.
---

# Workflow authoring

Create durable Python workflows with recorded activities and child calls. Use this skill when adding a workflow or changing code that existing executions may replay.

## Start with a runnable workflow

1. Locate the runner workspace and its `config.toml`. The attached target project and runner workspace can differ. `workflows_directory` defaults to `workflows` relative to the runner workspace; do not assume workflows belong at the target root.
2. Define module-level async entry points with an injected `WorkflowExecutionContext` first, followed by named, typed business parameters. Use defaults for optional inputs. Positional-only business parameters, variadic inputs, and closures are unsupported.
3. Keep orchestration deterministic. Put external I/O and nondeterministic values in activities.
4. Use JSON-compatible boundary inputs and results. Serialize Pydantic results with `model_dump(mode="json")`; do not return clients, paths, tasks, or live handles.

Save this as `workflows/greeting.py` in the runner workspace:

```python
from datetime import datetime, timezone

from vectorops.runner.api import composition
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.context import WorkflowExecutionContext


@composition.activity(kind="example.greeting.timestamp", version=1)
async def timestamp(ctx: ActivityExecutionContext) -> str:
    return datetime.now(timezone.utc).isoformat()


@composition.workflow(
    kind="example.greeting",
    version=1,
    description="Return a greeting with a recorded timestamp.",
)
async def main(ctx: WorkflowExecutionContext, name: str = "world") -> dict[str, str]:
    recorded_at = await timestamp()
    return {"message": f"Hello, {name}!", "recorded_at": recorded_at}
```

The timestamp activity runs once on completed execution; replay uses its recorded result. The caller supplies no context argument to decorated activities or child workflows.

References use `folder/file:callable`, relative to the workflows directory, with no `.py` suffix. Absolute paths, traversal, and escaping symlinks are rejected for workflow references. Importable library entry points use `python:package.module:callable`; shipped agent references include `builtin:agent` and `builtin:helper`. Workflow-reference restrictions differ from skill-file paths, which can be absolute.

Discovery imports trusted Python code and derives an input schema; it is not a sandbox or a registration step. New files are immediately discoverable. Changed source is reloaded on subsequent resolution, but helper/transitive imports follow Python's import cache. Keep import-time work free of side effects. Undecorated async root functions work too; use `@composition.workflow` for metadata and direct durable child calls.

## Choose the right API

| API | Use and boundary |
| --- | --- |
| `composition.workflow(...)` | Define a durable workflow. `await child(value)` records spawn/join and returns its final result. An ordinary async helper stays in the caller's history. |
| `composition.activity(...)` | Record an async external operation. Implementation receives `ActivityExecutionContext`; completed replay skips it. |
| `composition.streamable_activity(...)` | Async generator yielding `composition.delta(state)` and `composition.final(result)`. Each delta replaces the full progress state, not a patch or append. Workflows await only the final result. |
| `composition.activity_wrapper(...)` | Advanced adapter: its body runs on replay. Injected `ActivityCall.run(input=..., invoke=...)` or `.stream(...)` records at most one operation. Keep all effects inside `invoke`. |
| `ctx.spawn_workflow(reference, input=...)`, `ctx.join_workflows(handles)` | Explicit composition when references are dynamic. Join handles belong to the creating parent; `failure_policy="collect"` returns failed outcomes instead of propagating them. |
| `await handle` | Observe one handle returned by `ctx.spawn_workflow`. Returns a completed, failed, or cancelled `ChildOutcome`; cancelling the waiter does not cancel the child. |
| `ctx.next_completed(handles)` | Record and return one `ChildCompletion` with `handle` and `outcome`, ordered by committed child completion position. Other children keep running. |
| `ctx.cancel_workflows(handles, reason=None)` | Persist cancellation intent for owned children and await subtree cleanup. Returns completed, failed, or cancelled outcomes in handle order. Cancelling a raw join waiter does not cancel its children. |
| `api.configuration.get_config(ctx, name, default, model=...)` | Record a component configuration lookup and optionally validate/normalize with its owner model. Do not read live configuration in orchestration or use this API for secrets. |
| `api.llm.LLM` | Conversation with recorded generations and tool execution. Use `add_user_message`, `run`, or `run_with_choice`; call in workflow context, not inside an ordinary activity. |
| `api.tools` | Explicit tool bindings: `Tool`, `ToolBinding`, `WorkflowToolBinding`, `ToolResponse`, `ToolExecutionError`, `invoke_tool`, `load_collection`, and tool factories. Importing the facade does not grant permissions. |
| `await tools.subworkflow_tool(references)` | Expose an explicit child-workflow allowlist to an LLM. Descriptor snapshots and approval decisions are recorded; child execution remains durable spawn/join, not an opaque tool activity. |
| `api.mailbox.request_input(...)` | Durable structured input request with payload and response contracts. Host/control integration supplies answers. |
| `llm.request_user_message(...)` | Mailbox-backed chat input returning a user-role message. Distinct from initial workflow arguments. |
| `api.inbox.Inbox` | Addressed steering input; `drain()` is replay-aware. Can be attached to an LLM conversation. |
| `api.sleep.sleep(seconds)`, `api.sleep.sleep_until(deadline)` | Durable waits; relative waits use persisted creation time, absolute deadlines must be timezone-aware. Import the functions from `vectorops.runner.api.sleep`. |

The `api.*` names above refer to modules under `vectorops.runner.api`, not methods on the context. `ctx.data_path` exposes the runner data directory; it does not make arbitrary filesystem operations replay-safe. Activity context supplies identities and narrow runtime services such as the target environment, mailbox, and resource cleanup registry; optional services can be absent in injected-history tests. Keep service access and lifecycle inside the owning adapter instead of reaching into runner internals.

## Add an LLM conversation

Configure a model/provider through the runner's component settings; do not embed credentials in workflows or recorded inputs.

```python
from vectorops.runner.api import composition, llm, tools
from vectorops.runner.runner.context import WorkflowExecutionContext


@composition.workflow(kind="example.research", version=1)
async def main(ctx: WorkflowExecutionContext, question: str) -> str:
    helper = await tools.subworkflow_tool(["builtin:helper"])
    conversation = llm.LLM("research", system_prompt="Answer using verified evidence.")
    conversation.add_user_message(question)
    response = await conversation.run(tools=[helper], max_rounds=10)
    return "\n".join(block.text for block in response.content if block.type == "text")
```

Only offer bindings the task needs. Await async factories; pass bindings, not raw functions, to `run`. Creating a workflow file does not add it to an existing subworkflow allowlist. Tool policy and approval still apply. Custom workflow-composed tools must keep nondeterministic work in activities; ordinary activity-backed tools must not schedule child workflows from inside their activity implementation.

For constrained routing, `run_with_choice(choices={"ready": "Ready", "blocked": "Needs input"})` returns validated `answer` and `choice`. It uses the existing conversation/history, not a new child workflow.

## Use child workflows for parallel workers

Each independently progressing parallel worker must be a module-level `@composition.workflow` entry point. Each child then owns its history and replay cursor. Ordinary async helpers, including helpers that create LLM conversations, stay in their caller's history; scheduling them as tasks does not create child workflows.

Use `asyncio.gather` or `TaskGroup` to await decorated child workflow calls, or use `ctx.spawn_workflow` and `ctx.join_workflows` for explicit child handles. Start children in a deterministic input order. Do not fan out ordinary helper coroutines, activity calls, or LLM loops as parallel workers within one workflow. Keep each worker's orchestration sequential; use further child workflows for nested parallel workers. Activity implementations can use concurrent I/O internally because completed replay skips their implementation.

```python
import asyncio

from vectorops.runner.api import composition, llm
from vectorops.runner.runner.context import WorkflowExecutionContext


@composition.workflow(kind="example.parallel.worker", version=1)
async def worker(ctx: WorkflowExecutionContext, question: str) -> str:
    conversation = llm.LLM("research")
    conversation.add_user_message(question)
    response = await conversation.run(max_rounds=10)
    return "\n".join(block.text for block in response.content if block.type == "text")


@composition.workflow(kind="example.parallel", version=1)
async def main(ctx: WorkflowExecutionContext, questions: list[str]) -> list[str]:
    return await asyncio.gather(*(worker(question) for question in questions))
```

Here `worker` is decorated; removing its decorator would put all conversations in the parent's history. Recorded result-delivery order alone does not guarantee that concurrent ordinary coroutines submit their next operations in the same order on replay.

Consume child results in invocation order, or use `ctx.next_completed(handles)` for a recorded completion-order decision. Do not use an ordinary shared queue, `asyncio.as_completed`, or an unrecorded first-completed race to decide subsequent recorded operations. Do not share mutable orchestration state between workers; pass JSON-compatible inputs and return results.

Moving existing inline workers into child workflows changes the recorded operation structure. Version-gate the new path for new executions and preserve the old path for existing histories; this change does not repair an already failing replay.

### Await one spawned child

Handles returned by `ctx.spawn_workflow` are awaitable within the spawning workflow's lifetime:

```python
handle = await ctx.spawn_workflow("analysis:run", input={"question": question})
outcome = await handle
if outcome.kind == "completed":
    result = outcome.result
```

This delegates to a recorded single-child join with `failure_policy="collect"`. Child failures and cancellation are returned as outcomes, unlike decorated calls that return the result or raise. Each direct await records a join; it is not a cached future. Use `ctx.join_workflows` for explicit batch joins or failure propagation. Manually reconstructed handles are unbound and require an explicit context join.

Handles are accepted by `asyncio.gather`, `asyncio.as_completed`, and `asyncio.ensure_future`. `asyncio.create_task` and `TaskGroup.create_task` require coroutine objects, not general awaitables; use decorated child calls with those APIs. Awaitable handles do not add a durable first-completed selection operation or change the completion-order restrictions above. Cancelling a handle waiter only stops observation; use `ctx.cancel_workflows` to cancel children.

### Process the next completed child

Use `ctx.next_completed` for a pool that processes available results and replenishes workers without waiting for the oldest invocation:

```python
while outstanding:
    completion = await ctx.next_completed(outstanding)
    outstanding.remove(completion.handle)
    outcome = completion.outcome
    if outcome.kind == "completed":
        result = outcome.result
```

The method selects the earliest committed terminal completion among the supplied children, waits for that child's managed task cleanup, and persists its spawn/completion-step reference before returning. FIFO means history insertion order, not timestamps or callback order; recovery reconstructs the same order without a separate queue journal. Replay returns the recorded selection without restarting terminal children or repeating completed effects.

Failed and cancelled children are returned as outcomes. Empty selections, duplicate spawn IDs, foreign parents, and handles inconsistent with their recorded spawn are rejected. Remove the returned handle yourself; repeated calls retaining it can return the same child. Multiple observers may select the same child. Cancelling the waiter does not cancel children or revoke an already accepted selection operation. Inputs and returned business results must remain JSON-compatible; the completion's live handle is orchestration-only.

### Cancel stale workers

A task awaiting a decorated child owns that invocation. Calling `task.cancel()` requests durable cancellation of its child and descendants; awaiting the task settles cleanup before raising `asyncio.CancelledError`. `TaskGroup` awaits this cleanup when cancelling siblings after a failure. A child's already persisted completion is not overwritten, even when its owning invocation is cancelled.

New workflow instances store `child_cancellation_version=1`. Existing instances without that marker retain their original wait-only decorated-call cancellation behavior, so upgrading Runner does not insert cancellation operations into old histories. The explicit cancellation API is available for new cancellation boundaries.

For explicitly spawned children, use `await ctx.cancel_workflows(handles, reason="Analysis is stale")`. Cancelling a task awaiting `ctx.join_workflows(handles)` only stops that observer; other observers and the children continue. Repeated cancellation requests retain terminal outcomes. Cancellation is cooperative and does not roll back completed effects; a child that suppresses cancellation may complete normally or delay settlement. Runner shutdown suspension preserves resumable work instead of recording child cancellation.

Await cancellation before editing state that workers can access. Keep external cleanup and idempotency in activity implementations or their registered resource cleanup. Replay consumes persisted cancellation and does not repeat completed child effects. Cancellation does not make unrecorded completion-order scheduling decisions deterministic.

## Preserve deterministic replay

Replay means reconstructing orchestration from recorded outcomes, not obtaining the same answer from a fresh model or network call.

- Derive branching and operation arguments only from workflow inputs, recorded results, and deterministic transformations. Sort unordered collections before scheduling work. Avoid mutable process-global state.
- Keep filesystem/network/process access, environment-variable reads, randomness, UUID generation, and current time inside activities. Use async I/O or `asyncio.to_thread` for blocking operations inside activities.
- Preserve the order and identity of recorded operations. Ordinary activity replay matches `kind`, `name`, and `input`; versions are informational. Operations whose kinds start with `llm.` ignore input during matching. This exception is not a guarantee that arbitrary prompt or code changes are safe.
- Do not wrap an entire LLM/tool loop or child workflow orchestration in an ordinary activity. Activity implementations have no active workflow composition context.
- Require child workflows for parallel workers as described above; a fixed task creation order does not make concurrent ordinary helpers replay-safe. Raw timers, network events, and background tasks do not acquire durability merely by being awaited. Use the durable sleep API for waits.
- Await owned work before returning. Propagate cancellation; do not suppress replay or persistence failures or treat them as ordinary business failures. Recorded activity failures surface as `ActivityError` and can be handled deliberately.
- Assume interrupted activities can run again with the same `ctx.step_id`. Use it as an external idempotency key, or implement reconciliation. The runner does not guarantee exactly-once external effects, and a fork can create new identities.
- Treat history as persisted data: do not record secrets in activity arguments, results, progress, configuration snapshots, or diagnostic messages.

## Evolve workflows and contracts

`workflow`, `activity`, and `streamable_activity` accept positive integer `version` (default `1`) and either `kind` or `namespace`, not both. Generic default kinds are `workflow` and `activity`. Use stable explicit kinds for published contracts, with your project's namespace. `namespace="example.project"` generates a kind from namespace, module, and callable; renaming files or functions changes generated kinds. Activity `name` defaults to the function name and participates in replay matching.

Workflow versions and activity versions have different roles:

- A new workflow stores its declaration's version. `ctx.version` returns that durable version, even after code upgrades. Existing executions retain their old version; new executions use the new declaration. Each child has its own durable version.
- Workflow kind changes are rejected when that workflow executes against an existing configuration. Bumping its version is allowed; it does not migrate recorded inputs or results or rewrite history.
- Activity versions are recorded metadata, **not a replay compatibility gate**. Changing only an activity version does not force re-execution or reject old outcomes. Keep old result readers compatible, or preserve a legacy operation in a version-gated branch and introduce a distinct new kind/name for the incompatible operation.
- A reused completed child result skips execution of the child's internal activities. Root replay alone does not test compatibility of a partially completed child.

For an upgrade from workflow version 1 to 2, retain the old path:

```python
@composition.workflow(kind="example.pipeline", version=2)
async def main(ctx: WorkflowExecutionContext, text: str) -> str:
    if ctx.version < 2:
        return await legacy_transform(text)
    return await modern_transform(text)
```

Here `legacy_transform` and `modern_transform` are separately defined activities; keep the former's operation identity, input, and result semantics intact. Do not remove old branches while executions still depend on them. Preserve reference paths/callables and accept old inputs. Internal refactoring that leaves contracts and orchestration unchanged does not require a version bump. Source reload is not history migration.

## Verify before delivering

1. Resolve the reference and validate valid, invalid, omitted, and defaulted inputs against its descriptor. Test JSON serialization of results.
2. Test a fresh execution and completed replay using the project's test setup. Assert the same result, no repeated completed side effects/provider calls, and no appended history for a completed replay.
3. Interrupt an activity before terminal persistence, then resume. Assert stable step identity and idempotent effects. Test failures and cancellation.
4. For an upgrade, run old code first, retain its persisted workspace, then load new code. Test old root and child histories, including incomplete executions. Also test that new executions follow the new `ctx.version` branch.
5. For parallel workers, verify that each worker has a distinct child workflow ID and history. Test different child completion orders and partial-child resume without repeating completed effects. Test tool allowlists/approval, input requests, and offline replay when used. Replace external providers with fakes; do not require real credentials for unit tests.
6. Follow the project's test and formatting conventions.
