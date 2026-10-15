# Runner and worker execution boundary

This document proposes a runner/worker split with selectable in-process or isolated execution. It is an executive summary and high-level design, not a description of implemented configuration or a finalized wire protocol.

## Executive summary

Separate durable orchestration and privileged services from workflow Python and project-local tools. The runner owns history, replay decisions, credentials, approvals, and external capabilities. The worker loads workflows, reconstructs workflow-local state, and executes project-local activities and tools.

Users choose the deployment model:

| Mode | Deployment | Security contract |
| --- | --- | --- |
| In-process | Runner and worker execute in one Python process through direct adapters | Workflow code is trusted with the process's authority; tool policies govern cooperative agent behavior, not arbitrary Python |
| Isolated | Runner communicates over RPC with a worker in a container, sandbox, VM, or remote host | Worker code is untrusted; external isolation and runner-enforced capability grants protect runner resources |

Both modes use the same execution contracts and replay semantics. Isolation is optional, but isolated mode must never silently fall back to in-process execution.

The recommended tool model is fat worker-side implementations with narrow runner capabilities. Patching, file access, and process management execute beside the project filesystem. Authenticated LLM and MCP operations remain runner-side. This avoids filesystem RPC chatter without provisioning provider credentials into the worker.

Use transient RPC and disposable worker processes. The runner persists execution state; recovery starts a replacement worker and replays committed history. Uncertain side effects require explicit recovery policy, not unconditional retries.

The estimated implementation effort is 6-10 engineer-weeks for one engineer, including current-feature parity and failure testing. A constrained proof of concept is approximately 2-3 weeks. This is an execution-boundary refactor, not merely a transport addition.

## Contents

- [Goals and scope](#goals-and-scope)
- [Current architecture and change points](#current-architecture-and-change-points)
- [Proposed ownership](#proposed-ownership)
- [Execution and replay](#execution-and-replay)
- [Tool execution](#tool-execution)
- [RPC and security](#rpc-and-security)
- [Failure and recovery](#failure-and-recovery)
- [Deployment and provisioning](#deployment-and-provisioning)
- [Compatibility and migration](#compatibility-and-migration)
- [Implementation plan and estimate](#implementation-plan-and-estimate)
- [Validation and acceptance criteria](#validation-and-acceptance-criteria)

## Goals and scope

- Preserve workflow composition, local Python state, deterministic activity replay, streaming, child workflows, and interactive input.
- Support direct in-process execution and isolated execution through one set of contracts.
- Keep project Python imports and arbitrary activity implementations outside the trusted runner in isolated mode.
- Keep history, provider credentials, authorization, and privileged integration state under runner ownership.
- Execute complete local tools in the worker rather than forwarding individual filesystem operations.
- Permit workflows to modify project sources without granting access to runner authority.
- Recover from worker loss without losing committed execution history.

Initial scope excludes a sandbox provisioning platform, a worker-side durable mailbox, exactly-once arbitrary side effects, transparent serialization of Python objects, and live migration of running workflows to edited code. A separate minimal worker distribution and automatic source-artifact capture are follow-up packaging and reproducibility work.

## Current architecture and change points

The repository already centralizes important execution behavior, but its internal interfaces assume a shared Python process.

| Area | Current owner or interface | Change required |
| --- | --- | --- |
| Workflow startup | `runner/workflow.py`, `runner/executor.py` | Separate durable lifecycle from loading and invoking workflow Python |
| Operation submission | `runner/context.py`, `api/composition.py` | Replace cross-boundary callbacks with serializable requests and worker-local invocation handles |
| Replay and delivery | `runner/session.py`, `runner/handlers.py` | Retain runner ownership while dispatching executable work to the worker |
| Runtime capabilities | `runner/activity_context.py` | Separate worker-local resources from runner service contracts |
| Project state | `runner/project.py` | Stop exposing a combined paths, configuration, resolver, and database object to remote workflow execution |
| Workflow discovery | `runner/resolver.py` | Move project-code discovery, description, and validation into the worker |
| Tool invocation | `api/tool_execution.py`, `api/tool_approval.py` | Separate binding execution from trusted authorization and durable outcome handling |
| Provider I/O | `api/llm/provider.py`, `api/llm/invocation.py` | Dispatch serializable provider requests without moving credential resolution into the worker |
| MCP | `api/mcp.py`, `mcp/` | Expose discovery and invocation capabilities without exporting secret-bearing configuration |
| Transport | `control/protocol.py`, `control/transport.py`, `control/client.py` | Reuse framing and correlation patterns, not the administrative control permission surface |

Paths in this table are relative to `src/vectorops/runner/`.

The existing `OperationSubmitter` is a useful seam, but activity requests contain callables and streaming generators. `ActivityExecutionContext` contains live mailbox, authentication, environment, MCP, and cleanup objects. These are not wire payloads.

Discovery is also executable code: the resolver imports project modules to find and describe workflows. Moving only the final workflow invocation would leave an isolation bypass in the trusted process.

## Proposed ownership

```text
Trusted runner                         Worker execution boundary
-------------------------------        -------------------------------
History and checkpoints                Workflow loading and validation
Replay matching and delivery order     Workflow-local state and LLM loop
Execution and child coordination        Custom Python activities
Approval and capability enforcement     Tool bindings and local tools
Provider credentials and connections    Project filesystem and processes
Authenticated MCP runtime               Worker-local cleanup resources
                 <--- execution/capability contracts --->
                      direct adapters or transient RPC
```

The runner owns durable state transitions. The worker reports requests, stream updates, results, and failures; it does not append history or select arbitrary history cursors.

Replace whole-runtime access with explicit capabilities. Worker-visible project information contains local paths, workflow identity, and permitted non-secret configuration. It does not contain a database handle, authentication service, or a generic proxy to runner internals.

Custom workflow imports, tool factories, validators, and arbitrary callbacks execute in the worker. Trusted runner extensions are separately installed and explicitly registered by the operator. The worker cannot supply executable code for the runner to import.

## Execution and replay

### Invocation lifecycle

1. The worker loads a selected workflow and creates its workflow-local context.
2. At an activity call, it retains the Python callback locally and submits the operation description with a session-local invocation handle.
3. The runner matches the operation against history or durably reserves new work.
4. For a completed ordinary activity, the runner returns its recorded outcome at the recorded delivery point without executing the callback.
5. For live worker-owned work, the runner authorizes dispatch and sends an execution instruction referencing the retained callback. Trusted service operations use registered runner implementations instead.
6. The runner validates and commits the outcome, then delivers it according to session ordering.
7. Workflow completion is checked against replay or recorded by the runner.

Callbacks never cross RPC and are not persisted as recovery artifacts. Replacement workers recreate them by replaying the workflow.

### Preserve existing semantics

The session records delivery order separately from completion order. Transport arrival order must not change which coroutine resumes first. Preserve this behavior for parallel activities, joins, cancellation, and replay exhaustion.

Workflow tools require more than returning a recorded result. Existing reconstruction behavior re-executes selected workflow implementations to rebuild local state and emit nested durable operations. The protocol needs explicit reconstruction instructions and parent-operation identity in place of a process-local parent `ContextVar`.

Streaming updates retain the current full-snapshot semantics. Receiving a final-value message does not commit success until the stream terminates successfully; the current handler also validates generator exhaustion after `final()`.

Full Python access does not change the determinism contract. Direct time, randomness, filesystem reads, and network access can still diverge on replay. Replay-relevant inputs continue to require recorded activities or stable execution inputs.

### Source revisions

Project source edits affect subsequent runs, not an existing run's implementation. Resume requires a compatible retained execution revision and environment. If the original revision is unavailable, report that limitation instead of silently claiming replay compatibility with edited code.

The boundary protocol carries execution revision identity. Building and retaining immutable source/environment artifacts is separate work; a source hash alone neither retains code nor makes imports reproducible.

## Tool execution

Use one tool-facing abstraction with placement determined by the capability it needs, not a runner/worker implementation pair for every tool.

| Tool or behavior | Worker responsibility | Runner responsibility |
| --- | --- | --- |
| Read, list, patch, shell, process sessions | Entire local implementation and worker-local resources | Durable invocation, declared policy checks, recorded outcome |
| Custom Python tool | Binding, argument validation, implementation | Durable coordination and any explicitly granted capabilities |
| Plan tool | Reconstruct local plan state | Record durable operations and publish execution events |
| Subworkflow tool | Binding and workflow orchestration | Authorize and coordinate durable spawn/join operations |
| Web fetch | Network I/O under worker network policy | Record outcome |
| LLM | Conversation state, prompts, tool loop, local adapters | Provider authentication, permitted provider I/O, limits |
| Authenticated MCP | Tool-facing adapter and local result conversion | Server configuration, authentication, discovery, invocation |
| Approval and input | Request and consume decisions | Policy evaluation, persistent mailbox, grants |

For `apply_patch`, parsing, matching, reads, writes, and moves all execute in the worker using its local `TargetEnvironment`. Send the complete patch request and return its structured result. Do not stream whole files through the runner solely to apply a patch.

The LLM loop is already workflow-local. Tool calls can originate within the worker, so local tools need runner coordination but not an artificial extra forwarding hop. Remote authenticated operations use coarse calls such as a full provider request or MCP invocation, not credential-resolution RPC that returns secrets.

Keep argument validation near the owning implementation. The runner independently validates capability requests and enforces trusted grants. Worker-provided schemas, names, or assertions of authorization are not sufficient to authorize privileged effects.

Approval of a local tool call controls that invocation only. Workflow Python can directly modify its own filesystem or run processes. Tool history is not a complete or tamper-resistant audit of worker behavior.

## RPC and security

Define a worker protocol separate from the existing administrative control API. Exact packet names and schemas remain an implementation task.

| Message family | Purpose |
| --- | --- |
| Session setup | Negotiate protocol compatibility, worker identity, execution revision, and permitted capabilities |
| Workflow metadata and lifecycle | Discover, describe, start, complete, and report loading or validation failure |
| Operation submission and dispatch | Match durable operations and execute worker-local invocations |
| Capability request and response | Provider, MCP, approval, input, and permitted configuration access |
| Stream update and termination | Deliver ordered snapshots, metrics, final values, and terminal status |
| Cancellation and liveness | Cancel invocations, heartbeat, disconnect, and invalidate stale sessions |

Use JSON-safe typed models with explicit discriminators and bounded payloads. Do not use pickle or deserialize executable Python objects. Requests carry correlated IDs, execution/workflow scope, invocation and attempt identity, and parent identity where applicable.

The connection must support concurrent bidirectional traffic: a worker invocation can request runner input or emit nested operations while the runner awaits its result. A single blocking request/response dispatcher would deadlock these cases.

Use authenticated encrypted transport or a secured tunnel for remote hosts. Bearer authentication alone does not protect traffic confidentiality. Worker credentials are scoped, short-lived, and assumed extractable by workflow Python.

Enforce permissions, budgets, concurrency limits, and resource ownership in the runner. Fence stale worker generations and reject cross-run requests. Bound message sizes and buffering so an untrusted worker cannot consume unbounded runner resources.

Credential confidentiality is not authority restriction. An allowed LLM request can transmit readable project data, and an allowed MCP operation can exercise its granted external authority. Network isolation and capability policy must reflect those deliberate disclosures.

Never expose a generic credential resolver or arbitrary runner method invocation. Configuration returned to workers must be projected explicitly; MCP settings can contain inline secrets and credential references.

## Failure and recovery

Use transient RPC with a bounded reconnect window. The runner owns durable execution state; the worker does not maintain a second filesystem-backed execution mailbox.

| Failure state | Required behavior |
| --- | --- |
| Worker dies during workflow-local computation | Replace worker and reconstruct state by replay |
| Outcome is committed | Return recorded outcome; do not execute again |
| Invocation is definitely undispatched | Dispatch when a worker is available |
| Invocation was dispatched without a committed outcome | Mark outcome uncertain and apply its recovery policy |
| Connection drops while worker survives | Attempt bounded reconnect and reconcile outstanding invocations |
| Runner process dies | Recover from runner persistence and reconcile worker sessions before continuing |

Stable invocation IDs, separate attempt IDs, duplicate-result handling, and acknowledgements after durable commitment prevent duplicate history entries. They do not guarantee exactly-once side effects.

An activity can change a file or external system and die before its result reaches the runner. A durable mailbox does not remove that ambiguity unless completion and the effect share an atomic commit. Classify activities as retry-safe, deduplicated by stable operation identity, or requiring reconciliation. Do not assume patches and shell commands are retry-safe.

Treat infrastructure loss separately from a workflow exception. Do not turn a disconnect into an ordinary recorded tool failure or silently finalize a recoverable workflow. Persist interruption and attempt state under runner ownership, with explicit operator-visible recovery status.

Stop accepting new work after session expiry and attempt cancellation of active work. Cancellation is best-effort. Fencing RPC messages cannot stop an already-running shell process; terminate or isolate the old worker before letting a replacement mutate the same storage when effects could overlap.

Worker processes are disposable; project storage is not automatically disposable. Returning a recorded successful patch does not recreate files lost with a container. Provisioning must retain or restore the project filesystem, or report its loss before resume.

## Deployment and provisioning

Initially install the same Python distribution on both sides and expose separate runner and worker entry points. Installing runner code on an untrusted host is not itself an authority grant. Supplying runner secrets, persistence access, or broad machine identity is.

Worker startup must not initialize trusted services or load provider credentials. Importing reusable workflow APIs must not bootstrap runner capabilities. A smaller pip-installable worker package or bundled artifact can follow once the dependency boundary is stable.

The operator selects in-process or remote execution explicitly. Keep execution selection separate from `TargetEnvironment`: one selects where workflow Python executes, while the other supplies local operational interfaces inside that environment.

Docker, sandbox, VM, and EC2 provisioning are adapters or external operations, not protocol responsibilities. A manually provisioned worker is supported without requiring the runner to create infrastructure. Protect host mounts, cloud metadata, instance roles, network access, and runner storage through deployment controls.

No configuration field names or new CLI flags are finalized here. Configuration must define mode, connection/security settings, workspace mapping, timeout policy, and permitted capabilities without changing authority based on worker-supplied configuration.

## Compatibility and migration

Preserve existing activity kinds, versions, inputs, outcomes, and delivery semantics where possible. Do not add transport-specific history operations merely to model connection messages. Any additive persisted interruption or attempt metadata needs corresponding storage conversion and recovery tests.

Public workflow decorators and common tool factories should remain usable in both modes. Direct access to `RunnerProject`, authentication services, mailbox implementations, database handles, and injected live provider objects requires explicit adaptation. Arbitrary custom Python providers execute in the worker or become separately registered trusted runner extensions; they cannot be transparently shipped to the runner.

Use direct adapters for in-process mode, not mandatory serialization overhead. Run the same contract suite against direct and RPC adapters to prevent semantic drift. Keep wire-model validation at remote boundaries even when direct adapters use richer local types internally.

Start with matching runner/worker releases and explicit protocol-version checks. Reject incompatible peers clearly. Broad cross-version compatibility is not part of the initial delivery.

## Implementation plan and estimate

| Phase | Deliverable | Estimate |
| --- | --- | --- |
| 1. Extract ownership contracts | Separate worker invocation, runner session, project projection, and capability APIs; retain in-process behavior | 1-2 weeks |
| 2. Add remote execution | Typed bidirectional protocol, worker entry point, discovery/loading, ordinary activities, local tools | 2-3 weeks |
| 3. Restore feature parity | Streaming, reconstruction, parallel delivery, child workflows, approvals/input, LLM and MCP | 2-3 weeks |
| 4. Harden operations | Disconnect/recovery, stale-session handling, security checks, deployment selection, documentation | 1-2 weeks |
| Total | Tested initial implementation by one engineer | 6-10 engineer-weeks |

These are planning estimates, not measured implementation totals. Expect roughly 25-40 existing source modules touched, new protocol/worker modules, and substantial test additions. The main uncertainty is compatibility with existing direct context access and callback-based extension points.

A 2-3 week proof of concept can demonstrate in-process/RPC parity for a simple workflow, replay, a worker-local tool, and a runner-owned provider capability. It does not establish full isolation or production recovery guarantees.

## Validation and acceptance criteria

- Existing in-process workflow behavior remains covered by the current suite.
- Direct and RPC modes produce equivalent durable operation and delivery histories.
- Parallel activities, replay exhaustion, child joins, and nested workflow-tool reconstruction preserve ordering.
- Streaming handles disconnects, failure after `final()`, cancellation, and backpressure without premature success.
- Isolated discovery, description, validation, and execution never import project Python in the runner.
- Worker requests cannot read credentials, select arbitrary histories, bypass capability grants, or advance stale attempts.
- Approvals and grants survive recovery without worker assertions becoming authoritative.
- MCP and configuration requests do not leak secret-bearing configuration into worker responses or history.
- A complete patch executes beside project files without per-file filesystem RPC.
- Fault tests cover worker OOM/kill, network loss, runner restart, duplicate and late results, and uncertain effects.
- Filesystem loss is distinguished from process loss before replay resumes execution.
- Existing histories retain their replay contracts; intentional storage additions have round-trip and migration tests.

Relevant starting coverage includes `tests/runner/test_session.py`, `tests/runner/test_parallel_replay.py`, `tests/runner/test_replay_failures.py`, `tests/runner/test_sqlite_history.py`, `tests/api/test_tool_policy.py`, `tests/api/test_tool_execution.py`, `tests/patch/`, `tests/mcp/`, and `tests/control/`. Add a dedicated worker protocol and adapter-parity suite rather than treating control-plane reconnect tests as execution recovery coverage.