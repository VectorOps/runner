# MCP servers and workflow tools

Connect workflows to Model Context Protocol (MCP) servers over POSIX stdio or Streamable HTTP. The runner records discovery and results, applies existing tool approval, and owns session cleanup. Completed operations replay without contacting servers or resolving credentials.

## Contents

- [Quick start](#quick-start)
- [Server configuration](#server-configuration)
- [Credentials and OAuth](#credentials-and-oauth)
- [Shared pool and management APIs](#shared-pool-and-management-apis)
- [Root defaults and overrides](#root-defaults-and-overrides)
- [Select and call tools](#select-and-call-tools)
- [Dynamic discovery](#dynamic-discovery)
- [Prompts and resources](#prompts-and-resources)
- [Approval and trust](#approval-and-trust)
- [Replay and lifecycle](#replay-and-lifecycle)
- [Limits and troubleshooting](#limits-and-troubleshooting)

## Quick start

Prerequisites: an installed MCP server, an LLM model and credentials configured for the workspace, and a connected host that can answer tool-approval requests. See [component configuration](components.md) and [authentication](authentication.md).

Add a root-level MCP server to `<project>/.voc/config.toml` (or `config.toml` in your custom runner workspace). Replace the executable and project paths with your installed server and working directory; the runner does not install servers.

```toml
[mcp.servers.files]
kind = "stdio"
command = "/opt/mcp/bin/files-server"
args = ["/srv/project"]
cwd = "/srv/project"

[components.tools.approval]
approve_all = false
```

Start a new `builtin:agent` execution through your host or the [runner CLI](../usage/cli.md). Its default collection includes MCP: no collection edits or custom workflow are needed. Without configured servers it adds no MCP tools and opens no MCP connections. The helper's default tools are unchanged.

By default the agent discovers the catalog but exposes only a search tool initially. Matching concrete tools become available on subsequent rounds. With the default approval policy, search and concrete tool calls wait for host confirmation; launching a bare CLI does not supply an approval UI. Inspect pending requests through the [control protocol](../reference/control-protocol.md#workflow-scoped-tool-approval).

Discovery opens the configured server and records its tool catalog. An approved call validates its arguments and checks that the remote descriptor still matches the snapshot before invoking the tool.

## Server configuration

Servers live under `mcp.servers.<name>`. Names contain 1-64 ASCII letters, digits, underscores, or hyphens. Unknown configuration fields are rejected. The legacy `[components.mcp]` form remains supported, but using both root `[mcp]` and `[components.mcp]` in one file is rejected. Root configuration normalizes into the existing `components.mcp` namespace, so management APIs and configuration overrides such as `mcp.mode` continue to address the same settings. `component="other.path"` on discovery APIs selects an alternate component subtree with the same shape.

### Tool exposure

```toml
[mcp]
mode = "dynamic"
max_active = 16

[mcp.servers.docs]
kind = "http"
url = "https://docs.example.com/mcp"
tools = ["search", "fetch"]

[mcp.servers.archive]
kind = "stdio"
command = "/opt/mcp/bin/archive-server"
enabled = false
```

| Field | Default | Meaning |
| --- | --- | --- |
| `mcp.mode` | `"static"` | `static` exposes every selected tool schema immediately; `dynamic` exposes search first |
| `mcp.max_active` | `32` | Dynamic active-tool cap, 1-256, excluding the search tool |
| Server `enabled` | `true` | Include this server in tool discovery; `false` skips it without connecting |
| Server `tools` | `["*"]` | Exact remote tool names or `"*"`; `[]` exposes none |
| Server `exclude` | `[]` | Remote names to exclude; `"*"` excludes all |

Server filters bound explicit workflow/collection tool selections too. They do not disable administrative connect operations or prompt/resource APIs. Exposure settings do not change transport identity or namespaced tool names.

All selected schemas are exposed immediately by default. To opt into search-first exposure, set `mode = "dynamic"` under `[mcp]`, as in the example above. Dynamic mode still connects and records catalogs up front; it limits model context, not server startup. If no tools remain after selection, the configured collection exposes no MCP search tool either.

The built-in agent loads its collection once per execution. Start a new execution to apply changes. Dynamic activation starts search-only on each user turn; the catalog remains the recorded snapshot. Existing version-2 agent executions keep their original static collection behavior when resumed. Explicit collection `tools` lists still replace defaults: include `"mcp"` if you customize the agent's list.

For custom workflows, use `tools.load_tool_set(...)` and `LLM.run(tool_set=...)` to honor the mode toggle. The existing `tools.load_collection(...)` API remains static. See [tool collections](tool-collections.md).

MCP uses a validated configuration read: only accepted, normalized settings enter history. Validation failures have payload-free messages. This is not a secret scanner: values in accepted command arguments, paths, names, or descriptions are still recorded. Never put secrets in them.

### Stdio

| Field | Default | Meaning |
| --- | --- | --- |
| `kind` | `stdio` on the Python model; specify in TOML | Transport discriminator |
| `command` | Required | Executable, launched without a shell |
| `args` | `[]` | Command arguments; not suitable for secrets |
| `cwd` | Unset | Absolute runner-host directory; unset inherits the runner's working directory |
| `env_credentials` | `{}` | Environment-variable name to provisioned MCP credential-profile reference |
| `env` | `{}` | Ordinary non-secret environment values; credential references override matching values |
| `roots` | `[]` | Advisory MCP roots, described below |
| `limits` | Built-in defaults | See [limits](#limits-and-troubleshooting) |

Stdio runs on the **runner host**, not inside the attached target environment. It requires POSIX and uses a separate process group. Environment precedence is the MCP SDK's default allowlist, then `env`, then `env_credentials`; the child does not inherit the full runner environment. There is no shell interpolation. Obvious secret-variable names containing `TOKEN`, `SECRET`, `PASSWORD`, `API_KEY`, or `PRIVATE_KEY` are rejected in `env`, but this is not a complete secret detector. All secrets belong in the credential store. Server stderr is discarded, and stdout must contain MCP JSONL messages only.

### Streamable HTTP

```toml
[mcp.servers.remote]
kind = "http"
url = "https://mcp.example.com/mcp"
credential = "remote-api"
```

| Field | Default | Meaning |
| --- | --- | --- |
| `kind` | `http` on the Python model; specify in TOML | Streamable HTTP transport, not legacy HTTP+SSE |
| `url` | Required | Exact endpoint URL; HTTPS except for loopback HTTP |
| `credential` | Unset | Provisioned MCP API-key profile, sent as a Bearer token |
| `oauth` | Unset | Public-client OAuth settings; mutually exclusive with `credential` |
| `auth` | Unset | Discriminated authentication strategy; cannot be combined with legacy `credential` or `oauth` |
| `headers` | `{}` | Ordinary non-secret HTTP headers |
| `header_credentials` | `{}` | Header name to MCP credential-profile reference, using the stored value verbatim |
| `roots` | `[]` | Explicit advisory roots |
| `limits` | Built-in defaults | See [limits](#limits-and-troubleshooting) |

URLs must have a hostname and no embedded username/password, query, or fragment. Loopback exceptions are `localhost`, `127.0.0.1`, and `::1`. The HTTP client does not follow redirects or inherit proxy settings from the environment. Compressed MCP HTTP responses are rejected. Requests use the runner host's network; this is not a network sandbox.

HTTP sessions support server-issued session IDs, JSON and SSE responses, server requests during response streaming, and session termination. The client advertises roots support, including an empty root list, so idle sessions can accept root updates. Roots describe intended access, not enforced containment. Do not disclose runner-host paths to remote servers unless intended.

Header names are case-insensitive for collision checks. Protocol-owned headers (including Host, Content-Length, Content-Type, Accept, and MCP session/version headers) cannot be overridden. Common secret headers such as Authorization, Cookie, and X-Api-Key require credential references or an authentication strategy, not literal `headers`. Authentication must not target a header already configured elsewhere. Custom header values must be printable ASCII; arbitrary custom header names are not a secret scanner.

## Credentials and OAuth

### Provisioned API keys

Import an MCP credential using the existing credential command:

```bash
uv run vops-runner credentials-import < /protected/mcp-credential.json
```

The protected file contains a credential such as this placeholder:

```json
{"provider":"mcp","profile":"remote-api","kind":"api_key","secret":"replace-with-api-key"}
```

Use that profile as HTTP `credential`, or map it into a stdio environment variable:

```toml
[mcp.servers.local]
kind = "stdio"
command = "/opt/mcp/bin/private-server"

[mcp.servers.local.env_credentials]
SERVICE_TOKEN = "remote-api"
```

Credentials are resolved from the encrypted runner store during live execution, not from component settings or tool arguments. Missing, expired, or wrong-kind credentials fail without prompting. Provisioning a key does not grant approval to call tools. API-key profiles are deliberately reusable references; unlike OAuth profiles, their storage identity is not automatically bound to a destination.

### Public-client OAuth

Register a public client with the server's authorization provider first. Configure its client ID, the exact resource, an explicitly trusted issuer, and a loopback redirect URI matching the registration:

```toml
[mcp.servers.remote]
kind = "http"
url = "https://mcp.example.com/mcp"

[mcp.servers.remote.oauth]
issuer = "https://login.example.com"
resource = "https://mcp.example.com/mcp"
client_id = "registered-public-client"
redirect_uri = "http://127.0.0.1:1456/callback"
scopes = ["read"]
profile = "work"
```

`issuer`, `resource`, `client_id`, and `redirect_uri` are required. `resource` must exactly equal the MCP endpoint URL. `scopes` defaults to an empty list and is normalized to sorted unique tokens; at most 64 tokens are accepted. `profile` defaults to `default` and uses the same 1-64-character profile syntax as API keys. The callback requires loopback HTTP, an explicit nonzero port, and a path. There is no client secret setting.

The runner discovers protected-resource metadata at `/.well-known/oauth-protected-resource` followed by the resource path, and issuer metadata at `/.well-known/oauth-authorization-server` followed by the issuer path. It requires exact resource/issuer matches, an authorized configured issuer, PKCE S256, and authorization/token endpoints on the issuer's origin. Metadata and token redirects are rejected. Challenge-provided metadata URLs do not replace configured trust. Arbitrary query-bearing endpoints and alternative metadata discovery layouts are not supported by this implementation.

Complete login explicitly before starting a workflow:

```python
from vectorops.runner.control import oauth_host


async def login(host):
    requirement = await host.mcp_login("remote")
    await oauth_host.authenticate(host, requirement.id)
```

The host claims a requirement, listens on its own loopback interface, opens the browser, and relays the authorization code. The runner retains the PKCE verifier and tokens. MCP requirements use an opaque `mcp-oauth-...` provider identifier, derived from settings and validated endpoints; hosts must not assume all requirements are for ChatGPT. See the shared [authentication RPC reference](authentication.md#authentication-rpc-reference).

Encrypted credentials are bound to the resource, issuer, client, callback, scopes, profile, and validated endpoints. Changing those inputs selects a different credential identity. Refresh is serialized through the existing credential store, and rotated tokens finish publication before cancellation propagates. Workflows and connection operations never initiate or wait for interactive authentication; missing credentials and failed refresh fail the operation.

A Bearer 401 permits one non-interactive refresh and HTTP retry. A second rejection is returned without another retry. Invalid refresh grants require a separate explicit login before starting new work. The runner does not open a browser from workflow execution or automatically escalate scopes after a 403. Token exchanges are bounded to 30 seconds and 64 KiB and are not automatically retried after unknown outcomes. Token responses must be Bearer credentials with a finite `expires_in` greater than 60 seconds; a returned scope must include configured scopes. Refresh tokens are optional, but expiry without one requires login again.

Dynamic client registration, confidential authorization-code clients, automatic scope escalation, and cross-origin authorization/token endpoints for metadata-discovered public clients are not implemented.

### Configurable authentication and client credentials

New configuration can use `auth.kind`: `none`, `header`, `authorization_code`, or `client_credentials`. Existing `credential` and `oauth` settings remain supported, but cannot be combined with `auth`. `authorization_code` accepts the same fields as `oauth` above, plus its discriminator.

For a custom API-key header:

```toml
[mcp.servers.remote]
kind = "http"
url = "https://mcp.example.com/mcp"
headers = { X-Tenant = "engineering" }

[mcp.servers.remote.auth]
kind = "header"
credential = "remote-api"
header = "X-Api-Key"
prefix = ""
```

`header` defaults to `Authorization` and `prefix` to `Bearer `, including the space. The stored secret is appended to that prefix. Header authentication does not retry rejected keys.

For service-account authentication, provision the client secret as an API-key credential under provider `mcp`, then reference its profile:

```toml
[mcp.servers.remote]
kind = "http"
url = "https://mcp.example.com/mcp"

[mcp.servers.remote.auth]
kind = "client_credentials"
token_endpoint = "https://login.example.com/token"
resource = "https://mcp.example.com/mcp"
client_id = "registered-service-client"
client_secret = "service-client-secret"
token_auth_method = "client_secret_basic"
scopes = ["read"]
profile = "work"
```

`client_secret` is a credential-store reference, never the secret itself. The token endpoint is explicitly trusted configuration, uses HTTPS except on loopback, and is never redirected. `resource` must exactly match the MCP URL. `token_auth_method` is `client_secret_basic` (default) or `client_secret_post`; scopes default empty and profile defaults to `default`. This flow does not discover metadata or prompt a host. Missing secrets and rejected grants fail with sanitized errors.

Access tokens are encrypted and cached under a configuration/resource-bound identity. Concurrent acquisitions share the cache and profile lock. Expiry or one Bearer 401 obtains another client-credentials token rather than using a refresh token. Tokens must declare Bearer type and a finite lifetime greater than 60 seconds. The original client secret remains in its own profile when the cached token is logged out.

## Shared pool and management APIs

The daemon and embedded control service own one shared pool for management and workflows. Matching effective server configurations reuse the same process/HTTP session, including one opened through `mcp.connect` after login. Sessions stay open until explicit disconnect or service shutdown; there is no idle eviction yet. A standalone `WorkflowRunner` without an injected pool retains execution-owned cleanup.

Workflow runtimes lease each session they use until the root and its children finish. Management discovery leases it for the duration of the operation. Management cannot disconnect or change roots while a session is leased. Sharing transport does not share tool selections, conversations, approval grants, or recorded catalogs. It can share state maintained by the remote server; choose different effective configurations when transport-level isolation is required.

Python hosts use typed `RunnerClient` methods over embedded or authenticated TCP connections:

```python
from vectorops.runner.control.client import RunnerClient


async def inspect_servers(port):
    async with await RunnerClient.tcp(port) as host:
        listing = await host.mcp_servers()
        await host.mcp_connect("remote")
        catalog = await host.mcp_discover("remote")
        status = await host.mcp_status("remote")
        return listing, catalog, status
```

`mcp_login` returns a standalone OAuth requirement for the configured server/profile, without connecting an MCP session. Complete it with the host helper before `mcp_connect` or starting workflows. `mcp_connect`, `mcp_login`, `mcp_discover`, `mcp_refresh`, and `mcp_content` default to a 600-second client wait and accept `timeout`. A client timeout is not proof an operation failed; query status before retrying. Connections established before a host disconnect remain pooled. Shutdown cancels outstanding management operations before closing the pool.

All server-targeted RPCs accept `server` and optional `component` (default `mcp`). No credentials are accepted in these parameters. They operate on trusted current workspace settings, not model-supplied executable definitions.

| RPC | Typed client method | Result |
| --- | --- | --- |
| `mcp.list` | `mcp_servers(component="mcp")` | Configured names, transports, and session status |
| `mcp.status` | `mcp_status(server)` | Session digest, connected flag, lease count, roots, catalog-stale flag |
| `mcp.connect` | `mcp_connect` | Initialize a reusable session with preconfigured credentials |
| `mcp.login` | `mcp_login` | Standalone OAuth login requirement for the configured server/profile |
| `mcp.disconnect` | `mcp_disconnect(server)` | Close an unleased session without deleting credentials |
| `mcp.discover`, `mcp.refresh` | `mcp_discover`, `mcp_refresh` | Fresh tool catalog; does not change workflow snapshots |
| `mcp.content` | `mcp_content(server)` | Fresh prompt/resource catalog |
| `mcp.roots.set` | `mcp_roots(server, roots)` | Updated session status; null roots restores configuration defaults |
| `mcp.logout` | `mcp_logout(server)` | Delete the selected local token after closing idle pooled connections |

The corresponding owner API is `RunnerService.mcp` (`Management`), with typed `Query`, `RootUpdate`, and result models in `vectorops.runner.mcp.management`. Management is not a recorded workflow API and has no durable idempotency receipts. Repeated connect reuses a session; repeated discovery refreshes it. MCP failures return `mcp_error`, malformed parameters return payload-free `invalid_argument`, and new operations are rejected during shutdown. Oversized catalogs are rejected before control transmission rather than silently truncated.

Logout currently requires **all pool leases** to be released and closes all pooled connections, because profiles can be reused across configurations. It removes the selected OAuth/client-credentials access-token cache, or the selected ordinary Bearer/header API-key profile. It does not delete client-secret references or arbitrary environment/header credential references, revoke tokens remotely, or remove tokens held by another daemon. Cancel pending interactive authentication using existing `auth.cancel` before logout.

### Catalog change notifications

MCP `notifications/tools/list_changed` means the server's tool list changed; it is not a new catalog payload. For a server advertising this capability, the pool marks its catalog stale and emits `mcp.catalog_changed` with `session_id`. Subscribe without an execution filter and reconcile with status queries; events are advisory. No background refresh or workflow activation happens. Explicit management refresh or recorded workflow discovery fetches the new list. Existing recorded bindings stay immutable and live tool calls still check their descriptors.

## Root defaults and overrides

Configure shared defaults at `mcp.roots`. A root accepts exactly one absolute `path` or local `file:` `uri`, plus optional `name`. Paths normalize to URIs before recording. Per-server `roots` replace defaults when explicitly supplied, including an empty list; omitted roots inherit defaults. `roots_mode="merge"` combines defaults and server roots, deduplicates by URI, and lets the server entry win.

```toml
[mcp]
roots = [{ path = "/srv/project", name = "project" }]

[mcp.servers.files]
kind = "stdio"
command = "/opt/mcp/bin/files-server"
roots_mode = "merge"
roots = [{ path = "/srv/shared", name = "shared" }]
```

`mcp_roots`/`mcp.roots.set` installs a session-scoped override without editing `config.toml`. An empty list clears roots; null restores configured defaults. Updates require an unleased session, send `notifications/roots/list_changed` to an active server, and change its effective session identity. Newly recorded discovery captures the effective roots. Completed replay never re-applies management overrides; old catalogs remain pinned to their recorded configuration. Overrides disappear on daemon restart, and roots remain advisory rather than a filesystem sandbox.

## Select and call tools

`await mcp.discover(selections, component="mcp")` returns concrete `ToolBinding` objects from recorded catalogs. Each `mcp.Selection` has `server`, `tools` (default `["*"]`), and `exclude` (default `[]`). Matching is exact and case-sensitive except that a complete `"*"` matches all tools. Other glob syntax is not supported. Exclusions take precedence within a selection; multiple selections are combined as a union, so another selection can permit a tool excluded by one. Use one selection per server for an unambiguous exclusion policy.

```python
from vectorops.runner.api import llm, mcp


async def main(ctx):
    bindings = await mcp.discover([
        mcp.Selection(server="files", tools=["read_file", "list_directory"]),
    ])
    agent = llm.LLM("reader")
    agent.add_user_message("Read the project overview.")
    return (await agent.run(tools=bindings)).model_dump(mode="json")
```

An empty selection list returns no bindings. `tools=[]` exposes none from that selection. Unknown configured server names fail rather than silently selecting another server. A requested remote tool absent from the catalog is not bound.

Exposed tool names are generated from the server alias, original tool name, and a configuration digest. Use `binding.definition.name`; do not reconstruct names or hard-code their format. The original remote name is retained for dispatch. Changes to server settings can change names and invalidate exact-name approval grants.

Input schemas use JSON Schema 2020-12, require `type="object"`, and are validated locally without network reference retrieval. Tool descriptors are rechecked before live invocation. Output schemas, when supplied, validate successful structured results. Text and supported images become native content; structured results are retained as labeled JSON text. Other MCP blocks use explicit JSON fallbacks. These are untrusted server results, not system instructions.

## Dynamic discovery

The default agent uses static exposure unless `mcp.mode = "dynamic"` is configured. In a custom workflow, use `mcp.dynamic` for explicit selections when exposing every tool schema would make the initial LLM request too large:

```python
from vectorops.runner.api import llm, mcp


async def main(ctx):
    selected = await mcp.dynamic(
        [mcp.Selection(server="files")], max_active=16,
    )
    agent = llm.LLM("researcher")
    agent.add_user_message("Find tools to inspect the project, then use them.")
    return (await agent.run(tool_set=selected)).model_dump(mode="json")
```

The workflow still discovers and records the permitted catalog up front. Only a search tool is initially exposed to the model. Search runs against this local snapshot, not against a live server. Its arguments are `query` (default empty, at most 512 characters) and `limit` (default 5, range 1-20). Search ranks case-insensitive keyword matches in exposed names and descriptions, with tool-name ordering for ties. It returns bounded names/descriptions; concrete schemas appear when activated on the next round.

For richer search, pass `ranking=mcp.Ranking()` to `dynamic`. This opt-in version preserves existing simple-search replay contracts. It scores original remote names, titles, descriptions, and serialized schemas with weights 4, 2, 1, and 0.5 respectively. `min_score` defaults to 0; all weights and thresholds are finite nonnegative values. The search tool additionally accepts exact `source` filtering. Only the already-selected catalog is searched; source filters cannot broaden permissions. Custom ranking values and catalog metadata participate in the discovery identity.

Successful recorded search results advance an immutable tool set only after all calls in that round settle. A newly found tool called in the same round is still unknown. Error or rejected search results do not activate tools; strict round failures and cancellation do not advance the set. Parallel results are applied in model-call order, not completion order.

`max_active` defaults to 32, ranges from 1 through 256, and excludes the search tool itself. Search results are also capped by `max_active`. Later search matches move to the newest end of the activation order; when the cap is exceeded, the oldest activations are dropped. Ordinary tool use does not update that order. At most 4096 bindings can enter one dynamic catalog. Reusing the original `selected` object in another `LLM.run` starts search-only again; it is not mutated by the previous run. Independent LLMs can share it without sharing activation state.

Static `tools=[...]` and `tool_set=selected` can be combined. Duplicate exposed names are rejected, including collisions introduced by activation. A successful search does not approve its matches: every concrete tool retains normal approval and mandatory checks. Call `mcp.dynamic(...)` again explicitly for a fresh catalog; notifications never mutate the recorded snapshot.

## Prompts and resources

`discover_content` records a separate catalog without changing ordinary tool-discovery contracts. Selections are exact prompt names and resource URIs, or explicit `"*"`. Both selection lists default empty and grant no access.

```python
from vectorops.runner.api import mcp


async def main(ctx):
    selected = await mcp.discover_content(
        "files",
        prompts=["review"],
        resources=["file:///srv/project/README.md"],
    )
    prompt = await selected.get_prompt("review", {"language": "English"})
    resource = await selected.read_resource("file:///srv/project/README.md")
    return {
        "prompt": prompt.model_dump(mode="json"),
        "resource": resource.model_dump(mode="json"),
    }
```

Replace the names and prompt arguments with those your server advertises. `selected.catalog` returns a detached snapshot. Required prompt arguments must be supplied as strings, and undeclared arguments are rejected. Only selected, listed resources can be read; a URI is interpreted by the server, not by the runner's filesystem or HTTP client.

Prompt results contain `description` and ordered `messages`, each with a `user` or `assistant` role and content blocks. They are not automatically appended to the LLM and never become system instructions. Resource results contain ordered `contents`, each with `uri`, `mime_type`, and content blocks. PNG/JPEG/GIF/WebP images use native image blocks; other binary resources retain labeled base64. Resource links remain data and are not fetched as a conversion side effect. Assistant images can remain in prompt result data even when a model provider cannot accept them as assistant-message content.

Direct workflow retrieval is trusted Python orchestration and does not ask for tool approval. To let the model retrieve selected content, pass `selected.tools()` to `LLM.run(tools=...)`. These bindings use ordinary approval and require `source`, `identity` (prompt name or URI), and optional string-valued `arguments`. They validate source and selection before approval, so supplying a different source cannot bypass the catalog. Changed prompt/resource descriptors are rejected before retrieval.

Resource templates, unlisted URI reads, subscriptions, and automatic prompt injection are not implemented.

For common workflow operations, shorter recorded helpers are available: `await mcp.servers()`, `await mcp.tools("files", "read_file")`, `await mcp.list_prompts("files")`, `await mcp.list_resources("files")`, `await mcp.get_prompt("files", "review", {"language": "English"})`, and `await mcp.read_resource("files", uri)`. All accept `component=`. Omitting tool names in `mcp.tools` selects all tools on that server. Retrieval helpers keep the server explicit to avoid ambiguity and reuse the same selection, validation, and replay contracts as the catalog API.

## Approval and trust

MCP tools, dynamic search, and prompt/resource tool bindings use the [existing tool policy](components.md#tool-approval). Selection controls what is available, not whether it is approved. Read-only annotations do not auto-approve tools. Workflow-scoped grants match exact exposed names, do not inherit into children, and do not grant search matches merely because search was approved.

Server launch, metadata access, and catalog discovery run as trusted workflow configuration before tool-call approval. Tool approval is not permission to install or launch an arbitrary server supplied by model arguments. Review workspace configuration and server binaries separately.

Arguments and results enter workflow history, exports, and potentially the model conversation. Do not send secrets as prompt arguments, tool arguments, URI query parameters, or command arguments. Server responses can themselves contain sensitive data; the runner does not redact legitimate tool output before recording it. OAuth credentials and designated secret references are handled separately from saga history.

## Replay and lifecycle

Within the daemon/embedded control service, root executions and management share matching sessions; each root releases its leases after activities and children stop. Session cleanup happens on explicit unleased disconnect or service shutdown, not when the first workflow finishes. Standalone runners without a shared pool retain root-owned sessions. Session reuse is keyed by effective server configuration, not merely a display alias. Stdio cleanup targets the process group; deliberately detached descendants and hard runner crashes are outside the guarantee.

Discovery snapshots include remote identities, schemas, and non-secret configuration. Completed configuration, discovery, tool, and content operations replay without server I/O or credential acquisition. Dynamic activation reconstructs from recorded search outcomes. Fresh discovery reads current configuration; editing workspace settings does not redirect already-recorded bindings. Keep workflow code, selectors, and activity order stable when replaying existing history.

Completed committed remote calls are not repeated. An interrupted call can restart after the remote side effect occurred but before its result was committed. Timeout or disconnect can therefore leave an unknown remote outcome. MCP provides no generic exactly-once guarantee, and the runner does not inject idempotency arguments into server schemas. Tool mutations are not automatically retried after such failures; the bounded OAuth retry applies only to a rejected Bearer 401.

## Limits and troubleshooting

Configure per-server limits with `[mcp.servers.<name>.limits]`:

| Field | Default | Allowed range and use |
| --- | --- | --- |
| `startup_seconds` | 15 | Greater than 0, at most 300; transport/session startup, excluding credential resolution |
| `request_seconds` | 30 | Greater than 0, at most 3600; request or paginated catalog operation |
| `shutdown_seconds` | 10 | Greater than 0, at most 60; owned teardown budget |
| `max_bytes` | 1048576 | 1024-16777216; stdio frames, HTTP response streams, catalog/result and relevant request checks |
| `max_tools` | 256 | 1-4096; tools per catalog, or combined prompts/resources per content catalog |
| `max_pages` | 32 | 1-256; pagination bound per listing |

All durations must be finite. Repeated cursors and duplicate identities fail discovery. Oversized data fails rather than being silently truncated. Limits are transport/catalog safeguards, not a sandbox or a universal process-memory cap. HTTP stream accounting includes SSE traffic, so a long-running notification stream can exhaust its byte limit. SDK cancellation does not guarantee a server has stopped work.

| Symptom | Check or action |
| --- | --- |
| Authentication fails before discovery completes | Provision credentials or complete explicit `mcp_login` before starting a new workflow |
| Workflow waits on a tool step | Inspect the `tool.approval` mailbox request; authentication and approval are separate |
| Component configuration is invalid | Check field names, discriminator, URL rules, absolute cwd, and mutually exclusive `credential`/`oauth`; errors deliberately omit input values |
| Stdio initialization fails | Check the executable on the runner host and run it separately; stdout must be protocol-only and runner stderr capture is disabled |
| OAuth metadata is rejected | Check exact resource/issuer strings, metadata paths, S256 support, same-origin endpoints, and redirect-free responses |
| Tool or content descriptor changed | Explicitly discover a fresh catalog for new work; do not substitute schemas during replay |
| Tool found but unknown in the same round | Dynamic activation takes effect on the next round |
| Call fails with an unknown outcome | Inspect remote state before attempting another mutation |
| HTTP response exceeds a limit | Adjust `max_bytes` within its range or reduce the server response; compressed responses are unsupported |

The integration uses a pinned MCP SDK and supports the subset described here. It does not provide legacy HTTP+SSE transport, target-side Docker/SSH launch, MCP sampling/elicitation handlers, task APIs, or a general conformance guarantee for every server. Tests use local protocol and OAuth fixtures, not live third-party services.

## Related

- [Component configuration and tool approval](components.md)
- [Runner secrets and authentication RPCs](authentication.md)
- [Workflow authoring](../development/workflows.md)
- [Target environments](../development/target-environments.md)
- [Control protocol](../reference/control-protocol.md)