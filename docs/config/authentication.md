# Runner secrets and provider authentication

The runner stores API keys and OAuth credentials durably outside workspaces. Configure authentication before starting workflows. Login is an explicit control operation, never a workflow wait; missing credentials or authentication failures fail the affected operation and workflow rather than prompting a host. Existing OAuth tokens can refresh automatically without user interaction.

## Quick start

Start a daemon without provisioning a session token:

```bash
uv run vops-runner --project /path/to/workspace daemon serve --port 43821
uv run vops-runner daemon status --port 43821
```

Import a credential from a protected JSON file or a secret manager's stdout, not a command-line argument:

```bash
uv run vops-runner credentials-import < /protected/openai.json
```

The input describes one credential. This example contains a placeholder, not a usable key:

```json
{"provider":"openai","profile":"default","kind":"api_key","secret":"replace-with-api-key"}
```

Import prints only a success status. The source file is not removed or protected by the runner; manage it separately. Authenticated hosts can also call `RunnerClient.provision_credentials(Credential(...))` over the protected control connection.

For ChatGPT OAuth, create a profile and complete browser login before starting work:

```bash
uv run vops-runner client --port 43821 auth profile add work
uv run vops-runner client --port 43821 auth login chatgpt --profile work
```

The connected shell provides `/auth login chatgpt --profile work`. Omitting
`--profile` selects the active profile. `--no-browser` returns a standalone login
request for another host to complete instead of opening the local browser.

## Global configuration

The default directory is `~/.config/vops-runner`. Selection order is explicit `--config-dir` or Python `config_dir`, then `VOPS_RUNNER_CONFIG_DIR`, then the default. The environment override contains a directory path, never a secret.

| File | Contents |
| --- | --- |
| `secrets.json` | Version 1, 32-byte hex `encryption_key`, and URL-safe `control_token` |
| `profiles.json` | Non-secret profile names, active selection, and deleted-profile guards |
| `credentials/<provider>.<profile>.encrypted` | AES-GCM encrypted credential, bound to its provider/profile |
| Lock files | Synchronization only; no secrets |

First use creates the secrets once under a filesystem lock. The directory is mode 0700; secret and credential files are mode 0600. Files are published through atomic replacement and synced. Filesystem locking currently requires Unix and a filesystem supporting `flock`, atomic rename, and fsync.

To predefine the control token, supply a valid `secrets.json` or replace its `control_token` while runners are stopped. Tokens must contain 43-4096 URL-safe characters and should be generated from at least 32 random bytes. Keep the encryption key unchanged. All daemons using this directory share the control token and credential profiles. Use separate directories for isolation.

Explicit daemon `--token-stdin` and `--token-file` options remain supported as per-process overrides; they do not modify the persistent token. Embedded mode and local TCP clients use the persistent token by default. Remote hosts receive the control token separately, never the encryption key.

## Credential profiles and workflow behavior

Authentication profiles group provider credentials under a name. The initial active
profile is `default`. Manage the local global credential directory with:

```bash
uv run vops-runner auth profile list
uv run vops-runner auth profile add work
uv run vops-runner auth profile switch work
uv run vops-runner credentials-import < /protected/work-openai.json
uv run vops-runner auth profile delete work
```

Add `--config-dir PATH` before `auth` to select a different credential directory.
For an existing runner, use `client --port PORT auth profile ...`; its connected
shell accepts `/auth profile list`, `add NAME`, `switch NAME`, and `delete NAME`.
Local commands print JSON; client commands support `--json`. Lists mark the active
profile. Mutations report whether anything changed. Bare client command families
show help without changing state.

Adding a profile does not select it. Switching requires an existing profile.
`default` cannot be deleted. Deleting the active profile selects `default` and
removes locally stored credentials with that profile name across providers; it
does not revoke remote tokens or erase credentials already held by in-flight
requests or connected MCP sessions. Re-adding a deleted profile starts empty.
Deletion is serialized against token publication, and deleted-profile guards
prevent late refresh or provisioning from recreating credentials until explicitly
re-added. Profile names and selection are shared across runners using the same
global credential directory, not stored in workspace history.

Omitting `profile` in credential import/provisioning selects the active profile.
An explicit name targets that profile and registers it if new; a deleted profile
must first be added again. Responses and listings never include credential values.

Omitting `credential_profile` in LLM settings selects the active profile when a
live provider request starts. An explicit `default` or named reference stays
pinned. Switching does not retarget an in-flight transport, refresh, or explicit login;
subsequent requests use the new selection. Completed activities still replay
without credential access. Interrupted unpinned activities use the current active
profile when restarted. MCP's explicit credential references and resource-bound
OAuth selections do not follow the active LLM profile automatically.

Set a non-secret pinned profile reference in LLM configuration:

```toml
[components.llm]
model = "chatgpt/gpt-5.4"
credential_profile = "work"
```

Profile names contain 1-64 ASCII letters, digits, underscores, or hyphens. Provider names are lower-case identifiers. The provider is explicit or taken from the `provider/model` model prefix. Profile references participate in recorded settings; credentials do not.

The default LLM adapter uses the runner's store instead of provider environment variables. Explicit custom `ConnectProvider(auth=...)` remains available. API-key headers support OpenAI/OpenRouter bearer tokens, Anthropic `x-api-key`, and Gemini `x-goog-api-key`; other API-key providers use bearer authentication. ChatGPT is the interactive LLM OAuth provider. MCP public-client OAuth uses the same host coordination with resource-bound adapters; see [MCP credentials and OAuth](mcp.md#credentials-and-oauth).

Missing API keys and missing OAuth credentials fail without asking the host. Expired OAuth credentials can refresh using an already stored refresh token; rejected grants or refresh failures propagate rather than starting login. Token refresh is serialized per profile across runner processes; rotated credentials are saved before refresh succeeds. ChatGPT token requests have bounded timeouts and transient retries; MCP token exchanges have bounded timeouts but are not automatically retried after unknown outcomes. Configure or repair authentication explicitly, then start a new workflow. Completed failed activities retain their recorded failure on replay.

OAuth credentials can also be preconfigured with `kind: "oauth2"`, `secret` (access token), `refresh_token`, `expires_at` (Unix seconds), and optional `account_id`. A supported provider and refresh token are required. ChatGPT requests need the account ID, which can be extracted from the access token.

## Coordinate browser login from a remote host

```python
from vectorops.runner.control import oauth_host

async def login(host):
    request = await host.login("chatgpt", profile="work")
    await oauth_host.authenticate(host, request.id)
```

The helper claims an attempt, starts a callback listener on the host, then opens its browser. For ChatGPT the callback is `http://localhost:1455/auth/callback`. The runner retains the PKCE verifier and exchanges the relayed code itself. The helper never receives provider access or refresh tokens. A custom asynchronous `launch(url)` callback can replace browser launch.

Claims expire after ten minutes. A busy callback port or browser-launch failure releases the claim so another host can retry. Hosts must close an old listener before taking a replacement claim. Disconnect releases an idle claim; an already accepted token exchange finishes independently. `cancel_authentication(id)` removes that standalone login request without affecting workflows. Stale attempts, wrong states, and callbacks from a different connection are rejected.

Repeated login requests for the same provider/profile share a pending request. Login requests live only in the authentication service, with no execution ownership or workflow-recovery behavior. After a runner restart, initiate login again if needed; successfully stored credentials survive. Reconnecting to the same live runner can list and claim existing requests. Starting, stopping, or failing a workflow does not create or cancel login requests. Completed generation activities replay without credential resolution.

## Authentication RPC reference

All methods require control authentication. Parameters and results are not stored in control receipts. Do not log their payloads.

| Method | Parameters | Result |
| --- | --- | --- |
| `auth.providers` | None | Known authentication providers and supported credential methods; no stored credentials |
| `auth.login` | `provider`, optional `profile` (active when omitted) | Standalone login requirement |
| `auth.list` | None | `requests`: ID, provider, profile, waiting/claimed status; `execution_ids` is always empty |
| `auth.claim` | `id` | `id`, `attempt`, `state`, `redirect_uri`, `expires_at` |
| `auth.ready` | `id`, `attempt` | `url`; call after starting the host listener |
| `auth.complete` | `id`, `attempt`, `state`, `code` or `error` | Null after durable token storage, or cancellation |
| `auth.release` | `id`, `attempt` | Null; release the claim without canceling login |
| `auth.cancel` | `id` | Null; cancel standalone login |
| `auth.provision` | Credential object | Null after durable storage |
| `auth.credentials.list` | Optional `profile` (active when omitted), `after` provider, `limit` (1-500, default 100) | `profile`, secret-free `credentials`, `next_after` |
| `auth.credentials.check` | `provider`, optional `profile` (active when omitted) | Local credential status, without provider requests |
| `auth.credentials.delete` | `provider`, optional `profile` (active when omitted) | `provider`, `profile`, `deleted` (false when already absent) |
| `auth.profile.list` | None | `active_profile`, ordered `profiles` (default first) |
| `auth.profile.add` | `name` | `changed`, `active_profile`, `profiles` |
| `auth.profile.switch` | `name` | `changed`, `active_profile`, `profiles` |
| `auth.profile.delete` | `name` | `changed`, `active_profile`, `profiles` |

Typed clients expose `authentication_profiles`, `add_authentication_profile`,
`switch_authentication_profile`, and `delete_authentication_profile`. All profile
operations are local to the runner's global credential store and perform no provider
network requests.

### Discover authentication providers

Call `RunnerClient.authentication_providers()` or `auth.providers` with empty
parameters to populate a frontend provider picker. The result is independent of
the selected profile and existing credentials, and requires no provider or store I/O.

```json
{
  "providers": [
    {"provider": "openai", "display_name": "OpenAI", "credential_kinds": ["api_key"], "oauth_login": false},
    {"provider": "chatgpt", "display_name": "ChatGPT", "credential_kinds": ["api_key", "oauth2"], "oauth_login": true}
  ],
  "custom_api_key_providers": true
}
```

The example shows two entries; results are sorted by provider ID. Built-in entries
include Anthropic, ChatGPT, Google Gemini, MCP, OpenAI, and OpenRouter. OAuth
capabilities reflect the running service's registered adapters. Use `oauth_login`
to offer browser login through `auth.login`, and `credential_kinds` to offer
provisioning through `auth.provision`. These describe accepted credential methods,
not proof that a provider will accept a credential or that a model is available.
`custom_api_key_providers` allows a manually entered provider ID for generic
API-key provisioning; the list is not a validation allowlist.

MCP's entry supports provisioning shared API-key references. Resource-bound MCP
OAuth adapter IDs are not picker entries: discover configured servers with
`mcp.list` and initiate server-specific login through `mcp.login` instead.

### Manage stored credentials

Each provider/profile pair stores one credential. List or check metadata, replace
the complete credential through `auth.provision`, or delete that individual record:

```python
from vectorops.runner.authentication.models import Credential

page = await host.credentials(profile="work", limit=100)
status = await host.check_credential("openai", profile="work")
await host.provision_credentials(Credential(
    provider="openai", profile="work", kind="api_key", secret=new_api_key,
))
result = await host.delete_credential("openai", profile="work")
```

Listing is ordered by provider and includes API keys, OAuth records, and stored
MCP token-cache identities. Pass `next_after` as `after` to fetch another page.
Profiles must exist; omitted profiles resolve to the active selection once per
request. Concurrent mutations can change later pages; this is not a frozen snapshot.

Metadata contains only `provider`, `profile`, `status`, nullable `kind`, nullable
`expires_at` (Unix seconds), and `has_refresh_token`. No secret values, token
fragments, account IDs, or storage paths are returned. Listing and checking never
refresh tokens, initiate login, or contact a provider. Local status means:

| Status | Meaning |
| --- | --- |
| `missing` | No credential is stored for this provider/profile |
| `unreadable` | The entry could not be read, decrypted, or validated; replace or delete it |
| `present` | The credential is readable and not locally expired; an API key with no expiry also has this status |
| `expired` | The recorded expiration time has passed |
| `expiring` | An OAuth token expires within 60 seconds and would need refresh before use |
| `unknown_expiry` | An OAuth record has no known expiration time |

`present` does not prove that a provider accepts the key. `has_refresh_token` reports
presence only, not whether refresh will succeed. Global configuration failures are
errors rather than missing credentials. Storage-operation failures return a sanitized
`credential_storage_error` without filesystem details.

Provisioning replaces the entire credential, not selected fields, and returns no
secrets. Individual deletion removes only the selected provider/profile record;
profile deletion removes all locally stored credentials for that profile. Neither
operation revokes tokens remotely, clears already loaded transports, or cancels
independent login requests. Cancel pending login before deleting if it must not
publish a new credential later. Deletion is serialized with credential publication.

`auth.required`, `auth.completed`, and `auth.cancelled` events contain only ID, provider, and profile. Subscribe without an execution filter and reconcile with `auth.list`; events are advisory. Authentication conflicts return `authentication_error`. Malformed secret-bearing input returns a generic `invalid_argument`, without input values. Completion is not durably idempotent: after a lost response, query current requirements rather than replaying an old code.

## Protection and recovery boundaries

Encryption protects against copying credential files alone and accidental workspace/history disclosure. It does not protect against same-user code execution or a compromised runner. Backups containing both `secrets.json` and the credential directory contain plaintext-equivalent secrets and require protection.

Never delete or regenerate the encryption key to fix a startup error. Restore the original key and credential files together. A missing key with existing credential files, malformed configuration, wrong key, or corrupt ciphertext produces an error, with no plaintext fallback. Encryption-key rotation is not automated in v1.

Authentication does not encrypt TCP. Use a host-key-verified SSH tunnel for remote connections; direct remote plaintext connections remain unsupported. OAuth callback HTTP is loopback-local on the host, not exposed on the runner.

## Related

- [MCP credentials, pinned issuers, and public-client OAuth](mcp.md#credentials-and-oauth)
- [Daemon and SSH usage](../usage/daemon.md)
- [Control protocol](../reference/control-protocol.md)
- [Component configuration](components.md)