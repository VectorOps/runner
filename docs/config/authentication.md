# Runner secrets and provider authentication

The runner stores API keys and OAuth credentials durably outside workspaces. It owns OAuth token exchange and refresh; a connected host is needed only for browser interaction.

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

## Global configuration

The default directory is `~/.config/vops-runner`. Selection order is explicit `--config-dir` or Python `config_dir`, then `VOPS_RUNNER_CONFIG_DIR`, then the default. The environment override contains a directory path, never a secret.

| File | Contents |
| --- | --- |
| `secrets.json` | Version 1, 32-byte hex `encryption_key`, and URL-safe `control_token` |
| `credentials/<provider>.<profile>.encrypted` | AES-GCM encrypted credential, bound to its provider/profile |
| Lock files | Synchronization only; no secrets |

First use creates the secrets once under a filesystem lock. The directory is mode 0700; secret and credential files are mode 0600. Files are published through atomic replacement and synced. Filesystem locking currently requires Unix and a filesystem supporting `flock`, atomic rename, and fsync.

To predefine the control token, supply a valid `secrets.json` or replace its `control_token` while runners are stopped. Tokens must contain 43-4096 URL-safe characters and should be generated from at least 32 random bytes. Keep the encryption key unchanged. All daemons using this directory share the control token and credential profiles. Use separate directories for isolation.

Explicit daemon `--token-stdin` and `--token-file` options remain supported as per-process overrides; they do not modify the persistent token. Embedded mode and local TCP clients use the persistent token by default. Remote hosts receive the control token separately, never the encryption key.

## Credential profiles and workflow behavior

The default profile is `default`. Set a non-secret profile reference in LLM configuration:

```toml
[components.llm]
model = "chatgpt/gpt-5.4"
credential_profile = "work"
```

Profile names contain 1-64 ASCII letters, digits, underscores, or hyphens. Provider names are lower-case identifiers. The provider is explicit or taken from the `provider/model` model prefix. Profile references participate in recorded settings; credentials do not.

The default LLM adapter uses the runner's store instead of provider environment variables. Explicit custom `ConnectProvider(auth=...)` remains available. API-key headers support OpenAI/OpenRouter bearer tokens, Anthropic `x-api-key`, and Gemini `x-goog-api-key`; other API-key providers use bearer authentication. ChatGPT is the v1 interactive OAuth provider.

Missing API keys fail the affected workflow without asking the host. OAuth without usable credentials suspends the affected activity until authorization completes or is canceled. Other activities can continue. Token refresh is serialized per profile across runner processes; rotated credentials are saved before refresh succeeds. Token requests have bounded timeouts and transient retries. Invalid grants require login; storage failures and other token failures are not treated as missing credentials.

OAuth credentials can also be preconfigured with `kind: "oauth2"`, `secret` (access token), `refresh_token`, `expires_at` (Unix seconds), and optional `account_id`. A supported provider and refresh token are required. ChatGPT requests need the account ID, which can be extracted from the access token.

## Coordinate browser login from a remote host

```python
from vectorops.runner.control import oauth_host

async def authenticate_pending(host):
    for request in await host.pending_authentication():
        await oauth_host.authenticate(host, request.id)
```

The helper claims an attempt, starts a callback listener on the host, then opens its browser. For ChatGPT the callback is `http://localhost:1455/auth/callback`. The runner retains the PKCE verifier and exchanges the relayed code itself. The helper never receives provider access or refresh tokens. A custom asynchronous `launch(url)` callback can replace browser launch.

Claims expire after ten minutes. A busy callback port or browser-launch failure releases the claim without canceling the workflow. Hosts can retry, but must close an old listener before taking a replacement claim. Disconnect releases an idle claim; an already accepted token exchange finishes independently. User cancellation through `cancel_authentication(id)` fails all activities waiting on that shared authentication requirement. Stale attempts, wrong states, and callbacks from a different connection are rejected.

Authentication requirements are reconstructed from interrupted activities after restart, not stored in mailbox or saga records. Reconnect and query them again. Clean shutdown while waiting preserves execution for automatic recovery; explicit execution stop cancels it. Completed generation activities replay without credential resolution.

## Authentication RPC reference

All methods require control authentication. Parameters and results are not stored in control receipts. Do not log their payloads.

| Method | Parameters | Result |
| --- | --- | --- |
| `auth.list` | None | `requests`: ID, provider, profile, waiting/claimed status, execution IDs |
| `auth.claim` | `id` | `id`, `attempt`, `state`, `redirect_uri`, `expires_at` |
| `auth.ready` | `id`, `attempt` | `url`; call after starting the host listener |
| `auth.complete` | `id`, `attempt`, `state`, `code` or `error` | Null after durable token storage, or cancellation |
| `auth.release` | `id`, `attempt` | Null; release without canceling waiters |
| `auth.cancel` | `id` | Null; fail all waiters |
| `auth.provision` | Credential object | Null after durable storage |

`auth.required`, `auth.completed`, and `auth.cancelled` events contain only ID, provider, and profile. Subscribe without an execution filter and reconcile with `auth.list`; events are advisory. Authentication conflicts return `authentication_error`. Malformed secret-bearing input returns a generic `invalid_argument`, without input values. Completion is not durably idempotent: after a lost response, query current requirements rather than replaying an old code.

## Protection and recovery boundaries

Encryption protects against copying credential files alone and accidental workspace/history disclosure. It does not protect against same-user code execution or a compromised runner. Backups containing both `secrets.json` and the credential directory contain plaintext-equivalent secrets and require protection.

Never delete or regenerate the encryption key to fix a startup error. Restore the original key and credential files together. A missing key with existing credential files, malformed configuration, wrong key, or corrupt ciphertext produces an error, with no plaintext fallback. Encryption-key rotation is not automated in v1.

Authentication does not encrypt TCP. Use a host-key-verified SSH tunnel for remote connections; direct remote plaintext connections remain unsupported. OAuth callback HTTP is loopback-local on the host, not exposed on the runner.

## Related

- [Daemon and SSH usage](../usage/daemon.md)
- [Control protocol](../reference/control-protocol.md)
- [Component configuration](components.md)