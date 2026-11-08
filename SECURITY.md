# Security

VectorOps Runner executes trusted Python workflows and can expose powerful tools
to models. Tool approval is a permission check, not an operating-system sandbox.

## Run only trusted code

- Workflow discovery imports Python modules. Inspect workflow code before loading it.
- Local commands run with the runner account's permissions. A working directory
  or Git worktree does not confine filesystem or network access.
- File tools can access absolute and home-relative paths outside the target project.
- Shell commands are disabled by default, but an argv command can explicitly invoke
  a shell. Use an OS-level isolation boundary when handling untrusted execution.
- MCP server binaries and configuration are trusted setup. Server startup and
  catalog discovery happen before individual tool-call approval.
- Skills, tool results, and web pages are content from their sources. They do not
  grant permissions or override the user's task.

## Protect credentials and history

Store provider credentials in the [runner credential store](docs/config/authentication.md),
not workflow input, component settings, command arguments, or tool arguments.
History, exports, and model conversations can contain sensitive inputs and outputs.
Keep `.voc/data/` out of version control and protect backups.

Credential encryption does not protect against same-user code execution. A backup
containing both the encryption key and encrypted credentials is equivalent to
having the secrets. Protect the global configuration directory accordingly.

## Protect the control connection

Every authenticated controller has full runner access. Control tokens have no
per-command scopes. Keep the daemon on loopback and use a host-key-verified SSH
tunnel for remote access. Bearer authentication alone does not encrypt traffic or
verify the server's identity. See [daemon connections](docs/usage/daemon.md).

## Report a vulnerability

Use [GitHub private vulnerability reporting](https://github.com/vectorops/runner/security/advisories/new)
if it is enabled for this repository. If it is unavailable, open an issue requesting
a private contact channel without disclosing the vulnerability, credentials, or
an exploit. Wait for a private channel before sharing sensitive details.

Include affected versions, a minimal reproduction, impact, and suggested mitigation
when available. Do not include real credentials or private execution histories.
