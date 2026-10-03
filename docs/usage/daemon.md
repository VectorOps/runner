# Run and connect to a daemon

Run a workspace daemon with persistent runner-owned authentication. The host needs its loopback endpoint and bearer token, directly or through an SSH tunnel.

## Start and inspect

```bash
uv run vops-runner --project /path/to/workspace daemon serve --port 43821
uv run vops-runner daemon status --port 43821
uv run vops-runner daemon connect --port 43821
uv run vops-runner daemon stop --port 43821 --mode cancel
```

The runner creates and reuses a persistent control token in its [global configuration](../config/authentication.md). Local clients load it automatically. `--config-dir PATH` or `VOPS_RUNNER_CONFIG_DIR` selects another configuration directory. Optional `--token-file PATH` or `--token-stdin` overrides the token for that process; the options are mutually exclusive. The runner does not change or delete an externally supplied token file.

`serve` reads the token once before opening the listener, then prints one JSON readiness record containing `host`, `port`, `instance_id`, `workspace`, and `pid`, never the token. `--port` selects a fixed port; the default is OS-assigned. Only one daemon or local execution writer can own the workspace at a time. The workspace is required only by the runner, not its clients.

`status`, `connect`, and `stop` require `--port`. Their optional `--host` accepts `127.0.0.1` (default) or `::1`, including a local SSH-forwarded endpoint. They do not discover endpoints or read the workspace.

`status` authenticates and prints the endpoint, runner information, and active execution as JSON. `connect` prints runner information, current execution, and then live events as JSONL; it does not backfill history. It is an observational CLI attachment, not an interactive command shell or stdio relay.

`stop --mode cancel` requests cancellation and shutdown. `stop --mode drain` waits for the active execution to finish. The command acknowledges the request; it does not wait for daemon exit. Closing a host connection leaves the daemon and workflow running.

## Provision from a Python harness

For an isolated per-process override, generate a token from 32 random bytes and pass it over the child's stdin pipe. Normal startup needs no override. Do not put tokens in environment variables, arguments, or logs.

```python
import asyncio
import json
import secrets
import sys

from vectorops.runner.control.client import RunnerClient

async def main():
    token = secrets.token_urlsafe(32)
    process = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "vectorops.runner",
        "--project", "/path/to/workspace",
        "daemon", "serve", "--token-stdin",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
    )
    try:
        assert process.stdin is not None and process.stdout is not None
        process.stdin.write((token + "\n").encode("ascii"))
        await process.stdin.drain()
        process.stdin.close()
        async with asyncio.timeout(10):
            endpoint = json.loads(await process.stdout.readline())
        async with await RunnerClient.tcp(endpoint["port"], token) as host:
            print(await host.current())
            await host.shutdown()
        async with asyncio.timeout(10):
            await process.wait()
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()

asyncio.run(main())
```

`--token-stdin` reads one bounded line from a pipe or redirected file, not a terminal. Send a newline or close stdin to finish provisioning. Closing stdin afterward does not stop the daemon. `--token-file` reads a bounded ASCII file containing just the token, with an optional final LF or CRLF. Both reject empty, malformed, and oversized tokens without printing their contents. Token files can be removed after readiness; the daemon retains the token in memory until disposal.

Tokens contain 43-4096 URL-safe characters. A token grants full controller access, with no expiry or rotation while a daemon is running. The default token survives restart. To rotate it, change only `control_token` in global configuration while daemons are stopped, then update remote clients. External token-file protection and cleanup remain the harness's responsibility. No endpoint discovery file is written.

For an already running daemon, use `RunnerClient.tcp(port)` locally or `RunnerClient.tcp(port, token)` with an explicit token. In-process `embedded(workspace)` loads the same persistent configuration and uses the same authentication protocol.

## Connect through SSH

Start the remote daemon through an authenticated SSH connection and obtain its control token securely, or provision a per-process override. Capture its readiness record to learn the remote port, then forward a local loopback port:

```bash
ssh -N -T -o ExitOnForwardFailure=yes \
  -L 127.0.0.1:49152:127.0.0.1:43821 user@server
```

Replace `43821` with the remote readiness port. Keep normal SSH host-key verification enabled. The host uses `RunnerClient.tcp(49152, token)` with the remote daemon's token, not its own machine's default token.

Bearer authentication sends the token to the endpoint; it does not independently verify runner identity or encrypt local TCP. Trust the provisioned endpoint and use SSH for remote encryption and server authentication. Automated SSH provisioning is not implemented; forwarding needs no runner-specific relay.

Tunnel failure disconnects the client but leaves execution running. Reconnect, subscribe, and query history to restore the display.

## Related

- [Provider authentication and global secrets](../config/authentication.md)
- [Control protocol and Python client](../reference/control-protocol.md)
- [Existing local CLI](cli.md)