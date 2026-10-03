# Run and connect to a daemon

Run a disposable workspace daemon provisioned by a harness. The host needs only its loopback endpoint and bearer token, directly or through an SSH tunnel.

## Start and inspect

```bash
uv run vops-runner --project /path/to/workspace daemon serve --port 43821 --token-file /protected/runner.token
uv run vops-runner daemon status --port 43821 --token-file /protected/runner.token
uv run vops-runner daemon connect --port 43821 --token-file /protected/runner.token
uv run vops-runner daemon stop --port 43821 --token-file /protected/runner.token --mode cancel
```

The harness creates the token file first with user-only access (0600 on Unix, a user-only ACL on Windows). Each command requires exactly one of `--token-file PATH` or `--token-stdin`; there is no token argument, environment-variable fallback, or generated persistent credential. The runner does not change or delete the harness's file.

`serve` reads the token once before opening the listener, then prints one JSON readiness record containing `host`, `port`, `instance_id`, `workspace`, and `pid`, never the token. `--port` selects a fixed port; the default is OS-assigned. Only one daemon or local execution writer can own the workspace at a time. The workspace is required only by the runner, not its clients.

`status`, `connect`, and `stop` require `--port`. Their optional `--host` accepts `127.0.0.1` (default) or `::1`, including a local SSH-forwarded endpoint. They do not discover endpoints or read the workspace.

`status` authenticates and prints the endpoint, runner information, and active execution as JSON. `connect` prints runner information, current execution, and then live events as JSONL; it does not backfill history. It is an observational CLI attachment, not an interactive command shell or stdio relay.

`stop --mode cancel` requests cancellation and shutdown. `stop --mode drain` waits for the active execution to finish. The command acknowledges the request; it does not wait for daemon exit. Closing a host connection leaves the daemon and workflow running.

## Provision from a Python harness

Generate a fresh token from 32 random bytes for each daemon and pass it over the child's stdin pipe. Do not put it in environment variables, arguments, or logs.

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

Tokens contain 43-4096 URL-safe characters. A token grants full controller access, with no expiry or rotation while a daemon is running. Restart with a newly generated token to rotate it; never reuse a disposed daemon's token for a replacement. The harness is responsible for protecting and cleaning up any token file. No credential or endpoint files are written by the daemon.

For an already running daemon, use `RunnerClient.tcp(port, token)` directly. In-process `embedded(workspace)` generates a private temporary token internally and uses the same authentication protocol.

## Connect through SSH

Provision the remote daemon through the harness's authenticated SSH connection using stdin or a protected token file. Capture its readiness record to learn the remote port, then forward a local loopback port:

```bash
ssh -N -T -o ExitOnForwardFailure=yes \
  -L 127.0.0.1:49152:127.0.0.1:43821 user@server
```

Replace `43821` with the remote readiness port. Keep normal SSH host-key verification enabled. The host uses `RunnerClient.tcp(49152, token)` with the same harness-generated token.

Bearer authentication sends the token to the endpoint; it does not independently verify runner identity or encrypt local TCP. Trust the provisioned endpoint and use SSH for remote encryption and server authentication. Automated SSH provisioning is not implemented; forwarding needs no runner-specific relay.

Tunnel failure disconnects the client but leaves execution running. Reconnect, subscribe, and query history to restore the display.

## Related

- [Control protocol and Python client](../reference/control-protocol.md)
- [Existing local CLI](cli.md)