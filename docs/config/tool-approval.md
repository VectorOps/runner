# Tool approval

Control which model-selected tools can run automatically and which require a human
response. Approval does not sandbox the tool or guarantee that its effects can be undone.

## Configure automatic approval

In user or project `config.toml`:

```toml
[components.tools.approval]
approve_all = false
whitelist = ["read_file", "list_files"]
```

`approve_all` defaults to `false`; `whitelist` defaults to `[]`. Names match exactly,
with no wildcards or argument-pattern grants. Entries must be unique, nonempty,
and unpadded. Unknown settings fail closed.

Review scope before granting a name. For example, `read_file` accepts paths outside
the target project. `approve_all = true` permits every otherwise valid tool call
without confirmation, including tools added later.

Argument validation and mandatory registered checks still apply. Without a grant,
the default approval channel is `tool.approval`. A connected host or CLI client must
answer the request; a foreground workflow does not provide an approval interface.

## Answer a request

Inspect pending input before replying:

```bash
uv run vops-runner client --port 8765 input list EXECUTION_ID
uv run vops-runner client --port 8765 input show EXECUTION_ID REQUEST_ID
uv run vops-runner client --port 8765 input send EXECUTION_ID REQUEST_ID --value '{"approved":true}'
```

| Response | Meaning |
| --- | --- |
| `{"approved":true}` | Approve this call |
| `{"approved":false,"reason":"Not needed"}` | Reject this call |
| `{"approved":true,"scope":"workflow"}` | Approve this call and future calls to the same tool in this workflow instance |

Workflow grants cover every argument for the exact tool name. They do not apply
to children, siblings, or other root executions. They survive resume and branch
continuations, have no revocation API, and do not answer already-pending requests.
Mandatory checks still run. See the [protocol reference](../reference/control-protocol.md#workflow-scoped-tool-approval)
for response validation and durable receipt rules.

## Recovery and tool errors

Completed outcomes, including rejections, replay without executing the tool again.
Pending requests and accepted answers survive recovery. Enabling automatic approval
does not bypass an outstanding confirmation or rejection. An interrupted approved
tool can run again; side effects need their own idempotency or reconciliation.

By default, validation and implementation exceptions become tool responses with
`is_error=True` and the exception text. The model can recover or choose another
action. Tool implementations are not automatically retried.

To propagate tool exceptions instead:

```python
from vectorops.runner.api import llm


async def main(ctx):
    conversation = llm.LLM("assistant", raise_tool_errors=True)
    conversation.add_user_message("Complete the task or report a failure.")
    return (await conversation.run()).model_dump(mode="json")
```

Use `set_raise_tool_errors(...)` between calls to change this mode.
`invoke_tool(..., raise_errors=True)` offers the equivalent standalone behavior.
Explicit `ToolResponse(is_error=True)` remains a response in either mode. Parallel
calls settle before an exception propagates; no partial round is appended.

Cancellation, authentication failures, replay mismatches, storage failures, and
approval infrastructure failures remain runner errors. Exception text sent to the
model is also stored in history. Tool authors must not put secrets in errors.
Changing strict mode for an existing recorded invocation can break replay.

## Extend approval policy

Trusted Python integrations can use `RunnerProject.tool_policy` to register
`allow(name)`, `remove(name)`, `register_check(name, callback)`, and
`register_matcher(name, callback)` policies. Async checks return a denial reason
or `None`; async matchers return a boolean. Each receives a detached `ToolCall`
with arguments and execution identities. Checks precede grants; exceptions deny access.

Configure extensions before calls begin. Runtime registry grants are not persisted
or shared between separately opened projects. Workspace configuration applies to
all executions in that workspace. Neither policy restricts arbitrary trusted Python.

## Related

- [Security boundaries](../../SECURITY.md)
- [Tool collections](tool-collections.md)
- [Saved-execution compatibility](../reference/compatibility.md)
