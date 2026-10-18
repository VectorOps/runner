# Tool registration and named collections

Named collections select and configure tools independently of workflow code.
Use `load_tool_set` with `LLM.run(tool_set=...)` for static and dynamic tools, or
`load_collection` with `LLM.run(tools=...)` for static bindings only.

The default agent already includes MCP. Configure servers under root
`[mcp.servers.<name>]`; no collection edits are needed. MCP defaults to dynamic
discovery, and an empty server configuration adds no tools. See [MCP configuration](mcp.md).

New agent executions also include `web_fetch` for ordinary HTTP retrieval and prefer
it over shelling out to `curl`. Requests originate on the runner host, not the target.
Saved agent executions from before version 4 keep their previous tool defaults on
resume; start a new execution to pick up `web_fetch`. The helper's defaults are unchanged.

## Configure a collection

In the runner workspace's `config.toml`:

```toml
[components.tools.collections.research]
tools = ["read_file", "list_files", "repository"]

[components.tools.collections.research.config.repository]
factory = "mcp"
selections = [{ server = "repository", tools = ["search", "read_file"] }]

[components.workflows.helper]
tool_collection = "research"
```

Configure the `repository` server separately under
`mcp.servers.repository`; see [MCP configuration](mcp.md).
Collection keys are independent of workflow names. The agent defaults to collection
`agent`; the helper defaults to `helper`. Their code supplies default collections,
so existing installations need no new configuration.

`repository` above is a collection-entry name, not an exposed tool name. Its
`factory` chooses the registered implementation. Without `factory`, an entry uses
the registration with the same name. MCP retains its existing namespaced tool names.

## Defaults and overrides

- A missing collection uses its caller-supplied default. Without a default it fails.
- Omitting `tools` retains the default list. An explicit list replaces it, including `[]`.
- `config.<entry>` overlays the selected default entry's options by top-level field.
  Nested objects and lists replace, rather than merge recursively.
- Changing an entry's `factory` discards that entry's inherited options.
- Options for explicitly unselected entries, duplicate entries, unknown factories,
  invalid factory options, and duplicate exposed tool names fail.
- Entry names are Python identifiers. Collection keys use letters, digits,
  underscores, and hyphens, beginning with a letter or underscore.

For example, retain the agent's default tools but change patch format:

```toml
[components.tools.collections.agent.config.apply_patch]
format = "patch"
```

Replace the helper's tools to remove process execution:

```toml
[components.tools.collections.helper]
tools = ["read_file", "list_files"]
```

Configure delegation through the collection:

```toml
[components.tools.collections.agent.config.run_agent]
references = ["builtin:helper", "research:main"]
```

The existing `components.workflows.agent.agents` remains the source of default
`run_agent` references; explicit collection options take precedence. Setting
`agents = []` removes delegation from the default collection, not from an explicitly
configured replacement. Parent and helper collections are independent.

## Registered factories

Importing `vectorops.runner.api.tools` imports internal implementations and runs
their registration decorators. It does not create bindings, launch processes, or
contact MCP servers. `list_files_tool` is exported through this shared facade;
the old workflow-module import forwards to the same factory.

| Factory names | Configuration |
| --- | --- |
| `read_file`, `list_files`, `exec`, `poll_process`, `write_stdin`, `kill_process`, `list_processes`, `update_plan`, `web_fetch` | No options |
| `apply_patch` | `format`: `v4a` (default) or `patch` |
| `run_agent`, `subworkflow` | Required, nonempty, unique `references` list |
| `mcp` | Optional `selections` list; omitted selects all enabled configured servers, `[]` selects none. Optional `component`, default `mcp` |

Selection never grants approval. All bindings retain normal approval handling under
`components.tools.approval`. Collection configuration cannot install auto-approval
rules. Do not put secrets in factory options: configuration is recorded in history.
Use the owning subsystem's credential references instead.

## Load tools in a workflow

```python
from vectorops.runner.api import llm, tools

async def main(ctx, task: str):
    bindings = await tools.load_collection(
        "research",
        defaults={
            "research": tools.ToolCollection(tools=["read_file", "list_files"]),
        },
    )
    conversation = llm.LLM("research")
    conversation.add_user_message(task)
    response = await conversation.run(tools=bindings)
    return response.model_dump(mode="json")
```

Load once per conversation and reuse the bindings across turns. Each load constructs
fresh bindings; plans and other mutable tool state are never shared globally.
Selected configuration and module names use recorded configuration reads. Factories
run again during replay to reconstruct bindings; MCP and subworkflow factories use
their existing recorded discovery operations. Custom factories must do the same for
external I/O. Do not wrap collection loading in an activity that would skip binding
construction during replay. Changed workflow code/defaults must remain replay-compatible.

`load_collection` retains its static behavior, even when MCP mode is dynamic.
Use the dynamic-aware loader to honor `[mcp].mode` and `[mcp].max_active`:

```python
selected = await tools.load_tool_set("research")
response = await conversation.run(tool_set=selected)
```

Ordinary tools remain available every round; dynamic factories advance from
completed tool results. Each `LLM.run` begins with the original activation state.
Direct `mcp.dynamic(...)` remains available for explicit workflow-controlled catalogs.

## Register custom tools

Install an importable Python module, such as `my_project.tools`, in the runner's
environment. Use the decorator on a factory:

```python
import connect
import pydantic
from vectorops.runner.api import tools
from vectorops.runner.runner.activity_context import ActivityExecutionContext

class EchoConfig(tools.ToolConfig):
    prefix: str = "Echo"

class EchoArguments(pydantic.BaseModel):
    text: str

@tools.register_tool("project_echo", config=EchoConfig)
def create_echo(config: EchoConfig) -> tools.ToolBinding:
    async def invoke(
        ctx: ActivityExecutionContext, arguments: EchoArguments
    ) -> tools.ToolResponse:
        return tools.ToolResponse(
            content=[connect.TextBlock(text=f"{config.prefix}: {arguments.text}")]
        )

    return tools.Tool("project_echo", "Echo text", EchoArguments, invoke)
```

Enable the module and select the tool:

```toml
[components.tools]
modules = ["my_project.tools"]

[components.tools.collections.custom]
tools = ["project_echo"]

[components.tools.collections.custom.config.project_echo]
prefix = "Result"
```

Factories can be sync or async and return one binding or a sequence. With a `config`
model they receive one validated model argument; without one they are called with no
arguments and reject configuration options. Tool-call arguments remain separate from
factory configuration. The decorator preserves the original callable and rejects
duplicate registrations rather than replacing existing definitions.

An optional `tool_set=` factory on `register_tool` supplies a `ToolSet` to
`load_tool_set`; it accepts the same configuration argument as the static factory
and may be sync or async. Without one, the loader wraps ordinary bindings as a
static set. `load_collection` always uses the original static factory. Dynamic
factories own their binding authorization metadata and cannot be combined with
the decorator's `auth_context` option.

Imports execute trusted Python code and are not sandboxed. List the defining module
of each custom factory in `modules`; loading it in another workspace does not make
it selectable here. Python caches imported modules; restart the runner to pick up
module edits. Modules should register definitions only, without import-time resource
initialization. Only selected factories are invoked.

## Related

- [Built-in workflows and CLI](../usage/cli.md#coding-agent-and-helper)
- [MCP configuration](mcp.md)