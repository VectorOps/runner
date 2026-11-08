# Tool registration and collections

Named collections select tools independently of workflow code. Use them to give
an agent the capabilities it needs without changing its implementation. Selecting
a tool does not approve its execution; see [tool approval](tool-approval.md).

## Configure a collection

To remove process execution from the built-in helper, add this to `config.toml`:

```toml
[components.tools.collections.helper]
tools = ["read_file", "list_files"]
```

The helper normally includes `read_file`, `list_files`, `exec`, and `poll_process`.
The agent also includes stdin/process controls, patching, planning, delegation,
MCP, and [web fetching](../usage/web-fetch.md). Worktree management tools are opt-in.
Start a new execution to pick up collection changes.

To create a named collection with an MCP entry:

```toml
[components.tools.collections.research]
tools = ["read_file", "list_files", "repository"]

[components.tools.collections.research.config.repository]
factory = "mcp"
selections = [{ server = "repository", tools = ["search", "read_file"] }]

[components.workflows.helper]
tool_collection = "research"
```

Configure `mcp.servers.repository` separately using the [MCP guide](mcp.md).
`repository` is a collection-entry name; its `factory` selects the registered
implementation. Without `factory`, the entry uses the registration of the same name.

## Defaults and overrides

- A missing collection uses its caller-supplied default, or fails if none exists.
- Omitting `tools` keeps the default list. An explicit list replaces it, including `[]`.
- `config.<entry>` overlays default options by top-level field. Nested objects and
  lists replace, rather than recursively merge.
- Changing `factory` discards inherited options for that entry.
- Duplicate entries, unknown factories, invalid options, unselected-entry options,
  and duplicate exposed tool names fail.
- Entry names are Python identifiers. Collection names use letters, digits,
  underscores, and hyphens, beginning with a letter or underscore.

Keep the agent's tools but change patch format:

```toml
[components.tools.collections.agent.config.apply_patch]
format = "patch"
```

Configure delegation:

```toml
[components.tools.collections.agent.config.run_agent]
references = ["builtin:helper", "research:main"]
```

`components.workflows.agent.agents` supplies default delegation references;
explicit collection options take precedence. Setting `agents=[]` removes delegation
from the default collection, not an explicitly configured replacement. Parent and
helper collections are independent.

## Registered factories

| Factory | Options |
| --- | --- |
| `read_file`, `list_files`, `exec`, `poll_process`, `write_stdin`, `kill_process`, `list_processes` | None |
| `update_plan`, `web_fetch` | None |
| `worktree_create`, `worktree_list`, `worktree_delete` | None; see [worktrees](../usage/worktrees.md) |
| `apply_patch` | `format`: `v4a` (default) or `patch` (SEARCH/REPLACE) |
| `run_agent`, `subworkflow` | Required nonempty, unique `references` list |
| `mcp` | Optional `selections`; omission selects enabled servers, `[]` selects none. Optional `component`, default `mcp` |

Do not put secrets in options: configuration is recorded in history. Importing
`vectorops.runner.api.tools` registers its built-in factories but does not create
bindings, grant permissions, launch processes, or contact servers.

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
    return (await conversation.run(tools=bindings)).model_dump(mode="json")
```

Load once per conversation and reuse the bindings. Each load constructs fresh state,
so plans are not shared globally. Factories run again during replay to rebuild
bindings; external discovery must use recorded activities. Do not wrap collection
loading in an activity that would skip reconstruction on replay.

`load_collection` always selects static bindings. To honor MCP dynamic mode, use
`selected = await tools.load_tool_set("research")`, then
`await conversation.run(tool_set=selected)`. Static tools remain available each round;
dynamic tools activate from completed search results. Each `run` starts from the
original activation state.

## Register a custom tool

Install an importable module such as `my_project.tools` in the runner environment:

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

Enable and select it:

```toml
[components.tools]
modules = ["my_project.tools"]

[components.tools.collections.custom]
tools = ["project_echo"]

[components.tools.collections.custom.config.project_echo]
prefix = "Result"
```

Factories can be sync or async and return a binding or sequence. A configured
factory receives one validated model; a factory without a config model takes no
arguments and rejects options. Duplicate registration fails. Tool-call arguments
are separate from factory settings.

An optional `tool_set=` factory on `register_tool` supplies a dynamic `ToolSet` to
`load_tool_set`. Without it, the loader wraps static bindings. Dynamic factories
own authorization metadata and cannot combine with the decorator's `auth_context`.

Imports execute trusted Python. List each custom factory's defining module in
`modules`; another workspace's imports do not make it selectable here. Restart to
reload module edits. Keep imports free of resource initialization and external I/O.

## Related

- [Agent and helper usage](../usage/cli.md#coding-agent-and-helper)
- [MCP setup and dynamic discovery](mcp.md)
- [Tool approval](tool-approval.md)
- [Compatibility](../reference/compatibility.md)
