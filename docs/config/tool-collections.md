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

### Fetch pages and continue reading

`web_fetch` follows up to 10 HTTP redirects by default and converts HTML to Markdown
with headings, links, code blocks, lists, and tables. It prefers `main` or `article`
content and removes navigation, sidebars, footers, scripts, styles, and elements
marked `hidden` or `aria-hidden="true"`. This is HTML parsing, not browser rendering; JavaScript-only content
is not loaded. Plain text, including Markdown and JSON responses, is not converted.

```json
{"url": "https://example.com/docs", "max_bytes": 32000}
```

`max_bytes` limits returned content **after conversion**, before base64 encoding for
binary content. It defaults to 65,536 bytes and accepts 1–1,048,576 bytes. A separate
16 MiB decompressed download limit rejects oversized responses rather than silently
converting partial HTML. The timeout defaults to 30 seconds (maximum 120) and covers
the entire redirect chain, download, and conversion wait.

To continue, pass the previous result's `next_offset` as `offset`, keeping the URL,
format, headers, and selection options unchanged:

```json
{"url": "https://example.com/docs", "max_bytes": 32000, "offset": 32000}
```

The example assumes the first result's `next_offset` is 32000. Always use the actual
value: UTF-8 output can end slightly before the requested limit to avoid splitting a
character. Arbitrary offsets inside a UTF-8 character fail; a limit too small to fit
the next character also fails. Offsets beyond the end return empty content with `eof: true`.

Each call performs a fresh request, not a cached read or an HTTP Range request.
Compare `content_sha256` across ranges; if it changes, discard the combined result
and restart. Repeating a non-GET request also repeats its method and body, so do not
blindly paginate requests with side effects.

| Argument | Default | Meaning |
| --- | --- | --- |
| `offset` | `0` | Byte offset in the complete converted UTF-8 text or original binary content |
| `format` | `"markdown"` | HTML output: `"markdown"`, `"text"`, or `"raw"`; other text is unchanged |
| `main_content_only` | `true` | Select main/article content and remove navigation; `false` disables those two steps; ignored for raw HTML |
| `follow_redirects` | `true` | Follow up to 10 redirects; `false` returns the redirect response and `location` |

Results retain `status`, original `content_type`, and `bytes_read` (downloaded,
decompressed bytes). `content_format` identifies the output representation.
`output_bytes` counts bytes in this range, `total_bytes` counts the complete
representation, and `offset`/`next_offset` delimit the returned range. `truncated`
means more content follows; `eof` is its inverse. `content_sha256` hashes the complete
representation, not just this range. `url` is the final URL; `requested_url` and
`redirects` describe how it was reached. Complete supported images return image
blocks; partial images and other binary formats return base64, without text extraction.

Approval covers the redirect chain. HTTPS-to-HTTP redirects and cross-origin
redirects that would forward a request body fail. On cross-origin redirects, caller
headers are dropped except `Accept`, `Accept-Language`, and `User-Agent`; cookies
from responses are not retained. Standard HTTP redirect method rules apply: 303
changes non-HEAD requests to GET, 301/302 change POST to GET, and 307/308 preserve
method and body. Redirects can reach private addresses on the runner host's network;
this tool is not a network sandbox. HTTP statuses of 400 or higher set `is_error`.

This is tool version 2. Earlier calls limited raw response bytes, returned plain
text for HTML, and did not follow redirects. Completed calls still replay their
recorded results without network access; new calls use the updated behavior.

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