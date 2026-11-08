# Read configuration in a workflow

Use `get_config` to read non-secret settings in workflow code. Each read records
its result so replay takes the same branches and uses the same values.

## Read a value

Add project settings to `.voc/config.toml`:

```toml
[components.report]
language = "French"
max_sections = 8
```

Then define a workflow:

```python
from vectorops.runner.api.configuration import get_config


async def main(ctx):
    language = await get_config(ctx, "report.language", "English")
    count = await get_config(ctx, "report.max_sections", 5)
    return {"language": language, "max_sections": count}
```

The result is `{"language": "French", "max_sections": 8}`. No registration is
required. `ctx` is the injected workflow context; reads cannot be nested inside an
activity. Names follow the [configuration path rules](../config/components.md#override-component-settings).

A missing key or non-mapping intermediate value returns the default, which is
`None` when omitted. False, zero, empty lists, and empty mappings are actual values.
The getter does not merge a selected mapping with the fallback or infer its type.
Values and defaults must be finite JSON-compatible data.

**Never read secrets through this API.** Values and defaults are saved in history
and can appear in exports and events. Reading a subtree records the entire subtree.

## Validate settings

Pass a Pydantic model to validate and normalize a value before recording it:

```python
from pydantic import BaseModel, Field
from vectorops.runner.api.configuration import get_config


class ReportSettings(BaseModel):
    max_sections: int = Field(default=5, ge=1)


async def main(ctx):
    return await get_config(ctx, "report", {}, model=ReportSettings)
```

The return value is normalized JSON data, not a model instance. A missing setting
uses the supplied fallback, then validates it. Pass `{}` to use model defaults;
an omitted fallback is `None` and must be accepted by the model.

Validated reads record a schema digest. `contract_digest=` can supply an explicit
contract identifier and requires `model=`. Validation errors omit input payloads,
but accepted fields are not scanned for secrets. Design schemas accordingly.

## Reads outside workflow code

Application setup can access the running project's configuration directly:

```python
async def configure(runner):
    project = await runner.project()
    language = project.configuration.get("report.language", "English")
    await project.configuration.set("report.language", "German", permanent=True)
    return language
```

`get` is synchronous and reads memory. `get_validated(name, Model)` returns a model
instance, using `{}` for absent values. Returned containers are detached from shared
state. These APIs do not record history; do not substitute them for `get_config`
inside replayable orchestration.

## Preserve replay compatibility

Completed reads return their saved values. An unfinished read with no committed
result reads again. Changing a path, fallback, schema, or operation order can cause
a replay mismatch. Keep existing workflow branches compatible or start a new
execution. Explicit code-level LLM overrides also participate in request identity.

Ordinary reads use `configuration.get`, name `get`, version 1. Validated reads use
`configuration.validated`. Recorded names retain their original spelling, so do
not rewrite an existing shorthand path just to adopt the canonical prefix.

## Related

- [Configuration files and overrides](../config/components.md)
- [Workflow contracts](workflows.md#workflow-contracts)
- [Activity contracts](activities.md#payload-contracts)
