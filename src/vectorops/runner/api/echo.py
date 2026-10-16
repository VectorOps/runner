import pydantic

from vectorops.runner.api.composition import activity
from vectorops.runner.runner.activity_context import ActivityExecutionContext


@activity(kind="echo")
async def echo(
    context: ActivityExecutionContext, value: pydantic.JsonValue
) -> pydantic.JsonValue:
    """Record a JSON-compatible value and return it unchanged."""
    return value
