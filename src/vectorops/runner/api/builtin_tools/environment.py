"""Registration-free access to an activity's target environment."""

from vectorops.runner.api.tool_contracts import ToolExecutionError
from vectorops.runner.environment.local import LocalEnvironment
from vectorops.runner.runner.activity_context import ActivityExecutionContext


def environment(context: ActivityExecutionContext) -> LocalEnvironment:
    if context.environment is None:
        raise ToolExecutionError("This tool requires a configured target environment")
    return context.environment
