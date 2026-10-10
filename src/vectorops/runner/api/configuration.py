"""Activity adapters for the shared workspace configuration snapshot."""

from copy import deepcopy
import hashlib
import json
import typing

import pydantic

from vectorops.runner import models
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.context import WorkflowExecutionContext
from vectorops.runner.runner import project


async def get_config(
    ctx: WorkflowExecutionContext,
    name: str,
    default: pydantic.JsonValue = None,
    *,
    model: typing.Optional[type[pydantic.BaseModel]] = None,
    contract_digest: typing.Optional[str] = None,
) -> pydantic.JsonValue:
    """Record a dotted-path lookup, preserving old values during activity replay.

    Paths are relative to the workspace's components table. Only the selected
    value is recorded. An optional owner model validates and normalizes inside
    the activity before persistence. Missing values use the supplied default;
    pass {} to apply model defaults. Never use this API for secrets.
    """
    project.validate_name(name)
    if model is None and contract_digest is not None:
        raise ValueError("contract_digest requires a model")
    fallback = project.json_value(default)
    configuration = ctx.project.configuration
    operation = models.StepOperation(
        kind="configuration.get",
        name="get",
        version=1,
        input={"name": name, "default": fallback},
    )
    if model is not None:
        schema = json.dumps(
            model.model_json_schema(), sort_keys=True, separators=(",", ":")
        )
        inputs: dict[str, pydantic.JsonValue] = {
            "name": name,
            "schema_digest": contract_digest
            or hashlib.sha256(schema.encode()).hexdigest(),
        }
        if fallback != {}:
            encoded = json.dumps(
                fallback, sort_keys=True, separators=(",", ":"), allow_nan=False
            )
            inputs["default_digest"] = hashlib.sha256(encoded.encode()).hexdigest()
        operation = models.StepOperation(
            kind="configuration.validated",
            name="get_validated",
            version=1,
            input=inputs,
        )

    async def invoke(context: ActivityExecutionContext) -> pydantic.JsonValue:
        if model is None:
            return configuration.get(name, fallback)
        try:
            value = configuration.get(name, fallback)
            return project.json_value(
                model.model_validate(value).model_dump(mode="json")
            )
        except Exception:
            raise ValueError("component configuration is invalid") from None

    result = await ctx.run_activity(operation, invoke)
    return deepcopy(result)
