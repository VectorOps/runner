"""Replay-aware tool factory for an explicit set of durable child workflows."""

from collections.abc import Sequence
import json
import typing

import connect
import pydantic
from jsonschema.exceptions import ValidationError

from vectorops.runner.api import composition as c, definitions
from vectorops.runner.api.tool_registry import ToolConfig, register_tool
from vectorops.runner.api import tool_contracts as tools
from vectorops.runner.references import WorkflowReference
from vectorops.runner.runner.activity_context import ActivityExecutionContext
from vectorops.runner.runner.context import WorkflowExecutionContext


class SubworkflowArguments(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")

    reference: str
    input: dict[str, pydantic.JsonValue] = pydantic.Field(default_factory=dict)
    description: typing.Optional[str] = pydantic.Field(
        default=None, description="Brief description of this workflow task for the UI."
    )


class SubworkflowTool(tools.WorkflowToolBinding):
    def __init__(
        self,
        descriptors: Sequence[definitions.WorkflowDescriptor],
        *,
        name: str,
        approval: typing.Optional[tools.ToolApproval],
    ) -> None:
        self._descriptors = {
            descriptor.reference: descriptor.model_copy(deep=True)
            for descriptor in descriptors
        }
        self._approval = approval
        schema = SubworkflowArguments.model_json_schema()
        schema["properties"]["reference"]["enum"] = list(self._descriptors)
        catalog = [descriptor.model_dump(mode="json") for descriptor in descriptors]
        self._definition = connect.ToolSpec.external(
            name=name,
            description=(
                "Run an available child workflow and return its result. "
                "Supply reference and input matching its input_schema. Available workflows:\n"
                + json.dumps(catalog, ensure_ascii=True, sort_keys=True)
            ),
            input_schema=schema,
        )

    @property
    def definition(self) -> connect.ToolSpec:
        return self._definition.model_copy(deep=True)

    @property
    def version(self) -> int:
        return 1

    @property
    def approval(self) -> typing.Optional[tools.ToolApproval]:
        return self._approval

    def _validated(
        self, arguments: dict[str, pydantic.JsonValue]
    ) -> SubworkflowArguments:
        try:
            parsed = SubworkflowArguments.model_validate(arguments)
            descriptor = self._descriptors.get(parsed.reference)
            if descriptor is None:
                raise tools.ToolExecutionError(
                    f"workflow is not available: {parsed.reference}"
                )
            descriptor.preflight_input(parsed.input)
            return parsed
        except (pydantic.ValidationError, ValidationError) as exc:
            raise tools.ToolExecutionError(str(exc)) from exc

    def validate(self, arguments: dict[str, pydantic.JsonValue]) -> None:
        self._validated(arguments)

    async def execute_workflow(
        self,
        context: WorkflowExecutionContext,
        arguments: dict[str, pydantic.JsonValue],
    ) -> tools.ToolResponse:
        parsed = self._validated(arguments)
        descriptor = self._descriptors[parsed.reference]
        handle = await context.spawn_workflow(
            descriptor.reference,
            input=parsed.input,
            kind=descriptor.kind,
            version=descriptor.version,
            description=parsed.description,
        )
        outcomes = await context.join_workflows([handle], failure_policy="collect")
        outcome = outcomes[0]
        if outcome.kind == "failed":
            return tools.ToolResponse(
                content=[connect.TextBlock(text=outcome.error.message)], is_error=True
            )
        return tools.ToolResponse(
            content=[
                connect.TextBlock(text=json.dumps(outcome.result, ensure_ascii=True))
            ]
        )


@c.activity_wrapper(kind="workflow.describe", version=1)
async def subworkflow_tool(
    call: c.ActivityCall,
    references: Sequence[str],
    *,
    name: str = "subworkflow",
    approval: typing.Optional[tools.ToolApproval] = None,
) -> SubworkflowTool:
    """Snapshot descriptors; call again with a new list to expose generated workflows."""
    normalized = [str(WorkflowReference.parse(reference)) for reference in references]
    if not normalized or len(set(normalized)) != len(normalized):
        raise ValueError("available workflows must be nonempty and unique")
    resolver = c.workflow_context().project.resolver

    async def describe(ctx: ActivityExecutionContext) -> pydantic.JsonValue:
        return [
            (await resolver.describe(reference)).model_dump(mode="json")
            for reference in normalized
        ]

    snapshot = await call.run(input={"references": list(normalized)}, invoke=describe)
    descriptors = pydantic.TypeAdapter(
        list[definitions.WorkflowDescriptor]
    ).validate_python(snapshot)
    return SubworkflowTool(descriptors, name=name, approval=approval)


async def subagent_tool(
    references: Sequence[str],
    *,
    name: str = "run_agent",
    approval: typing.Optional[tools.ToolApproval] = None,
) -> SubworkflowTool:
    """Create a tool for permitted agents without starting a child workflow."""
    return await subworkflow_tool(references, name=name, approval=approval)


class SubworkflowConfig(ToolConfig):
    references: list[str] = pydantic.Field(min_length=1)

    @pydantic.field_validator("references", mode="after")
    @classmethod
    def validate_references(cls, values: list[str]) -> list[str]:
        normalized = [str(WorkflowReference.parse(value)) for value in values]
        if len(set(normalized)) != len(normalized):
            raise ValueError("workflow references must be unique")
        return normalized


@register_tool("run_agent", config=SubworkflowConfig)
async def configured_subagent(config: SubworkflowConfig) -> SubworkflowTool:
    return await subagent_tool(config.references)


@register_tool("subworkflow", config=SubworkflowConfig)
async def configured_subworkflow(config: SubworkflowConfig) -> SubworkflowTool:
    return await subworkflow_tool(config.references)
