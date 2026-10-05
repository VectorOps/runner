"""Workflow discovery results, independent of control transport and execution state."""

import typing

import pydantic

from vectorops.runner.api.definitions import WorkflowDescriptor


class DiscoveryError(pydantic.BaseModel):
    source: str
    error: str


class WorkflowCatalog(pydantic.BaseModel):
    workflows: list[WorkflowDescriptor] = pydantic.Field(default_factory=list)
    errors: list[DiscoveryError] = pydantic.Field(default_factory=list)
    next_after: typing.Optional[str] = None
