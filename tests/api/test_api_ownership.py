from pathlib import Path
from typing import Annotated
from unittest.mock import patch

import pydantic
import pytest

from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from vectorops.runner.api import composition, definitions
from vectorops.runner.api.configuration import get_config
from vectorops.runner.runner.errors import ActivityError, ReplayMismatchError
from vectorops.runner.runner.resolver import WorkflowResolver
from vectorops.runner.runner.workflow import WorkflowRunner


def positive(value: int) -> int:
    if value < 0:
        raise ValueError("negative input")
    return value


async def typed_workflow(
    context: object, value: Annotated[int, pydantic.AfterValidator(positive)]
) -> int:
    return value


def test_schema_preflight_is_not_authoritative_validation() -> None:
    descriptor = definitions.WorkflowDescriptor(
        reference="example:typed_workflow",
        name="typed_workflow",
        input_schema=definitions.input_model(typed_workflow).model_json_schema(),
    )
    descriptor.preflight_input({"value": -1})
    descriptor.validate_input({"value": -1})
    with pytest.raises(pydantic.ValidationError, match="negative input"):
        definitions.validated_input(typed_workflow, {"value": -1})
    assert definitions.validated_input(typed_workflow, {"value": "2"}) == {"value": 2}


@pytest.mark.parametrize("decorated", [False, True])
async def test_executor_validates_before_body_and_retains_typed_values(
    tmp_path: Path,
    history: MemoryHistory,
    decorated: bool,
) -> None:
    directory = tmp_path / "workflows"
    directory.mkdir()
    source = """
from typing import Annotated
import pydantic
from vectorops.runner.api.composition import workflow

class Payload(pydantic.BaseModel):
    value: int

def positive(value: int) -> int:
    if value < 0:
        raise ValueError("negative input")
    return value

DECORATOR
async def main(ctx, payload: Payload, value: Annotated[int, pydantic.AfterValidator(positive)]):
    return payload.value + value
"""
    (directory / "checked.py").write_text(
        source.replace("DECORATOR", "@workflow()" if decorated else "")
    )
    runner = WorkflowRunner(tmp_path, history)
    with pytest.raises(ActivityError, match="negative input"):
        await runner.run("checked:main", input={"payload": {"value": 3}, "value": -1})
    assert (
        await runner.run(
            "checked:main", input={"payload": {"value": "3"}, "value": "2"}
        )
        == 5
    )


async def test_callable_reference_lookup_has_no_filesystem_work(tmp_path: Path) -> None:
    (tmp_path / "flow.py").write_text("""
from vectorops.runner.api.composition import workflow
@workflow()
async def child(ctx, value: int = 1):
    return value
""")
    resolver = WorkflowResolver(tmp_path)
    entry = await resolver.resolve("flow:child")
    assert isinstance(entry, composition.WorkflowDefinition)
    with patch.object(
        Path, "resolve", side_effect=AssertionError("filesystem in workflow")
    ):
        assert resolver.reference_for(entry.func) == "flow:child"
        assert (
            resolver.reference_for(typed_workflow)
            == f"python:{typed_workflow.__module__}:typed_workflow"
        )


async def test_normalization_version_detects_python_validator_changes(
    history: MemoryHistory,
) -> None:
    class Settings(pydantic.BaseModel):
        value: int = 1

    version = "1"

    async def run() -> pydantic.JsonValue:
        return await get_config(
            composition.workflow_context(),
            "example",
            {},
            model=Settings,
            normalization_version=version,
        )

    first = await replay.run(history, run)
    repeated = await replay.run(history, run, first.id)
    assert repeated.outcome == first.outcome
    version = "2"
    with pytest.raises(ReplayMismatchError):
        await replay.run(history, run, first.id)
