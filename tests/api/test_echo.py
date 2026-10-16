import pydantic
import pytest

from tests.fixtures import replay
from tests.fixtures.history import MemoryHistory
from vectorops.runner import models
from vectorops.runner.api.echo import echo
from vectorops.runner.runner.errors import ReplayMismatchError


@pytest.mark.parametrize(
    "value",
    [
        "Hello from the workflow",
        "",
        None,
        True,
        42,
        1.5,
        ["hello", 1, None],
        {"message": "hello", "details": [False, {"count": 2}]},
    ],
)
async def test_echo_records_and_replays(
    history: MemoryHistory, value: pydantic.JsonValue
) -> None:
    async def workflow() -> pydantic.JsonValue:
        result = await echo(value)
        assert result == value
        return result

    first = await replay.run(history, workflow)
    records = history.path(first.id)
    activities = [
        step for step in records if isinstance(step.operation, models.StepOperation)
    ]
    assert len(activities) == 1
    assert activities[0].operation == models.StepOperation(
        kind="echo", name="echo", version=1, input={"value": value}
    )
    assert activities[0].outcome == models.StepCompleted(result=value)
    assert first.outcome == models.StepCompleted(result=value)

    repeated = await replay.run(history, workflow, first.id)
    assert repeated == first
    assert history.path(repeated.id) == records


async def test_echo_rejects_changed_input_on_replay(history: MemoryHistory) -> None:
    value = "original"

    async def workflow() -> pydantic.JsonValue:
        return await echo(value=value)

    first = await replay.run(history, workflow)
    value = "changed"
    with pytest.raises(ReplayMismatchError):
        await replay.run(history, workflow, first.id)
