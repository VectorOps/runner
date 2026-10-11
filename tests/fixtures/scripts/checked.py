from typing import Annotated

import pydantic

from vectorops.runner.api.composition import workflow


class Payload(pydantic.BaseModel):
    value: int


def positive(value: int) -> int:
    if value < 0:
        raise ValueError("negative input")
    return value


@workflow()
async def main(
    ctx: object,
    payload: Payload,
    value: Annotated[int, pydantic.AfterValidator(positive)],
) -> int:
    return payload.value + value
