"""Control protocol adapter for workspace worktree management."""

import pydantic
from collections.abc import Awaitable, Callable
from typing import Optional

from . import models as m
from .manager import Manager


async def dispatch(
    manager: Manager,
    method: str,
    params: dict[str, pydantic.JsonValue],
    *,
    delete: Optional[Callable[[m.Delete], Awaitable[m.Record]]] = None,
) -> pydantic.JsonValue:
    if method == "worktrees.create":
        request = m.CreateRequest.model_validate(params)
        record = await manager.create(
            m.Create.model_validate(request.model_dump(exclude={"idempotency_key"})),
            key="user:" + request.idempotency_key,
        )
        return record.model_dump(mode="json")
    if method == "worktrees.list":
        return (await manager.listing(m.Query.model_validate(params))).model_dump(
            mode="json"
        )
    if method == "worktrees.get":
        return (await manager.get(m.Get.model_validate(params))).model_dump(mode="json")
    if method == "worktrees.update":
        return (await manager.update(m.Update.model_validate(params))).model_dump(
            mode="json"
        )
    if method == "worktrees.delete":
        return (
            await (delete or manager.delete)(m.Delete.model_validate(params))
        ).model_dump(mode="json")
    raise m.WorktreeError("unsupported worktree method")
