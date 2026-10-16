"""Coalesced snapshot pushes driven by committed changes, never polling."""

import asyncio
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from typing import Union

import pydantic

from vectorops.runner import diagnostics, models
from vectorops.runner.runner import events, inspection


class SnapshotPublisher:
    """Serialize per-execution reads and drain accepted updates before shutdown."""

    def __init__(
        self,
        summary: Callable[[str], Awaitable[inspection.ExecutionSummary]],
        workflow: Callable[[str, str], Awaitable[models.Workflow]],
        emit: Callable[[dict[str, pydantic.JsonValue]], None],
    ) -> None:
        self._summary = summary
        self._workflow = workflow
        self._emit = emit
        self._pending: dict[str, set[str]] = {}
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._snapshots: OrderedDict[tuple[str, str], pydantic.BaseModel] = (
            OrderedDict()
        )
        self._closed = False

    def changed(self, execution_id: str, workflow_ids: tuple[str, ...] = ()) -> None:
        if self._closed:
            return
        self._pending.setdefault(execution_id, set()).update(workflow_ids)
        if execution_id not in self._tasks:
            self._tasks[execution_id] = asyncio.create_task(self._publish(execution_id))

    def _send(
        self,
        key: tuple[str, str],
        snapshot: pydantic.BaseModel,
        event: Union[events.WorkflowUpdated, events.ExecutionUpdated],
    ) -> None:
        if self._snapshots.get(key) == snapshot:
            return
        self._emit(event.model_dump(mode="json"))
        self._snapshots[key] = snapshot
        self._snapshots.move_to_end(key)
        while len(self._snapshots) > 256:
            self._snapshots.popitem(last=False)

    async def _publish(self, execution_id: str) -> None:
        try:
            while execution_id in self._pending:
                identifiers = self._pending.pop(execution_id)
                for identifier in sorted(identifiers):
                    workflow = await self._workflow(execution_id, identifier)
                    self._send(
                        (execution_id, identifier),
                        workflow,
                        events.WorkflowUpdated(
                            execution_id=execution_id, workflow=workflow
                        ),
                    )
                try:
                    summary = await self._summary(execution_id)
                except (FileNotFoundError, KeyError):
                    continue
                self._send(
                    (execution_id, ""),
                    summary,
                    events.ExecutionUpdated(execution_id=execution_id, summary=summary),
                )
        except Exception as exc:
            diagnostics.log(
                "snapshot.publish_failed", level=diagnostics.Level.ERROR, error=exc
            )
        finally:
            self._tasks.pop(execution_id, None)

    async def close(self) -> None:
        self._closed = True
        await asyncio.gather(*self._tasks.values())
        self._snapshots.clear()
