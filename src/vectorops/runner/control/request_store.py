"""Durable mutation receipts prevent duplicate launches across reconnections."""

import typing

from vectorops.runner.control import protocol as p
from vectorops.runner.runner.workspace_database import WorkspaceDatabase


class RequestStore:
    def __init__(self, database: WorkspaceDatabase) -> None:
        self.database = database

    async def lookup(self, key: str, fingerprint: str) -> typing.Optional[p.Accepted]:
        row = await self.database.receipt(key)
        if row is None:
            return None
        if row[0] != fingerprint:
            raise p.RemoteError(
                "idempotency_conflict", "key already used for different arguments"
            )
        return p.Accepted.model_validate_json(row[1])

    async def save(self, key: str, fingerprint: str, result: p.Accepted) -> None:
        await self.database.save_receipt(key, fingerprint, result.model_dump_json())
