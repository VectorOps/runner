"""Public configuration and immutable workspace search snapshots."""

from typing import NewType, Optional

import pydantic

RefreshId = NewType("RefreshId", int)


class SearchConfig(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    max_candidates: int = pydantic.Field(default=100_000, gt=0)
    max_memory_bytes: int = pydantic.Field(default=128 * 1024 * 1024, gt=0)
    max_scan_entries: int = pydantic.Field(default=1_000_000, gt=0)
    scan_timeout: float = pydantic.Field(default=30.0, gt=0, allow_inf_nan=False)
    max_results: int = pydantic.Field(default=100, gt=0)
    max_query_length: int = pydantic.Field(default=256, gt=0)
    batch_size: int = pydantic.Field(default=256, gt=0)
    publication_interval: float = pydantic.Field(
        default=0.05, gt=0, allow_inf_nan=False
    )
    max_ignore_bytes: int = pydantic.Field(default=1024 * 1024, gt=0)
    max_cache_candidates: int = pydantic.Field(default=100_000, ge=0)
    rg_executable: Optional[str] = "rg"


class SearchResult(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    path: str
    highlights: tuple[tuple[int, int], ...] = ()


class SearchUpdate(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", frozen=True)

    index_revision: int
    results: tuple[SearchResult, ...]
    refreshing: bool
    incomplete: bool
    truncated: bool
    refresh_id: Optional[RefreshId] = None
    refresh_error: Optional[str] = None
