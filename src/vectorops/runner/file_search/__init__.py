"""Standalone workspace file autocomplete without UI or transport dependencies."""

from . import models
from .protocol import FileSearch
from .service import FileSearchService

RefreshId = models.RefreshId
SearchConfig = models.SearchConfig
SearchResult = models.SearchResult
SearchUpdate = models.SearchUpdate

__all__ = [
    "FileSearch",
    "FileSearchService",
    "RefreshId",
    "SearchConfig",
    "SearchResult",
    "SearchUpdate",
]
