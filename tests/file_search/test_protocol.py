from pathlib import Path

from vectorops.runner import file_search as fs
from vectorops.runner.file_search.protocol import FileSearch


async def test_public_protocol_and_service_conformance(tmp_path: Path) -> None:
    service: FileSearch = fs.FileSearchService(
        tmp_path, fs.SearchConfig(rg_executable=None)
    )
    assert fs.FileSearch is FileSearch
    assert isinstance(service, FileSearch)
    (tmp_path / "example.py").touch()
    try:
        refresh_id = await service.refresh()
        updates = service.search("example", limit=1)
        try:
            async for update in updates:
                if not update.refreshing:
                    assert update.refresh_id == refresh_id
                    assert [result.path for result in update.results] == ["example.py"]
                    break
        finally:
            await updates.aclose()
    finally:
        await service.close()
