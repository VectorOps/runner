import errno
import io
import os
from pathlib import Path

import pytest

from vectorops.runner.environment import local, models as m, text
from vectorops.runner.environment.configuration import LocalEnvironmentConfig
from vectorops.runner.environment.errors import EnvironmentError, ErrorCode
from vectorops.runner.patch import PatchService


def test_file_reader_reads_multiple_chunks() -> None:
    result = text.read_lines(
        io.BytesIO(b"first\nsecond\n"), m.LineReadRequest(), chunk_size=2
    )
    assert result.text == "first\nsecond\n"
    assert result.complete_lines == 2
    assert result.eof and not result.truncated


def test_file_reader_rejects_invalid_chunk_size() -> None:
    with pytest.raises(ValueError):
        text.read_lines(io.BytesIO(), m.LineReadRequest(), chunk_size=0)


async def test_non_posix_attachment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = LocalEnvironmentConfig(base_project_path=str(tmp_path))
    with monkeypatch.context() as patch:
        patch.setattr(local.os, "name", "nt")
        with pytest.raises(EnvironmentError) as error:
            await local.LocalEnvironment.open(config)
    assert error.value.code == ErrorCode.UNSUPPORTED


@pytest.mark.skipif(os.name != "posix", reason="local backend requires POSIX")
async def test_failed_replacement_preserves_original_and_cleans_temporary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = tmp_path / "file"
    original.write_bytes(b"original")

    def fail_replace(source: Path, destination: Path) -> None:
        raise OSError(errno.ENOSPC, "no space")

    monkeypatch.setattr(local.os, "replace", fail_replace)
    result = await PatchService(tmp_path).apply(
        "*** Begin Patch\n*** Update File: file\n@@\n-original\n+new\n*** End Patch"
    )
    assert result.is_error and not result.changes
    assert "resource_limit" in result.errors[0].msg
    assert result.errors[0].may_have_occurred
    assert original.read_bytes() == b"original"
    assert set(tmp_path.iterdir()) == {original}


@pytest.mark.skipif(os.name != "posix", reason="local backend requires POSIX")
async def test_failed_removal_reports_uncertainty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = tmp_path / "file"
    original.write_text("original")

    def fail_remove(path: Path, missing_ok: bool = False) -> None:
        raise PermissionError(errno.EACCES, "denied")

    monkeypatch.setattr(Path, "unlink", fail_remove)
    result = await PatchService(tmp_path).apply(
        "*** Begin Patch\n*** Delete File: file\n*** End Patch"
    )
    assert result.is_error and not result.changes
    assert "denied" in result.errors[0].msg
    assert result.errors[0].may_have_occurred
    assert original.read_text() == "original"


@pytest.mark.skipif(os.name != "posix", reason="local backend requires POSIX")
async def test_mkdir_checks_final_symlink(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (root / "escape").symlink_to(outside, target_is_directory=True)
    result = await PatchService(root).apply(
        "*** Begin Patch\n*** Add File: escape/file\n+new\n*** End Patch"
    )
    assert result.is_error and not result.changes
    assert "outside_root" in result.errors[0].msg
    assert list(outside.iterdir()) == []
