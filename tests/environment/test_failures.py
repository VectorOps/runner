import errno
import os
from pathlib import Path

import pytest

from vectorops.runner.environment import local, models as m, text
from vectorops.runner.environment.configuration import LocalEnvironmentConfig
from vectorops.runner.environment.errors import EnvironmentError, ErrorCode


class ShortReader:
    async def read_file(self, request: m.ReadRequest) -> m.ReadResult:
        content = b"first\nsecond\n"
        end = min(request.offset + 2, request.offset + request.limit)
        return m.ReadResult(
            content=content[request.offset : end], eof=end >= len(content)
        )


class StalledReader:
    async def read_file(self, request: m.ReadRequest) -> m.ReadResult:
        return m.ReadResult(content=b"", eof=False)


async def test_shared_reader_does_not_treat_short_read_as_eof() -> None:
    result = await text.read_lines(ShortReader(), m.LineReadRequest())
    assert result.text == "first\nsecond\n"
    assert result.complete_lines == 2
    assert result.eof and not result.truncated


async def test_shared_reader_rejects_no_progress() -> None:
    with pytest.raises(EnvironmentError) as error:
        await text.read_lines(StalledReader(), m.LineReadRequest())
    assert error.value.code == ErrorCode.IO


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
    environment = await local.LocalEnvironment.open(
        LocalEnvironmentConfig(base_project_path=str(tmp_path))
    )
    original = tmp_path / "file"
    original.write_bytes(b"original")

    def fail_replace(source: Path, destination: Path) -> None:
        raise OSError(errno.ENOSPC, "no space")

    monkeypatch.setattr(local.os, "replace", fail_replace)
    with pytest.raises(EnvironmentError) as error:
        await environment.write_file(
            m.WriteRequest(path="file", content=b"new", mode="replace")
        )
    assert error.value.code == ErrorCode.RESOURCE_LIMIT
    assert error.value.may_have_occurred
    assert original.read_bytes() == b"original"
    assert set(tmp_path.iterdir()) == {original}


@pytest.mark.skipif(os.name != "posix", reason="local backend requires POSIX")
async def test_partial_removal_reports_uncertainty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    environment = await local.LocalEnvironment.open(
        LocalEnvironmentConfig(base_project_path=str(tmp_path))
    )
    directory = tmp_path / "directory"
    directory.mkdir()
    (directory / "first").touch()
    (directory / "second").touch()

    def partial_remove(path: Path) -> None:
        (path / "first").unlink()
        raise PermissionError(errno.EACCES, "denied")

    monkeypatch.setattr(local.shutil, "rmtree", partial_remove)
    with pytest.raises(EnvironmentError) as error:
        await environment.remove(m.RemoveRequest(path="directory", recursive=True))
    assert error.value.code == ErrorCode.DENIED
    assert error.value.may_have_occurred
    assert not (directory / "first").exists()
    assert (directory / "second").exists()


@pytest.mark.skipif(os.name != "posix", reason="local backend requires POSIX")
async def test_mkdir_checks_final_symlink(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (root / "escape").symlink_to(outside, target_is_directory=True)
    environment = await local.LocalEnvironment.open(
        LocalEnvironmentConfig(base_project_path=str(root))
    )
    with pytest.raises(EnvironmentError) as error:
        await environment.mkdir(m.MkdirRequest(path="escape", exist_ok=True))
    assert error.value.code == ErrorCode.OUTSIDE_ROOT
