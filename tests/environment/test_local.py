import io
import os
from pathlib import Path

import pytest

from vectorops.runner.environment import models, paths
from vectorops.runner.environment.configuration import LocalEnvironmentConfig
from vectorops.runner.environment.errors import EnvironmentError, ErrorCode
from vectorops.runner.environment.local import LocalEnvironment
from vectorops.runner.environment.text import read_lines


async def test_attachment_requires_existing_directory(tmp_path: Path) -> None:
    for path, code in (
        (tmp_path / "missing", ErrorCode.MISSING),
        (tmp_path / "file", ErrorCode.CONFLICT),
    ):
        if path.name == "file":
            path.touch()
        with pytest.raises(EnvironmentError) as error:
            await LocalEnvironment.open(
                LocalEnvironmentConfig(base_project_path=str(path))
            )
        assert error.value.code == code
        assert not error.value.may_have_occurred


@pytest.mark.parametrize(
    "value", ["../outside", "nested/../../outside", "/tmp/outside", "a/../b"]
)
def test_path_rejection(tmp_path: Path, value: str) -> None:
    with pytest.raises(EnvironmentError) as error:
        paths.resolve_path(tmp_path, value)
    assert error.value.code == ErrorCode.OUTSIDE_ROOT


@pytest.mark.parametrize("value", ["", "bad\0path"])
def test_invalid_path(tmp_path: Path, value: str) -> None:
    with pytest.raises(EnvironmentError) as error:
        paths.resolve_path(tmp_path, value)
    assert error.value.code == ErrorCode.INVALID_PATH


def test_symlink_and_mutation_guards(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    (root / "escape").symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(EnvironmentError):
        paths.resolve_path(root, "escape/file")
    assert paths.resolve_path(root, "escape", follow_final=False) == root / "escape"
    with pytest.raises(EnvironmentError) as error:
        paths.resolve_path(root, ".", mutation=True)
    assert error.value.code == ErrorCode.DENIED


@pytest.mark.skipif(os.name != "posix", reason="FIFO requires POSIX")
def test_special_file_is_not_read(tmp_path: Path) -> None:
    fifo = tmp_path / "fifo"
    os.mkfifo(fifo)
    with pytest.raises(EnvironmentError) as error:
        paths.open_regular(fifo)
    assert error.value.code == ErrorCode.UNSUPPORTED


@pytest.mark.parametrize("chunk_size", [1, 2, 3, 8, 65536])
def test_lines_preserve_multibyte_and_newlines(chunk_size: int) -> None:
    result = read_lines(
        io.BytesIO("skip\n\u00e9\r\nlast".encode()),
        models.LineReadRequest(start_line=2),
        chunk_size=chunk_size,
    )
    assert result.text == "\u00e9\r\nlast"
    assert result.complete_lines == 2
    assert result.eof and not result.truncated


def test_line_boundaries_and_budget() -> None:
    content = b"one\ntwo\nlast"
    selected = read_lines(
        io.BytesIO(content), models.LineReadRequest(start_line=2, max_lines=1)
    )
    assert selected.text == "two\n" and selected.complete_lines == 1
    assert not selected.eof and not selected.truncated
    skipped = read_lines(
        io.BytesIO(content), models.LineReadRequest(start_line=20, byte_limit=3)
    )
    assert skipped.text == "" and skipped.truncated
    partial = read_lines(io.BytesIO(content), models.LineReadRequest(byte_limit=6))
    assert partial.text == "one\ntw" and partial.complete_lines == 1
    assert partial.truncated and partial.bytes_read == 6
    beyond = read_lines(io.BytesIO(content), models.LineReadRequest(start_line=20))
    assert beyond.text == "" and beyond.eof and not beyond.truncated
    empty = read_lines(io.BytesIO(), models.LineReadRequest())
    assert empty.eof and empty.complete_lines == 0


def test_partial_character_and_invalid_utf8() -> None:
    partial = read_lines(
        io.BytesIO("\u00e9".encode()), models.LineReadRequest(byte_limit=1)
    )
    assert partial.text == "" and partial.truncated
    with pytest.raises(EnvironmentError) as error:
        read_lines(io.BytesIO(b"\xc3"), models.LineReadRequest())
    assert error.value.code == ErrorCode.INVALID_TEXT
