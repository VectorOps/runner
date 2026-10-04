import os
from pathlib import Path

import pydantic
import pytest

from vectorops.runner.environment import models as m
from vectorops.runner.environment.configuration import LocalEnvironmentConfig
from vectorops.runner.environment.errors import EnvironmentError, ErrorCode
from vectorops.runner.environment.factory import open_environment
from vectorops.runner.environment.local import LocalEnvironment
from vectorops.runner.environment.protocol import TargetEnvironment
from vectorops.runner.environment.text import read_lines


pytestmark = pytest.mark.skipif(
    os.name != "posix", reason="local backend requires POSIX"
)


@pytest.fixture
async def environment(tmp_path: Path) -> TargetEnvironment:
    return await open_environment(
        LocalEnvironmentConfig(base_project_path=str(tmp_path))
    )


async def test_description(environment: TargetEnvironment) -> None:
    assert isinstance(environment, TargetEnvironment)
    description = await environment.describe()
    assert description.identity.startswith("file:///")
    assert description.capabilities.atomic_file_replacement
    assert description.capabilities.processes
    assert not description.capabilities.patching
    assert not description.race_resistant_containment
    assert description.process_execution_allowed


async def test_attachment_requires_existing_directory(tmp_path: Path) -> None:
    with pytest.raises(EnvironmentError) as missing:
        await open_environment(
            LocalEnvironmentConfig(base_project_path=str(tmp_path / "missing"))
        )
    assert missing.value.code == ErrorCode.MISSING
    assert not missing.value.may_have_occurred
    (tmp_path / "file").touch()
    with pytest.raises(EnvironmentError) as invalid:
        await open_environment(
            LocalEnvironmentConfig(base_project_path=str(tmp_path / "file"))
        )
    assert invalid.value.code == ErrorCode.CONFLICT


@pytest.mark.parametrize(
    ("offset", "limit", "expected", "eof"),
    [(0, 2, b"ab", False), (2, 2, b"cd", True), (4, 2, b"", True), (20, 2, b"", True)],
)
async def test_byte_ranges(
    environment: TargetEnvironment, offset: int, limit: int, expected: bytes, eof: bool
) -> None:
    await environment.write_file(m.WriteRequest(path="file", content=b"abcd"))
    result = await environment.read_file(
        m.ReadRequest(path="file", offset=offset, limit=limit)
    )
    assert result.content == expected
    assert result.eof is eof


async def test_binary_serialization(environment: TargetEnvironment) -> None:
    request = m.WriteRequest(path="binary", content=b"\x00\xff\x80")
    assert m.WriteRequest.model_validate_json(request.model_dump_json()) == request
    await environment.write_file(request)
    result = await environment.read_file(m.ReadRequest(path="binary"))
    assert result.content == request.content
    assert m.ReadResult.model_validate_json(result.model_dump_json()) == result


@pytest.mark.parametrize(
    "path", ["../outside", "nested/../../outside", "/tmp/outside", "a/../b"]
)
async def test_path_rejection(environment: TargetEnvironment, path: str) -> None:
    with pytest.raises(EnvironmentError) as error:
        await environment.write_file(m.WriteRequest(path=path, content=b"bad"))
    assert error.value.code == ErrorCode.OUTSIDE_ROOT


@pytest.mark.parametrize("path", ["", "bad\0path"])
async def test_invalid_path(environment: TargetEnvironment, path: str) -> None:
    with pytest.raises(EnvironmentError) as error:
        await environment.stat(m.PathRequest(path=path))
    assert error.value.code == ErrorCode.INVALID_PATH


async def test_symlink_semantics(
    environment: TargetEnvironment, tmp_path: Path
) -> None:
    root = tmp_path / "target"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "file").write_bytes(b"outside")
    (root / "escape").symlink_to(outside, target_is_directory=True)
    (root / "dangling").symlink_to(outside / "missing")
    environment = await LocalEnvironment.open(
        LocalEnvironmentConfig(base_project_path=str(root))
    )
    assert (await environment.stat(m.PathRequest(path="escape"))).kind == "symlink"
    assert (await environment.stat(m.PathRequest(path="dangling"))).kind == "symlink"
    with pytest.raises(EnvironmentError) as read_error:
        await environment.read_file(m.ReadRequest(path="escape/file"))
    assert read_error.value.code == ErrorCode.OUTSIDE_ROOT
    with pytest.raises(EnvironmentError) as write_error:
        await environment.write_file(m.WriteRequest(path="escape/new", content=b"bad"))
    assert write_error.value.code == ErrorCode.OUTSIDE_ROOT
    await environment.write_file(m.WriteRequest(path="source", content=b"source"))
    with pytest.raises(EnvironmentError) as rename_error:
        await environment.rename(
            m.RenameRequest(path="source", destination="escape/new")
        )
    assert rename_error.value.code == ErrorCode.OUTSIDE_ROOT
    await environment.remove(m.RemoveRequest(path="escape", recursive=True))
    assert (outside / "file").read_bytes() == b"outside"
    (root / "alias").symlink_to(root / "source")
    assert (
        await environment.read_file(m.ReadRequest(path="alias"))
    ).content == b"source"
    with pytest.raises(EnvironmentError) as replace_error:
        await environment.write_file(
            m.WriteRequest(path="alias", content=b"bad", mode="replace")
        )
    assert replace_error.value.code == ErrorCode.CONFLICT


async def test_mutation_defaults(
    environment: TargetEnvironment, tmp_path: Path
) -> None:
    await environment.mkdir(m.MkdirRequest(path="nested/child", parents=True))
    await environment.write_file(
        m.WriteRequest(path="nested/child/file", content=b"first")
    )
    with pytest.raises(EnvironmentError) as conflict:
        await environment.write_file(
            m.WriteRequest(path="nested/child/file", content=b"second")
        )
    assert conflict.value.code == ErrorCode.CONFLICT
    assert (tmp_path / "nested/child/file").read_bytes() == b"first"
    (tmp_path / "nested/child/file").chmod(0o640)
    await environment.write_file(
        m.WriteRequest(path="nested/child/file", content=b"second", mode="replace")
    )
    assert (tmp_path / "nested/child/file").stat().st_mode & 0o777 == 0o640
    with pytest.raises(EnvironmentError) as missing:
        await environment.write_file(
            m.WriteRequest(path="absent", content=b"", mode="replace")
        )
    assert missing.value.code == ErrorCode.MISSING
    await environment.write_file(
        m.WriteRequest(path="destination", content=b"existing", mode="upsert")
    )
    with pytest.raises(EnvironmentError):
        await environment.rename(
            m.RenameRequest(path="nested/child/file", destination="destination")
        )
    assert (tmp_path / "destination").read_bytes() == b"existing"
    await environment.rename(
        m.RenameRequest(
            path="nested/child/file", destination="destination", overwrite=True
        )
    )
    assert (tmp_path / "destination").read_bytes() == b"second"
    with pytest.raises(EnvironmentError):
        await environment.remove(m.RemoveRequest(path="nested"))
    await environment.remove(m.RemoveRequest(path="nested", recursive=True))
    assert not (tmp_path / "nested").exists()
    assert not list(tmp_path.glob(".target-*"))


async def test_root_cannot_be_mutated(environment: TargetEnvironment) -> None:
    with pytest.raises(EnvironmentError) as error:
        await environment.remove(m.RemoveRequest(recursive=True))
    assert error.value.code == ErrorCode.DENIED
    assert (await environment.stat(m.PathRequest())).kind == "directory"


async def test_directory_listing(environment: TargetEnvironment) -> None:
    for name in ("a", "b", "c"):
        await environment.write_file(m.WriteRequest(path=name, content=b"data"))
    result = await environment.list_dir(m.ListRequest(limit=2))
    assert len(result.entries) == 2
    assert result.truncated
    all_entries = await environment.list_dir(m.ListRequest(limit=3))
    assert not all_entries.truncated
    assert {entry.path for entry in all_entries.entries} == {"a", "b", "c"}
    assert all(
        entry.kind == "file" and entry.size == 4 for entry in all_entries.entries
    )


async def test_special_file_is_not_read(
    environment: TargetEnvironment, tmp_path: Path
) -> None:
    os.mkfifo(tmp_path / "fifo")
    with pytest.raises(EnvironmentError) as error:
        await environment.read_file(m.ReadRequest(path="fifo"))
    assert error.value.code == ErrorCode.UNSUPPORTED


async def test_limits(tmp_path: Path) -> None:
    environment = await LocalEnvironment.open(
        LocalEnvironmentConfig(
            base_project_path=str(tmp_path),
            max_read_bytes=4,
            max_write_bytes=4,
            max_list_entries=1,
        )
    )
    with pytest.raises(EnvironmentError) as error:
        await environment.write_file(m.WriteRequest(path="file", content=b"12345"))
    assert error.value.code == ErrorCode.RESOURCE_LIMIT
    with pytest.raises(EnvironmentError):
        await environment.read_file(m.ReadRequest(path="file", limit=5))
    with pytest.raises(EnvironmentError):
        await environment.read_lines(m.LineReadRequest(path="file", byte_limit=5))
    with pytest.raises(EnvironmentError):
        await environment.list_dir(m.ListRequest(limit=2))
    assert not (tmp_path / "file").exists()


@pytest.mark.parametrize("chunk_size", [1, 2, 3, 8, 65536])
async def test_lines_preserve_multibyte_and_newlines(
    environment: TargetEnvironment, chunk_size: int
) -> None:
    await environment.write_file(
        m.WriteRequest(path="text", content="skip\n\u00e9\r\n\U0001f600 last".encode())
    )
    result = await read_lines(
        environment, m.LineReadRequest(path="text", start_line=2), chunk_size=chunk_size
    )
    assert result.text == "\u00e9\r\n\U0001f600 last"
    assert result.complete_lines == 2
    assert result.eof and not result.truncated


async def test_line_boundaries_and_budget(environment: TargetEnvironment) -> None:
    await environment.write_file(m.WriteRequest(path="text", content=b"one\ntwo\nlast"))
    selected = await environment.read_lines(
        m.LineReadRequest(path="text", start_line=2, max_lines=1)
    )
    assert selected.text == "two\n"
    assert selected.complete_lines == 1
    assert not selected.eof and not selected.truncated
    skipped = await environment.read_lines(
        m.LineReadRequest(path="text", start_line=20, byte_limit=3)
    )
    assert skipped.text == "" and skipped.truncated
    partial = await environment.read_lines(m.LineReadRequest(path="text", byte_limit=6))
    assert partial.text == "one\ntw" and partial.complete_lines == 1
    assert partial.truncated and partial.bytes_read == 6
    beyond = await environment.read_lines(m.LineReadRequest(path="text", start_line=20))
    assert beyond.text == "" and beyond.eof and not beyond.truncated
    await environment.write_file(m.WriteRequest(path="empty", content=b""))
    empty = await environment.read_lines(m.LineReadRequest(path="empty"))
    assert empty.eof and empty.complete_lines == 0


async def test_partial_character_and_invalid_utf8(
    environment: TargetEnvironment,
) -> None:
    await environment.write_file(m.WriteRequest(path="text", content="\u00e9".encode()))
    partial = await environment.read_lines(m.LineReadRequest(path="text", byte_limit=1))
    assert partial.text == "" and partial.truncated
    await environment.write_file(m.WriteRequest(path="bad", content=b"\xc3"))
    with pytest.raises(EnvironmentError) as error:
        await environment.read_lines(m.LineReadRequest(path="bad"))
    assert error.value.code == ErrorCode.INVALID_TEXT


def test_request_validation() -> None:
    for data in ({"offset": -1}, {"limit": 0}, {"limit": True}):
        with pytest.raises(pydantic.ValidationError):
            m.ReadRequest.model_validate(data)
    for data in ({"start_line": 0}, {"max_lines": 0}, {"byte_limit": 0}):
        with pytest.raises(pydantic.ValidationError):
            m.LineReadRequest.model_validate(data)
