from pathlib import Path
import sys

import pydantic
import pytest

from vectorops.runner.environment.configuration import LocalEnvironmentConfig
from vectorops.runner.environment.errors import EnvironmentError, ErrorCode
from vectorops.runner.environment.local import LocalEnvironment
from vectorops.runner.environment.processes import models as processes
from vectorops.runner.environment.target import LocalExecutionTarget


@pytest.mark.parametrize(
    "value",
    [
        {"root": ""},
        {"root": "relative"},
        {"root": "/bad\0path"},
        {"root": "/project", "kind": "remote"},
        {"root": "/project", "unknown": True},
    ],
)
def test_invalid_target(value: dict[str, object]) -> None:
    with pytest.raises(pydantic.ValidationError):
        LocalExecutionTarget.model_validate(value)


async def test_override_ignores_missing_default_and_roots_processes(
    tmp_path: Path,
) -> None:
    root = tmp_path.resolve()
    config = LocalEnvironmentConfig(base_project_path=str(root / "missing"))
    target = await LocalEnvironment.resolve_target(config, str(root))
    assert target.model_dump(mode="json") == {"kind": "local", "root": str(root)}
    assert LocalExecutionTarget.model_validate_json(target.model_dump_json()) == target
    environment = await LocalEnvironment.open(config, target=target)
    try:
        assert environment.root == root
        assert config.base_project_path == str(root / "missing")
        result = await environment.processes.exec(
            processes.ExecRequest(
                command=processes.ArgvCommand(
                    argv=[sys.executable, "-c", "import os; print(os.getcwd())"]
                ),
                yield_timeout_ms=1000,
            )
        )
        output = b"".join(chunk.data for chunk in result.chunks)
        while result.state != "exited" or not result.output_complete:
            result = await environment.processes.poll(
                processes.PollRequest(
                    process_id=result.process_id,
                    cursor=result.cursor,
                    wait_timeout_ms=1000,
                )
            )
            output += b"".join(chunk.data for chunk in result.chunks)
        assert result.returncode == 0
        assert output.decode().strip() == str(root)
    finally:
        await environment.processes.close()


async def test_replaced_target_symlink_is_not_silently_followed(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    replacement = tmp_path / "replacement"
    replacement.mkdir()
    config = LocalEnvironmentConfig(base_project_path=str(root))
    target = await LocalEnvironment.resolve_target(config)
    root.rmdir()
    root.symlink_to(replacement, target_is_directory=True)
    with pytest.raises(EnvironmentError) as error:
        await LocalEnvironment.open(config, target=target)
    assert error.value.code == ErrorCode.INVALID_PATH
