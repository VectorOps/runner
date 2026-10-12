"""Process policy and serializable commands, session results, and controls."""

import typing

import pydantic


class Model(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(
        extra="forbid", frozen=True, ser_json_bytes="base64", val_json_bytes="base64"
    )


def validate_environment(values: dict[str, str]) -> dict[str, str]:
    if any(
        not key or "=" in key or "\0" in key or "\0" in value
        for key, value in values.items()
    ):
        raise ValueError("environment names and values must be valid process strings")
    return values


class EnvPolicy(Model):
    inherit_parent: bool = True
    allowlist: typing.Optional[list[str]] = None
    denylist: typing.Optional[list[str]] = None
    defaults: dict[str, str] = pydantic.Field(default_factory=dict)

    _validate_env = pydantic.field_validator("defaults")(validate_environment)


class ProcessConfig(Model):
    allow_shell: bool = False
    environment: EnvPolicy = pydantic.Field(default_factory=EnvPolicy)
    max_sessions: int = pydantic.Field(default=32, gt=0, strict=True)
    max_output_bytes: int = pydantic.Field(default=1048576, gt=0, strict=True)
    max_response_bytes: int = pydantic.Field(default=65536, gt=0, strict=True)
    max_stdin_bytes: int = pydantic.Field(default=65536, gt=0, strict=True)
    retention_seconds: float = pydantic.Field(default=300, gt=0, allow_inf_nan=False)
    termination_grace_seconds: float = pydantic.Field(
        default=1, gt=0, allow_inf_nan=False
    )
    io_timeout_seconds: float = pydantic.Field(default=5, gt=0, allow_inf_nan=False)


class ArgvCommand(Model):
    kind: typing.Literal["argv"] = "argv"
    argv: list[str] = pydantic.Field(min_length=1)

    @pydantic.field_validator("argv")
    @classmethod
    def validate_argv(cls, value: list[str]) -> list[str]:
        if not value[0] or any("\0" in item for item in value):
            raise ValueError("argv requires an executable and cannot contain NULs")
        return value


class ShellCommand(Model):
    kind: typing.Literal["shell"] = "shell"
    script: str = pydantic.Field(min_length=1, pattern=r"^[^\x00]*$")


Command = typing.Annotated[
    typing.Union[ArgvCommand, ShellCommand], pydantic.Field(discriminator="kind")
]


class ExecRequest(Model):
    command: Command
    cwd: str = "."
    env: dict[str, str] = pydantic.Field(default_factory=dict)
    mode: typing.Literal["pipe", "pty"] = "pipe"
    stdin: bytes = b""
    keep_stdin_open: bool = False
    kill_tree: bool = True
    yield_timeout_ms: int = pydantic.Field(default=60000, ge=0, le=600000, strict=True)
    execution_timeout_ms: typing.Optional[int] = pydantic.Field(
        default=None, gt=0, strict=True
    )

    _validate_env = pydantic.field_validator("env")(validate_environment)


class SessionRequest(Model):
    process_id: str


class PollRequest(SessionRequest):
    cursor: int = pydantic.Field(default=0, ge=0, strict=True)
    wait_timeout_ms: int = pydantic.Field(default=0, ge=0, le=30000, strict=True)


class StdinRequest(SessionRequest):
    data: bytes = b""
    eof: bool = False


class DriverCapabilities(Model):
    pty: bool = False
    stdin_eof: bool = True
    termination_scope: typing.Literal["process", "group", "none"] = "none"


class OutputChunk(Model):
    stream: typing.Literal["stdout", "stderr", "terminal"]
    data: bytes


class ExitResult(Model):
    returncode: typing.Optional[int] = None
    outcome: typing.Literal["exited", "unknown"] = "exited"

    @pydantic.model_validator(mode="after")
    def validate_outcome(self) -> typing.Self:
        if (self.outcome == "exited") != (self.returncode is not None):
            raise ValueError("only a confirmed exit has a return code")
        return self


class ProcessResult(Model):
    process_id: str
    state: typing.Literal["starting", "running", "exited", "failed", "unknown"]
    chunks: list[OutputChunk] = pydantic.Field(default_factory=list)
    cursor: int = 0
    dropped_bytes: int = 0
    output_complete: bool = False
    retained: bool = True
    returncode: typing.Optional[int] = None
    error: typing.Optional[str] = None
    timed_out: bool = False


class KillResult(Model):
    process_id: str
    termination_confirmed: bool
    scope: typing.Literal["process", "group", "none"]
