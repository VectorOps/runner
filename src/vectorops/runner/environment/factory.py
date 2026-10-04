"""Backend attachment without constructing workflow consumers."""

from .configuration import EnvironmentConfig
from .local import LocalEnvironment
from .protocol import TargetEnvironment


async def open_environment(config: EnvironmentConfig) -> TargetEnvironment:
    return await LocalEnvironment.open(config)
