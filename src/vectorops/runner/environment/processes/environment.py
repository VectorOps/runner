"""Explicit child-environment precedence, shared without reading host state."""

from collections.abc import Mapping
from types import MappingProxyType
from typing import Final

from .models import EnvPolicy

PLAIN_OUTPUT_DEFAULTS: Final[Mapping[str, str]] = MappingProxyType(
    {
        "TERM": "dumb",
        "NO_COLOR": "1",
        "COLORTERM": "",
        "PAGER": "cat",
        "GIT_PAGER": "cat",
        "GH_PAGER": "cat",
    }
)


def build_environment(
    policy: EnvPolicy,
    parent: Mapping[str, str],
    overlay: Mapping[str, str],
) -> dict[str, str]:
    values = dict(parent) if policy.inherit_parent else {}
    values.update(PLAIN_OUTPUT_DEFAULTS)
    if policy.allowlist is not None:
        values = {
            key: value for key, value in values.items() if key in policy.allowlist
        }
    for key in policy.denylist or ():
        values.pop(key, None)
    values.update(policy.defaults)
    values.update(overlay)
    return values
