"""Provider selection for Connect model references without catalog lookup."""

from typing import Optional

import connect


def qualified(model: str) -> bool:
    return connect.split_model_reference(model)[0] is not None


def provider(model: str, explicit: Optional[str] = None) -> Optional[str]:
    """Resolve qualified and legacy selections without rewriting recorded settings."""
    selected, name, _tag = connect.split_model_reference(model)
    if not name.strip():
        raise ValueError("model must not be blank")
    if selected is not None and (not selected.strip() or selected != selected.strip()):
        raise ValueError("model must use a nonempty provider/model reference")
    if explicit is not None:
        if not explicit.strip() or explicit != explicit.strip():
            raise ValueError("provider must not be blank or padded")
        if selected is not None and explicit != selected:
            raise ValueError("provider conflicts with the provider/model reference")
    return selected if selected is not None else explicit
