"""Provider selection for Connect model references without catalog lookup."""

from typing import Optional

import connect


def provider(model: str) -> Optional[str]:
    """Read the provider prefix without consulting the catalog or credentials."""
    selected, name, _tag = connect.split_model_reference(model)
    if not name.strip():
        raise ValueError("model must not be blank")
    if selected is not None and (not selected.strip() or selected != selected.strip()):
        raise ValueError("model must use a nonempty provider/model reference")
    return selected
