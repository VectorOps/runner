"""Runtime failures distinguished from recorded activity outcomes."""

from vectorops.runner import models


class ActivityError(Exception):
    def __init__(self, error: models.StepError) -> None:
        self.error = error
        super().__init__(f"{error.type}: {error.message}")


class ReplayMismatchError(Exception):
    """Workflow code or referenced records disagree with selected history."""


class AuthorizationPersistenceError(Exception):
    """A tool cannot execute because its authorization was not durably recorded."""
