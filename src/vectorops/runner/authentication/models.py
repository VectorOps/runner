"""Authentication contracts; secret-bearing messages never enter workflow history."""

from typing import Annotated, Literal, Optional

import pydantic


class Model(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", hide_input_in_errors=True)


ProfileName = Annotated[str, pydantic.Field(pattern=r"^[A-Za-z0-9_-]{1,64}$")]


class ProfileQuery(Model):
    name: ProfileName


class ProfileListing(Model):
    active_profile: ProfileName = "default"
    profiles: list[ProfileName] = pydantic.Field(default_factory=lambda: ["default"])

    @pydantic.model_validator(mode="after")
    def valid_profiles(self) -> "ProfileListing":
        if "default" not in self.profiles or self.active_profile not in self.profiles:
            raise ValueError("default and active profiles must exist")
        if len(set(self.profiles)) != len(self.profiles):
            raise ValueError("profile names must be unique")
        return self


class ProfileResult(ProfileListing):
    changed: bool


class Profile(Model):
    provider: str = pydantic.Field(pattern=r"^[a-z][a-z0-9_-]{0,63}$")
    profile: ProfileName = "default"

    @property
    def key(self) -> str:
        return f"{self.provider}.{self.profile}"


class Credential(Profile):
    kind: Literal["api_key", "oauth2"]
    secret: str = pydantic.Field(min_length=1, max_length=65536, repr=False)
    refresh_token: Optional[str] = pydantic.Field(
        default=None, max_length=65536, repr=False
    )
    expires_at: Optional[float] = pydantic.Field(default=None, allow_inf_nan=False)
    account_id: Optional[str] = None

    def reusable(self, now: float, rejected: Optional[str] = None) -> bool:
        return (
            self.kind == "oauth2"
            and self.secret != rejected
            and self.expires_at is not None
            and self.expires_at > now + 60
        )


class Login(Model):
    provider: str = pydantic.Field(pattern=r"^[a-z][a-z0-9_-]{0,63}$")
    profile: Optional[ProfileName] = None


class Requirement(Profile):
    id: str
    status: Literal["waiting", "claimed"] = "waiting"
    execution_ids: list[str] = pydantic.Field(default_factory=list)


class Reference(Model):
    id: str = pydantic.Field(min_length=1, max_length=128)


class Claim(Reference):
    attempt: str
    state: str = pydantic.Field(repr=False)
    redirect_uri: str
    expires_at: float


class Attempt(Reference):
    attempt: str = pydantic.Field(min_length=1, max_length=128)


class Callback(Attempt):
    state: str = pydantic.Field(min_length=1, max_length=256, repr=False)
    code: Optional[str] = pydantic.Field(
        default=None, min_length=1, max_length=16384, repr=False
    )
    error: Optional[str] = pydantic.Field(default=None, max_length=256, repr=False)


class AuthenticationError(RuntimeError):
    pass


class AuthenticationCancelled(AuthenticationError):
    pass


class AuthenticationConflict(AuthenticationError):
    pass


class CredentialStorageError(RuntimeError):
    pass


class LoginRequired(AuthenticationError):
    pass
