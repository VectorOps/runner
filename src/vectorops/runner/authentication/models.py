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


class AuthenticationProvider(Model):
    provider: str = pydantic.Field(pattern=r"^[a-z][a-z0-9_-]{0,63}$")
    display_name: str
    credential_kinds: list[Literal["api_key", "oauth2"]]
    oauth_login: bool


class AuthenticationProviders(Model):
    providers: list[AuthenticationProvider] = pydantic.Field(default_factory=list)
    custom_api_key_providers: bool = True


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


class CredentialQuery(Model):
    provider: str = pydantic.Field(pattern=r"^[a-z][a-z0-9_-]{0,63}$")
    profile: Optional[ProfileName] = None


class CredentialListQuery(Model):
    profile: Optional[ProfileName] = None
    after: Optional[str] = pydantic.Field(
        default=None, pattern=r"^[a-z][a-z0-9_-]{0,63}$"
    )
    limit: int = pydantic.Field(default=100, ge=1, le=500, strict=True)


class CredentialStatus(Profile):
    """Local metadata only; no assertion of provider acceptance or refresh success."""

    status: Literal[
        "missing", "unreadable", "present", "expired", "expiring", "unknown_expiry"
    ]
    kind: Optional[Literal["api_key", "oauth2"]] = None
    expires_at: Optional[float] = None
    has_refresh_token: bool = False

    @classmethod
    def from_credential(cls, value: Credential, now: float) -> "CredentialStatus":
        status: Literal["present", "expired", "expiring", "unknown_expiry"] = "present"
        if value.expires_at is not None and value.expires_at <= now:
            status = "expired"
        elif value.kind == "oauth2":
            if value.expires_at is None:
                status = "unknown_expiry"
            elif not value.reusable(now):
                status = "expiring"
        return cls(
            provider=value.provider,
            profile=value.profile,
            status=status,
            kind=value.kind,
            expires_at=value.expires_at,
            has_refresh_token=bool(value.refresh_token),
        )


class CredentialPage(Model):
    profile: ProfileName
    credentials: list[CredentialStatus] = pydantic.Field(default_factory=list)
    next_after: Optional[str] = None


class CredentialDeleted(Profile):
    deleted: bool


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
