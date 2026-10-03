"""ChatGPT authorization adapter with sanitized, classified token failures."""

import asyncio
import time
from typing import Optional, Protocol

import aiohttp
from connect import auth
from connect.credentials import chatgpt

from vectorops.runner.authentication import models as m


class OAuthAdapter(Protocol):
    redirect_uri: str

    def authorization_url(self, challenge: str, state: str) -> str: ...

    async def exchange(
        self, profile: m.Profile, code: str, verifier: str
    ) -> m.Credential: ...

    async def refresh(self, credential: m.Credential) -> m.Credential: ...


class ChatGPT:
    redirect_uri = chatgpt.ChatGPTOAuthSettings().redirect_uri

    def authorization_url(self, challenge: str, state: str) -> str:
        return chatgpt.build_chatgpt_authorization_url(
            code_challenge=challenge, state=state
        )

    async def exchange(
        self, profile: m.Profile, code: str, verifier: str
    ) -> m.Credential:
        return await self._request(
            profile,
            {
                "grant_type": "authorization_code",
                "code": code,
                "code_verifier": verifier,
                "redirect_uri": self.redirect_uri,
            },
        )

    async def refresh(self, credential: m.Credential) -> m.Credential:
        if not credential.refresh_token:
            raise m.LoginRequired("OAuth authorization required")
        return await self._request(
            credential,
            {
                "grant_type": "refresh_token",
                "refresh_token": credential.refresh_token,
            },
            previous=credential,
        )

    async def _request(
        self,
        profile: m.Profile,
        data: dict[str, str],
        *,
        previous: Optional[m.Credential] = None,
    ) -> m.Credential:
        """Bound token retries and never propagate provider payloads into history."""
        data = {**data, "client_id": chatgpt.CLIENT_ID}
        for attempt in range(3):
            try:
                async with aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as session:
                    async with session.post(chatgpt.TOKEN_URL, data=data) as response:
                        if response.status == 429 or response.status >= 500:
                            raise aiohttp.ClientConnectionError()
                        if response.status >= 400:
                            failure = await response.json(content_type=None)
                            if isinstance(failure, dict) and failure.get("error") in (
                                "invalid_grant",
                                "invalid_token",
                                "access_denied",
                            ):
                                raise m.LoginRequired("OAuth authorization required")
                            raise m.AuthenticationError(
                                "OAuth token request was rejected"
                            )
                        payload = await response.json(content_type=None)
                if not isinstance(payload, dict):
                    raise ValueError()
                token = payload.get("access_token")
                refresh = payload.get("refresh_token") or (
                    previous.refresh_token if previous else None
                )
                lifetime = payload.get("expires_in")
                if (
                    not isinstance(token, str)
                    or not token
                    or not isinstance(refresh, str)
                    or not refresh
                ):
                    raise ValueError()
                if (
                    not isinstance(lifetime, (int, float))
                    or isinstance(lifetime, bool)
                    or lifetime <= 0
                ):
                    raise ValueError()
                account = auth.extract_chatgpt_account_id(token)
                if not account:
                    raise ValueError()
                return m.Credential(
                    provider=profile.provider,
                    profile=profile.profile,
                    kind="oauth2",
                    secret=token,
                    refresh_token=refresh,
                    expires_at=time.time() + lifetime,
                    account_id=account,
                )
            except m.AuthenticationError:
                raise
            except (aiohttp.ClientError, TimeoutError):
                if attempt == 2:
                    raise m.AuthenticationError(
                        "OAuth token service is unavailable"
                    ) from None
                await asyncio.sleep(2**attempt)
            except Exception:
                raise m.AuthenticationError("invalid OAuth token response") from None
        raise m.AuthenticationError("OAuth token service is unavailable")
