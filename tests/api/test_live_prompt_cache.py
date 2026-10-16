import json
from typing import Optional
from unittest.mock import AsyncMock

import aiohttp
import connect
import pytest

from tests import live_prompt_cache
from tests.api.test_prompt_cache_history import Prompt
from tests.live_prompt_cache import Response, payload, select


def prompt() -> Prompt:
    return Prompt(
        "step",
        json.dumps(
            {
                "model": "gpt-6.1-sol",
                "instructions": "private",
                "tools": [{"name": "tool"}],
                "reasoning": {"effort": "high"},
            }
        ),
        [
            json.dumps({"type": "reasoning", "encrypted_content": "opaque"}),
            json.dumps({"role": "user", "content": "exact history"}),
        ],
        {"input_tokens": 100, "cache_read_tokens": 200, "cache_write_tokens": 10},
    )


def test_payload_preserves_history_and_only_adds_explicit_controls() -> None:
    saved = prompt()
    body = payload(saved, 256, "baseline")
    assert body["instructions"] == "private"
    assert body["input"] == [json.loads(item) for item in saved.items]
    assert body["tools"] == [{"name": "tool"}]
    assert body["reasoning"] == {"effort": "high"}
    assert body["prompt_cache_options"] == {"comparison_response_id": "baseline"}
    assert body["max_output_tokens"] == 256
    assert body["store"] is False
    assert "prompt_cache_key" not in body
    assert payload(saved, 256, cache_key="stable")["prompt_cache_key"] == "stable"


@pytest.mark.parametrize(
    "start,count,limit", [(0, 1, 1000), (1, 11, 10000), (2, 2, 1000), (1, 2, 619)]
)
def test_selection_rejects_invalid_ranges_and_budgets(
    start: int, count: int, limit: int
) -> None:
    with pytest.raises(ValueError):
        select([prompt(), prompt()], start, count, limit)


def test_selection_counts_all_input_categories() -> None:
    saved = [prompt(), prompt()]
    assert select(saved, 1, 2, 620) == saved
    saved[0].recorded_usage = {}
    with pytest.raises(ValueError, match="recorded input"):
        select(saved, 1, 1, 620)


def test_response_discards_generated_content_but_retains_diagnostics() -> None:
    result = Response.model_validate(
        {
            "id": "response",
            "status": "completed",
            "output": [{"text": "private"}],
            "usage": {
                "input_tokens": 1000,
                "input_tokens_details": {
                    "cached_tokens": 900,
                    "cache_write_tokens": 100,
                },
            },
            "prompt_cache_diagnostics": {"type": "cache_hit"},
        }
    )
    assert result.usage.input_tokens_details.cached_tokens == 900
    assert result.prompt_cache_diagnostics == {"type": "cache_hit"}
    assert "output" not in result.model_dump()


@pytest.mark.parametrize("cache_key", [None, "stable-session"])
async def test_chatgpt_uses_configured_auth_and_optional_session(
    monkeypatch: pytest.MonkeyPatch, cache_key: Optional[str]
) -> None:
    auth = connect.NoAuth()
    transport = AsyncMock(return_value=auth)
    stream = AsyncMock(side_effect=RuntimeError("transport boundary"))
    monkeypatch.setattr(live_prompt_cache.AuthenticationService, "transport", transport)
    monkeypatch.setattr(live_prompt_cache.HttpTransport, "stream", stream)
    body = payload(prompt(), 256, cache_key=cache_key)
    async with aiohttp.ClientSession() as session:
        with pytest.raises(RuntimeError, match="transport boundary"):
            await live_prompt_cache.send_chatgpt(session, body, "configured-profile")
    transport.assert_awaited_once_with("chatgpt", "gpt-6.1-sol", "configured-profile")
    stream.assert_awaited_once()
    assert stream.call_args.args == (
        "POST",
        "https://chatgpt.com/backend-api/codex/responses",
    )
    options = stream.call_args.kwargs
    assert options["auth"] is auth
    assert options["json_body"] == body
    assert options["headers"].get("session_id") == cache_key
    assert options["headers"]["idempotency-key"]
