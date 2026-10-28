"""Endpoint policy shared by the official DSH adapter and connection tests."""
from __future__ import annotations

from urllib.parse import urlsplit, urlunsplit


DEFAULT_MESSAGES_BASE_URL = "https://api.deepseek.com/anthropic"


def messages_base_url(base_url: str | None = None) -> str:
    """Accept saved official API roots without directing Messages at the chat API.

    Custom proxy prefixes remain intact. The locked DSH runtime already reuses
    a final /v1, so no version path is stripped or appended here.
    """
    value = (base_url or DEFAULT_MESSAGES_BASE_URL).strip().rstrip("/")
    parts = urlsplit(value)
    if (parts.scheme == "https" and parts.hostname == "api.deepseek.com"
            and parts.port in {None, 443} and parts.path in {"", "/v1"}
            and not parts.username and not parts.password and not parts.query and not parts.fragment):
        return urlunsplit((parts.scheme, parts.netloc, "/anthropic", "", ""))
    return value


def messages_url(base_url: str | None = None) -> str:
    """Mirror dsh-llm-deepseek 0.2.1rc2 messagesApiRoot exactly."""
    base = messages_base_url(base_url)
    return base + ("/messages" if urlsplit(base).path.endswith("/v1") else "/v1/messages")
