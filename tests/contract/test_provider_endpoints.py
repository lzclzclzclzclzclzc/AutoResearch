import pytest

from backend.adapters.provider_endpoints import messages_base_url, messages_url


@pytest.mark.parametrize("saved", [None, "https://api.deepseek.com", "https://api.deepseek.com/", "https://api.deepseek.com/v1/"])
def test_saved_official_chat_roots_resolve_to_messages_api(saved):
    assert messages_base_url(saved) == "https://api.deepseek.com/anthropic"
    assert messages_url(saved) == "https://api.deepseek.com/anthropic/v1/messages"


@pytest.mark.parametrize("base,expected", [
    ("https://api.deepseek.com/anthropic/", "https://api.deepseek.com/anthropic/v1/messages"),
    ("https://api.deepseek.com/anthropic/v1", "https://api.deepseek.com/anthropic/v1/messages"),
    ("http://127.0.0.1:8080", "http://127.0.0.1:8080/v1/messages"),
    ("http://127.0.0.1:8080/proxy/v1/", "http://127.0.0.1:8080/proxy/v1/messages"),
    ("https://proxy.example/api", "https://proxy.example/api/v1/messages"),
    ("https://api.deepseek.com.proxy.example", "https://api.deepseek.com.proxy.example/v1/messages"),
])
def test_custom_prefixes_and_explicit_versions_are_not_rewritten(base, expected):
    assert messages_url(base) == expected
