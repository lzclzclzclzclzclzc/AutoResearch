"""Bounded diagnostics from the locked Harness failure contract.

Only fixed messages and allowlisted provider facts cross this boundary. Provider
messages, request bodies, prompts, headers and exception strings never do.
"""
from __future__ import annotations

import re
from collections.abc import Sequence


_PROVIDER_CODES = frozenset({
    "AUTH", "QUOTA", "RATE_LIMIT", "CONTEXT_WINDOW_EXCEEDED", "INVALID_REQUEST", "SERVER",
    "TRANSPORT", "TIMEOUT", "ABORTED", "STREAM_CLOSED", "MALFORMED_RESPONSE", "EMPTY_RESPONSE",
    "MISSING_CREDENTIAL", "INVALID_CREDENTIAL", "INVALID_RESPONSE", "INVALID_REPLAY_STATE",
    "UNSUPPORTED_CONTENT", "UNSUPPORTED_REASONING_EFFORT", "REQUEST_EXTENSION", "NO_ADAPTER", "UNKNOWN",
})


def _failure(events):
    if not isinstance(events, Sequence) or isinstance(events, (str, bytes, bytearray)):
        return {}
    for event in reversed(events):
        if not isinstance(event, dict):
            continue
        if event.get("type") == "turn/end":
            data = event.get("data")
            reason = data.get("reason") if isinstance(data, dict) else None
            # The final root turn boundary is authoritative. Never classify an
            # earlier failed attempt or an unrelated tool result as this failure.
            if not isinstance(reason, dict) or reason.get("kind") != "error":
                return {}
            error = reason.get("error")
            return error if isinstance(error, dict) else {}
    for event in reversed(events):
        if not isinstance(event, dict) or event.get("type") != "finish":
            continue
        reason = event.get("reason")
        if not isinstance(reason, dict) or reason.get("kind") != "error":
            return {}
        failure = reason.get("failure")
        return failure if isinstance(failure, dict) else {}
    return {}


def classify_model_failure(events) -> dict:
    """Return exactly four safe fields from official turn/end or finish events."""
    failure = _failure(events)
    raw_code, raw_status = failure.get("code"), failure.get("status")
    provider_code = raw_code if isinstance(raw_code, str) and raw_code in _PROVIDER_CODES else None
    http_code = re.fullmatch(r"HTTP_([1-5][0-9]{2})", raw_code) if isinstance(raw_code, str) else None
    if http_code:
        provider_code = raw_code
    status = raw_status if isinstance(raw_status, int) and not isinstance(raw_status, bool) and 100 <= raw_status <= 599 else None
    if status is None and http_code:
        status = int(http_code.group(1))

    if status == 404:
        code, message = "model_endpoint_error", "模型接口地址不存在（HTTP 404），请检查模型服务地址和接口路径。"
    elif status in {400, 413} or provider_code in {"INVALID_REQUEST", "CONTEXT_WINDOW_EXCEEDED", "INVALID_REPLAY_STATE", "UNSUPPORTED_CONTENT", "UNSUPPORTED_REASONING_EFFORT", "REQUEST_EXTENSION", "NO_ADAPTER"}:
        code, message = "model_request_invalid", "模型请求或配置无效，请检查模型、接口协议和请求设置。"
    elif status == 402 or provider_code == "QUOTA":
        code, message = "model_quota_exceeded", "模型服务额度不足，请检查账户余额和可用额度。"
    elif status in {401, 403} or provider_code in {"AUTH", "MISSING_CREDENTIAL", "INVALID_CREDENTIAL"}:
        code, message = "credential_invalid", "模型凭据无效或无访问权限，请检查设置并显式重新绑定凭据。"
    elif status == 429 or provider_code == "RATE_LIMIT":
        code, message = "rate_limited", "模型服务请求受限，将在研究截止时间内等待重试。"
    elif provider_code == "TIMEOUT":
        code, message = "model_timeout", "模型请求超时，将在研究截止时间内重试。"
    elif status is not None and status >= 500 or provider_code in {"TRANSPORT", "SERVER"}:
        code, message = "model_transport_error", "模型服务暂时不可用或连接失败，将在研究截止时间内重试。"
    else:
        code, message = "model_execution_failed", "模型回合未正常完成，请检查已保存的运行事件。"
    return {"code": code, "message": message, "http_status": status, "provider_error_code": provider_code}
