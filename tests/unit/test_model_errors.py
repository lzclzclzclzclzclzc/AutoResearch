import io
import json

import pytest

from backend.adapters.model_errors import classify_model_failure
from backend.adapters.sdk_child import emit


def turn_error(code, status=None, **extra):
    return {"type": "turn/end", "data": {"reason": {"kind": "error", "error": {
        "code": code, "status": status, "message": "RAW_PROVIDER_MESSAGE_DUMMY_SECRET", **extra}}}}


@pytest.mark.parametrize("provider,status,expected", [
    ("HTTP_404", 404, "model_endpoint_error"),
    ("HTTP_404", None, "model_endpoint_error"),
    ("INVALID_REQUEST", 400, "model_request_invalid"),
    ("INVALID_REQUEST", 413, "model_request_invalid"),
    ("CONTEXT_WINDOW_EXCEEDED", None, "model_request_invalid"),
    ("QUOTA", 402, "model_quota_exceeded"),
    ("QUOTA", None, "model_quota_exceeded"),
    ("AUTH", 401, "credential_invalid"),
    ("AUTH", 403, "credential_invalid"),
    ("AUTH", None, "credential_invalid"),
    ("INVALID_CREDENTIAL", None, "credential_invalid"),
    ("MISSING_CREDENTIAL", None, "credential_invalid"),
    ("RATE_LIMIT", 429, "rate_limited"),
    ("RATE_LIMIT", None, "rate_limited"),
    ("SERVER", 503, "model_transport_error"),
    ("SERVER", None, "model_transport_error"),
    ("HTTP_500", 500, "model_transport_error"),
    ("TRANSPORT", None, "model_transport_error"),
    ("TIMEOUT", None, "model_timeout"),
    ("MALFORMED_RESPONSE", None, "model_response_invalid"),
    ("EMPTY_RESPONSE", None, "model_response_invalid"),
    ("INVALID_RESPONSE", None, "model_response_invalid"),
    ("STREAM_CLOSED", None, "model_response_invalid"),
    ("ABORTED", None, "model_execution_failed"),
    ("HTTP_418", 418, "model_execution_failed"),
])
def test_official_turn_failure_has_precise_safe_classification(provider, status, expected):
    result = classify_model_failure([turn_error(provider, status, headers={"Authorization": "DUMMY_SECRET"}, prompt="DUMMY_PROMPT")])
    assert result["code"] == expected
    assert result["provider_error_code"] == provider
    assert set(result) == {"code", "message", "http_status", "provider_error_code"}
    assert "DUMMY" not in json.dumps(result)
    assert len(json.dumps(result, ensure_ascii=False).encode("utf-8")) < 512


def test_official_finish_reason_failure_without_http_status_is_retryable():
    result = classify_model_failure([{"type": "finish", "reason": {"kind": "error", "failure": {
        "code": "TRANSPORT", "message": "RAW_PROVIDER_MESSAGE", "headers": {"Authorization": "DUMMY_SECRET"}}}}])
    assert result["code"] == "model_transport_error" and result["http_status"] is None
    assert result["provider_error_code"] == "TRANSPORT" and "RAW_PROVIDER" not in result["message"]


def test_only_the_final_root_failure_controls_classification():
    events = [turn_error("AUTH", 401), {"type": "tool/result", "data": {"status": 429}}, turn_error("HTTP_404", 404)]
    assert classify_model_failure(events)["code"] == "model_endpoint_error"
    completed = {"type": "turn/end", "data": {"reason": {"kind": "completed"}}}
    assert classify_model_failure([*events, completed])["code"] == "model_execution_failed"
    assert classify_model_failure([{"type": "tool/result", "data": {"status": 401}}])["http_status"] is None


@pytest.mark.parametrize("events", [None, {}, "RAW_SECRET", [None, {}, {"type": "turn/end", "data": []}],
    [turn_error("SECRET" * 10000, True)], [turn_error("HTTP_999", "404")]])
def test_malformed_and_untrusted_failure_values_cannot_escape(events):
    result = classify_model_failure(events)
    assert result["code"] == "model_execution_failed"
    assert result["http_status"] is None and result["provider_error_code"] is None
    assert "SECRET" not in json.dumps(result) and len(json.dumps(result)) < 1000


def test_child_json_keeps_chinese_when_sdk_changes_stdout_encoding(monkeypatch):
    wire = io.BytesIO()
    stream = io.TextIOWrapper(wire, encoding="ascii", errors="strict", write_through=True)
    monkeypatch.setattr("sys.stdout", stream)
    emit({"type": "error", "message": "模型接口地址不存在（HTTP 404）"})
    encoded = wire.getvalue()
    assert encoded.isascii()
    assert json.loads(encoded)["message"] == "模型接口地址不存在（HTTP 404）"


@pytest.mark.parametrize("message,code", [
    ("AUTORESEARCH_STREAM_RECORDER_UNAVAILABLE", "stream_recorder_unavailable"),
    ("Research stream recorder is unavailable or stopped", "stream_recorder_unavailable"),
    ("AUTORESEARCH_STREAM_RECORDER_STOPPED", "stream_recorder_stopped"),
    ("AUTORESEARCH_STREAM_RECORDER_REJECTED", "stream_recorder_rejected"),
])
def test_local_recorder_failures_are_not_misclassified_as_provider_errors(message, code):
    result = classify_model_failure([turn_error("UNKNOWN", message=message)])
    assert result["code"] == code
    assert result["http_status"] is None
