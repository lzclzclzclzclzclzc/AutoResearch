import json
import os
import zipfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
import pytest

from fastapi.testclient import TestClient

from apps.api.main import Provider, create_app
from backend.persistence import StoreError
from backend.services.credentials import CredentialService


class FakeControl:
    def __init__(self):
        self.projects = {}

    def status(self, research_id=None):
        if not research_id:
            return {"control_epoch": "unit-test", "projects": self.projects}
        return self.projects.get(research_id, {"state": "unknown", "stopped": True})

    def terminate(self, research_id, reason="user_requested", stop_id=None):
        result = self.projects.setdefault(research_id, {"research_id": research_id, "stop_id": stop_id,
            "reason": reason, "state": "stopped_pending_sync", "stopped": True, "unfinished": [],
            "stop_journal_persisted": True, "db_synced": False})
        return result

    def allows_progress(self, *args):
        return True


def client(tmp_path):
    app = create_app(tmp_path, control_client=FakeControl(), testing=True)
    c = TestClient(app)
    csrf = c.get("/api/session").json()["csrf_token"]
    c.headers.update({"X-CSRF-Token": csrf})
    return c, app


@pytest.fixture
def messages_service():
    routes, received = {}, []
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            payload = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))))
            received.append({"path": self.path, "headers": dict(self.headers), "body": payload})
            status, data = routes.get(self.path, (404, {"error": "Messages endpoint not found"}))
            response = json.dumps(data).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response)

        def log_message(self, *args):
            pass
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", routes, received
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_local_session_and_csrf_reject_foreign_requests(tmp_path):
    app = create_app(tmp_path, control_client=FakeControl(), testing=True)
    c = TestClient(app)
    assert c.get("/api/settings/team").status_code == 401
    c.get("/api/session")
    assert c.post("/api/research", json={"title": "API 测试任务", "topic": "x", "client_request_id": "one"}).status_code == 403
    assert c.get("/api/session", headers={"Origin": "https://evil.example"}).status_code == 403


def test_external_input_retry_creates_one_new_recovery_generation(tmp_path):
    c, app = client(tmp_path)
    result = c.post("/api/research", json={"title": "恢复测试", "topic": "external input",
        "mode": "demo", "client_request_id": "external-project"})
    assert result.status_code == 201, result.text
    rid = result.json()["research_id"]
    store = app.state.store
    p = store.start_project(rid)
    task = store.pending_tasks()[0]
    store.finish_task(task["id"], error={"code": "arbitration_needs_external_input", "message": "missing input"})
    store.update_project(rid, status="blocked", blocked_reason="all_lines_blocked")
    command = {"kind": "retry", "client_request_id": "retry-external"}
    first = c.post(f"/api/research/{rid}/commands", json=command)
    assert first.status_code == 202, first.text
    assert c.post(f"/api/research/{rid}/commands", json=command).json() == first.json()
    task = store.get("task", task["id"])
    assert task["status"] == "pending" and task["recovery_generation"] == 1
    assert store.project(rid)["deadline_at"] == p["deadline_at"]


def test_manual_name_and_history_configuration_are_independent_and_frozen(tmp_path):
    c, app = client(tmp_path)
    body = {"title": "  论文基线对照  ", "topic": "很长的研究指令，与任务名称不同", "description": "约束",
        "client_request_id": "named-task", "mode": "demo", "workspace_path": str(tmp_path),
        "team": {"n": 3, "boss_members": 1}, "max_runtime_seconds": 600,
        "model_concurrency": 2, "experiment_concurrency": 1}
    for invalid in (None, "", "   "):
        invalid_body = {**body, "title": invalid}
        if invalid is None:
            invalid_body.pop("title")
        assert c.post("/api/research", json=invalid_body).status_code == 422
    created = c.post("/api/research", json=body).json()
    assert created["title"] == "论文基线对照" and created["topic"] == body["topic"]
    app.state.store.patch("settings", "team", n=9, model_concurrency=12)
    saved = c.get(f"/api/research/{created['id']}/configuration").json()
    assert saved["team"]["n"] == 3 and saved["model_concurrency"] == 2
    assert saved["max_runtime_seconds"] == 600 and saved["workspace_path"] == str(tmp_path)
    assert saved["description"] == "约束" and saved["mode"] == "demo"
    assert not ({"provider", "credential_ref", "api_key", "credential_version"} & set(saved))
    copied = c.post("/api/research", json={**body, "title": "第二次对照", "client_request_id": "copied-task",
        "team": saved["team"], "max_runtime_seconds": saved["max_runtime_seconds"]}).json()
    assert copied["title"] == "第二次对照" and copied["id"] != created["id"]


def test_delete_restore_records_preserves_history_and_rejects_running_project(tmp_path):
    c, app = client(tmp_path)
    p = c.post("/api/research", json={"title": "可恢复记录", "topic": "prompt", "client_request_id": "delete-test"}).json()
    rid = p["id"]
    url = f"/api/research/{rid}"
    assert c.delete(url, headers={"Idempotency-Key": "too-early"}).status_code == 409
    app.state.store.update_project(rid, status="terminated")
    app.state.store.create_conversation(rid, "kept-conversation", title="kept")
    app.state.store.create_message(rid, "kept-conversation", "boss/member-01", "保留证据", source_system="test", source_event_id="one")
    first = c.delete(url, headers={"Idempotency-Key": "delete-one"})
    assert first.status_code == 200 and first.json()["deleted"]
    assert c.delete(url, headers={"Idempotency-Key": "delete-one"}).json() == first.json()
    assert not c.get("/api/research").json()["items"]
    assert c.get("/api/research?include_deleted=true").json()["items"][0]["deleted_at"]
    assert c.get(url + "/configuration").status_code == 404
    restored = c.post(url + "/restore", json={"client_request_id": "restore-one"})
    assert restored.status_code == 200 and not restored.json()["deleted"]
    assert len(c.get("/api/research").json()["items"]) == 1
    assert app.state.store.messages("kept-conversation")["items"][0]["content_markdown"] == "保留证据"
    types = [e["type"] for e in app.state.store.events(rid)]
    assert types.count("research.record_deleted") == types.count("research.record_restored") == 1
    assert c.get("/api/session", headers={"Host": "evil.example"}).status_code == 403


def test_create_snapshot_timeline_and_frozen_settings(tmp_path):
    c, app = client(tmp_path)
    body = {"title": "科研测试", "client_request_id": "create-1", "topic": "Study a negative result", "mode": "demo", "team": {"n": 2, "m1": 2, "m2": 2, "k": 3, "boss_members": 3}}
    created = c.post("/api/research", json=body)
    assert created.status_code == 201, created.text
    project = created.json()
    rid = project["research_id"]
    assert c.post("/api/research", json=body).json()["research_id"] == rid
    snapshot = c.get(f"/api/research/{rid}/snapshot").json()
    assert snapshot["topology"]["agent_count"] == 17
    assert len(snapshot["agents"]) == 17
    assert snapshot["project"]["mode"] == "demo"
    assert snapshot["snapshot_seq"] == 1
    timeline = c.get(f"/api/research/{rid}/timeline?after=0").json()
    assert timeline["items"][0]["type"] == "research.created"
    team = c.get("/api/settings/team").json()
    assert c.put("/api/settings/team", json={**team, "n": 3, "client_request_id": "team-1"}).status_code == 200
    assert c.get(f"/api/research/{rid}/snapshot").json()["project"]["config_snapshot"]["team"]["n"] == 2
    assert c.put("/api/settings/team", json={**team, "n": 4, "client_request_id": "team-2"}).status_code == 409
    assert c.post("/api/research", json={**body, "topic": "Changed idempotency content"}).status_code == 409


def test_terminate_remains_available_without_business_store(tmp_path):
    c, app = client(tmp_path)
    class UnavailableStore:
        def __getattribute__(self, name):
            raise AssertionError("Business store must never be accessed on control routes")
    app.state.store = UnavailableStore()
    first = c.post("/api/research/anything/terminate", json={"client_request_id": "stop-1"})
    assert first.status_code == 202
    assert first.json()["stopped"] is True
    assert first.json()["db_synced"] is False
    assert c.get("/api/research/anything/control-status").json()["stop_id"] == "stop-1"
    assert c.get("/api/commands/stop-1").json()["stopped"] is True


def test_message_hierarchical_ids_and_evidence_export(tmp_path):
    c, app = client(tmp_path)
    project = c.post("/api/research", json={"title": "API 测试任务", "topic": "Export test", "client_request_id": "export-project"}).json()
    rid = project["research_id"]
    s = app.state.store
    cid = f"{rid}/line-01/exploration/epoch-1"
    s.create_message(rid, cid, "line-01/exploration/member-01", "Actual test message", edge_id="line-01/exploration->line-01/critic",
        recipients=[{"type": "group", "id": "line-01/critic"}])
    response = c.get(f"/api/conversations/{cid}/messages")
    assert response.status_code == 200, response.text
    assert response.json()["items"][0]["content_markdown"] == "Actual test message"
    assert c.get(f"/api/conversations/{cid}/messages", params={"q": "absent search"}).json()["items"] == []
    assert c.get(f"/api/conversations/{cid}/messages", params={"q": "ACTUAL"}).json()["items"]
    assert c.get(f"/api/research/{rid}/conversations", params={"agent_id": "line-01/critic/member-02"}).json()["items"]
    exported = c.post(f"/api/research/{rid}/exports", json={"client_request_id": "export-1"})
    assert exported.status_code == 200, exported.text
    assert c.get(exported.json()["download_url"]).status_code == 200


def test_api_key_never_echoed_on_validation_error(tmp_path):
    c, app = client(tmp_path)
    synthetic = "sk-SENSITIVE-SYNTHETIC-SECRET"
    response = c.put("/api/settings/providers/deepseek-default", json={"api_key": synthetic, "base_url": "bad url", "client_request_id": "bad-key"})
    assert response.status_code == 422
    assert synthetic not in response.text


@pytest.mark.parametrize("saved_base", ["https://api.deepseek.com", "https://api.deepseek.com/v1", "https://api.deepseek.com/anthropic/"])
def test_provider_save_normalizes_official_messages_endpoint_idempotently(tmp_path, saved_base):
    c, app = client(tmp_path)
    current = c.get("/api/settings/providers").json()["items"][0]
    body = {"version": current["version"], "base_url": saved_base, "client_request_id": "normalize-messages"}
    saved = c.put("/api/settings/providers/deepseek-default", json=body)
    assert saved.status_code == 200, saved.text
    assert saved.json()["base_url"] == "https://api.deepseek.com/anthropic"
    assert Provider().base_url == "https://api.deepseek.com/anthropic"
    assert c.put("/api/settings/providers/deepseek-default", json=body).json() == saved.json()


def test_provider_get_does_not_rewrite_legacy_endpoint_or_version(tmp_path):
    c, app = client(tmp_path)
    c.get("/api/settings/providers")
    saved = app.state.store.patch("provider", "deepseek-default", base_url="https://api.deepseek.com")
    public = c.get("/api/settings/providers").json()["items"][0]
    assert public["version"] == saved["version"]
    assert public["base_url"] == "https://api.deepseek.com"


@pytest.mark.parametrize("base_suffix,expected_path", [
    ("/anthropic", "/anthropic/v1/messages"),
    ("/anthropic/v1", "/anthropic/v1/messages"),
    ("/proxy/v1/", "/proxy/v1/messages"),
])
def test_provider_test_uses_real_messages_request_and_redacts_response(tmp_path, monkeypatch, messages_service, base_suffix, expected_path):
    base, routes, received = messages_service
    synthetic = "sk-LOCAL-MESSAGES-FIXTURE-0123456789"
    monkeypatch.setenv("DEEPSEEK_API_KEY", synthetic)
    # Neither response content nor server-controlled model metadata may echo a key.
    routes[expected_path] = (200, {"id": "msg-local", "type": "message", "role": "assistant", "model": synthetic,
        "content": [{"type": "text", "text": "OK " + synthetic}], "stop_reason": "end_turn"})
    c, app = client(tmp_path)
    version = c.get("/api/settings/providers").json()["items"][0]["version"]
    saved = c.put("/api/settings/providers/deepseek-default", json={"version": version, "base_url": base + base_suffix,
        "client_request_id": "save-local-messages"})
    assert saved.status_code == 200, saved.text
    request = {"client_request_id": "test-local-messages"}
    tested = c.post("/api/settings/providers/deepseek-default/test", json=request)
    assert tested.status_code == 200, tested.text
    result = tested.json()
    assert result["test_kind"] == "messages_model_call"
    assert result["credential_valid"] is True and result["model_available"] is True
    assert result["streaming_contract_verified"] is result["tools_contract_verified"] is False
    assert len(received) == 1 and received[0]["path"] == expected_path
    headers = {key.lower(): value for key, value in received[0]["headers"].items()}
    assert headers["x-api-key"] == synthetic and headers["anthropic-version"] == "2023-06-01"
    assert "authorization" not in headers and headers["content-type"] == "application/json"
    assert received[0]["body"] == {"model": "deepseek-flash", "messages": [{"role": "user", "content": "Reply OK."}],
        "max_tokens": 4096, "stream": False, "thinking": {"type": "disabled"}}
    assert c.post("/api/settings/providers/deepseek-default/test", json=request).json() == result
    assert len(received) == 1
    assert app.state.store.get("provider", "deepseek-default")["test_status"] == "passed"
    assert synthetic not in tested.text
    assert synthetic not in c.get("/api/settings/providers").text
    for path in tmp_path.glob("business.sqlite*"):
        assert synthetic.encode() not in path.read_bytes()


def test_chat_success_cannot_hide_missing_messages_endpoint(tmp_path, monkeypatch, messages_service):
    base, routes, received = messages_service
    synthetic = "sk-LOCAL-NOT-FOR-REMOTE-404"
    monkeypatch.setenv("DEEPSEEK_API_KEY", synthetic)
    routes["/chat/completions"] = (200, {"choices": [{"message": {"content": "OK"}}]})
    routes["/anthropic/v1/messages"] = (404, {"error": {"message": "PRIVATE_ERROR_BODY " + synthetic}})
    assert httpx.post(base + "/chat/completions", json={}, timeout=5, trust_env=False).status_code == 200
    received.clear()
    c, app = client(tmp_path)
    version = c.get("/api/settings/providers").json()["items"][0]["version"]
    assert c.put("/api/settings/providers/deepseek-default", json={"version": version, "base_url": base + "/anthropic",
        "client_request_id": "save-missing-messages"}).status_code == 200
    response = c.post("/api/settings/providers/deepseek-default/test", json={"client_request_id": "missing-messages"})
    assert response.status_code == 200, response.text
    assert response.json()["model_available"] is False and response.json()["http_status"] == 404
    assert "Messages" in response.json()["message"] and "/anthropic" in response.json()["message"]
    assert [request["path"] for request in received] == ["/anthropic/v1/messages"]
    assert app.state.store.get("provider", "deepseek-default")["test_status"] == "failed"
    assert synthetic not in response.text and "PRIVATE_ERROR_BODY" not in response.text
    assert synthetic not in c.get("/api/settings/providers").text
    for path in tmp_path.glob("business.sqlite*"):
        assert synthetic.encode() not in path.read_bytes()


@pytest.mark.parametrize("invalid", [
    {"choices": [{"message": {"content": "OK"}}]},
    {"type": "message", "content": [{"type": "tool_use", "name": "echo"}]},
    {"type": "message", "content": [{"type": "text", "text": "  "}]},
    {"type": "other", "content": [{"type": "text", "text": "OK"}]},
    {"type": "message", "content": [{"type": "text", "text": ""}], "stop_reason": "max_tokens"},
])
def test_provider_test_rejects_non_messages_success_body(tmp_path, monkeypatch, messages_service, invalid):
    base, routes, received = messages_service
    monkeypatch.setenv("DEEPSEEK_API_KEY", "sk-LOCAL-INVALID-CONTRACT")
    routes["/v1/messages"] = (200, invalid)
    c, app = client(tmp_path)
    version = c.get("/api/settings/providers").json()["items"][0]["version"]
    assert c.put("/api/settings/providers/deepseek-default", json={"version": version, "base_url": base,
        "client_request_id": "save-invalid-contract"}).status_code == 200
    tested = c.post("/api/settings/providers/deepseek-default/test", json={"client_request_id": "invalid-contract"})
    assert tested.status_code == 200, tested.text
    assert tested.json()["model_available"] is False
    if invalid.get("stop_reason") == "max_tokens":
        assert tested.json()["error_code"] == "model_response_truncated"
        assert "token 上限" in tested.json()["message"]
        assert "响应格式无效" not in tested.json()["message"]
    else:
        assert "响应格式无效" in tested.json()["message"]
    assert app.state.store.get("provider", "deepseek-default")["test_status"] == "failed"


@pytest.mark.skipif(os.name != "nt", reason="Windows DPAPI requires the current Windows user")
def test_dpapi_storage_version_rebind_and_export_never_expose_key(tmp_path):
    c, app = client(tmp_path)
    first_key, second_key = "sk-SYNTHETIC-FIRST-0123456789", "sk-SYNTHETIC-SECOND-9876543210"
    first = c.put("/api/settings/providers/deepseek-default", json={"api_key": first_key,
        "version": 1, "client_request_id": "secret-first"})
    assert first.status_code == 200, first.text
    metadata = first.json()
    assert metadata["configured"] is True
    assert first_key not in first.text
    project = c.post("/api/research", json={"title": "API 测试任务", "topic": "Credential isolation", "client_request_id": "secret-project"}).json()
    rid = project["research_id"]
    second = c.put("/api/settings/providers/deepseek-default", json={"api_key": second_key,
        "version": metadata["version"], "client_request_id": "secret-second"})
    assert second.status_code == 200, second.text
    service = CredentialService(tmp_path, app.state.store)
    assert service.get_secret("deepseek-default") == second_key
    with pytest.raises(StoreError, match="显式重新绑定"):
        service.get_secret("deepseek-default", version=project["credential_version"])
    rebound = c.post(f"/api/research/{rid}/commands", json={"kind": "rebind_credential", "client_request_id": "secret-rebind"})
    assert rebound.status_code == 202, rebound.text
    assert app.state.store.project(rid)["credential_version"] == second.json()["credential_version"]
    exported = c.post(f"/api/research/{rid}/exports", json={"client_request_id": "secret-export"}).json()
    _, path = app.state.store.artifact(exported["artifact_id"])
    with zipfile.ZipFile(path) as archive:
        contents = b"".join(archive.read(name) for name in archive.namelist())
    for key in (first_key, second_key):
        assert key.encode() not in contents
        for file in tmp_path.glob("business.sqlite*"):
            assert key.encode() not in file.read_bytes()
        for file in (tmp_path / "credentials").glob("*.dpapi"):
            assert key.encode() not in file.read_bytes()


def test_terminate_is_not_delayed_by_a_business_command_waiting_on_write_lock(tmp_path, monkeypatch):
    c, app = client(tmp_path)
    rid = c.post("/api/research", json={"title": "API 测试任务", "topic": "Locked DB", "client_request_id": "locked-project"}).json()["research_id"]
    s = app.state.store
    entered = threading.Event()
    original = s.update_project
    def waiting_writer(*args, **kwargs):
        entered.set()
        return original(*args, **kwargs)
    monkeypatch.setattr(s, "update_project", waiting_writer)
    with ThreadPoolExecutor(max_workers=1) as executor:
        with s.connect(True):
            blocked = executor.submit(c.post, f"/api/research/{rid}/commands", json={"kind": "pause", "client_request_id": "locked-pause"})
            assert entered.wait(1), "Pause request must have reached its blocked database writer"
            started = time.monotonic()
            stop = c.post(f"/api/research/{rid}/terminate", json={"client_request_id": "locked-stop"})
            elapsed = time.monotonic() - started
            assert stop.status_code == 202
            assert stop.json()["stopped"] is True
            assert elapsed < 1, f"Stop incorrectly waited {elapsed:.2f}s for the business write lock"
        assert blocked.result(timeout=5).status_code == 202


def test_history_message_and_edge_endpoints_reconstruct_stream_prefix_at_cursor(tmp_path):
    c, app = client(tmp_path)
    rid = c.post("/api/research", json={"title": "API 测试任务", "topic": "Stream history", "client_request_id": "stream-history-project"}).json()["research_id"]
    s = app.state.store
    conversation, edge = f"{rid}/line-01/chat", "line-01/exploration->line-01/critic"
    partial = s.upsert_stream_message(rid, "api-history-stream", conversation, "line-01/exploration/member-01", "实际前缀",
        source_event_id="api-stream-source", chunk_id="1", edge_id=edge)
    s.complete_stream_message("api-history-stream", "仅未来可见的最终结果", structured={"future": True})
    for number in range(50):
        s.create_message(rid, conversation, "agent-1", f"Future {number}", edge_id=edge)
    params = {"at_seq": partial["latest_event_seq"], "limit": 1}
    messages = c.get(f"/api/conversations/{conversation}/messages", params=params)
    edges = c.get(f"/api/research/{rid}/edges/{edge}/messages", params=params)
    assert messages.status_code == edges.status_code == 200
    assert messages.json()["items"] == edges.json()["items"]
    historic = messages.json()["items"][0]
    assert historic["content_markdown"] == "实际前缀"
    assert historic["status"] == "streaming"
    assert "structured" not in historic


def test_reference_lookup_finds_older_messages_and_respects_replay_and_project(tmp_path):
    c, app = client(tmp_path)
    rid = c.post("/api/research", json={"title": "API 测试任务", "topic": "Reference", "client_request_id": "reference-project"}).json()["research_id"]
    other = c.post("/api/research", json={"title": "API 测试任务", "topic": "Other", "client_request_id": "reference-other"}).json()["research_id"]
    s = app.state.store
    cid = f"{rid}/chat"
    target = s.upsert_stream_message(rid, "reference-stream", cid, "agent-1", "原始前缀", source_event_id="reference-turn", chunk_id="1")
    cutoff = target["latest_event_seq"]
    s.complete_stream_message("reference-stream", "未来全文")
    for number in range(55):
        s.create_message(rid, cid, "agent-1", f"Later {number}")
    path = f"/api/research/{rid}/messages/reference-stream"
    assert c.get(path).json()["content_markdown"] == "未来全文"
    assert c.get(path, params={"at_seq": cutoff}).json()["content_markdown"] == "原始前缀"
    assert c.get(path, params={"at_seq": 1}).status_code == 404
    assert c.get(f"/api/research/{other}/messages/reference-stream").status_code == 404


@pytest.mark.parametrize("name,media_type", [("artifact.html", "text/html"), ("artifact.svg", "image/svg+xml"), ("artifact.txt", "application/javascript")])
def test_active_artifacts_are_sandboxed_attachments(tmp_path, name, media_type):
    c, app = client(tmp_path)
    rid = c.post("/api/research", json={"title": "API 测试任务", "topic": "Artifact isolation", "client_request_id": "artifact-isolation"}).json()["research_id"]
    source = tmp_path / "artifacts" / name
    source.write_text("<script>fetch('/api/session')</script>", encoding="utf-8")
    artifact = app.state.store.register_artifact(rid, source, media_type)
    response = c.get(f"/api/artifacts/{artifact['artifact_id']}")
    assert response.status_code == 200
    assert response.headers["content-disposition"].startswith("attachment;")
    csp = response.headers["content-security-policy"]
    assert "sandbox" in csp and "default-src 'none'" in csp
    assert "allow-scripts" not in csp and "allow-same-origin" not in csp
    assert response.headers["x-content-type-options"] == "nosniff"
    safe = tmp_path / "artifacts" / "notes.md"
    safe.write_text("# Safe text preview", encoding="utf-8")
    safe_artifact = app.state.store.register_artifact(rid, safe, "text/markdown")
    preview = c.get(f"/api/artifacts/{safe_artifact['artifact_id']}")
    assert preview.status_code == 200
    assert not preview.headers.get("content-disposition", "").startswith("attachment")
    assert "sandbox" in preview.headers["content-security-policy"]
