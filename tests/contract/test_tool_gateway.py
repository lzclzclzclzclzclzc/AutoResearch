import threading

import pytest

from backend.adapters.gateway import ToolGateway
from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor


@pytest.fixture
def gateway(tmp_path):
    store = Store(tmp_path)
    project = store.create_project({"client_request_id": "tools", "topic": "tools", "mode": "demo"})
    project = store.start_project(project["id"])
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(project["id"], project["deadline_at"])
    # Policy tests call the same handler directly; no network server is needed.
    gateway = ToolGateway.__new__(ToolGateway)
    gateway.store, gateway.control, gateway.runner = store, control, None
    gateway.lock, gateway.capabilities = threading.RLock(), {}
    gateway.project = project
    yield gateway
    control.close()


def capability(gateway, role="verify", line_id="line-01"):
    return gateway.register(research_id=gateway.project["id"], role=role, agent_id=f"{line_id}/{role}",
        line_id=line_id, control_generation=1, task_id="task", execution_id="execution", conversation_id="conversation")


def test_evidence_reads_respect_project_line_and_integrity(gateway, tmp_path):
    path = tmp_path / "evidence.json"
    path.write_text('{"mse":0.2}', encoding="utf-8")
    artifact = gateway.store.register_artifact(gateway.project["id"], path, "application/json", line_id="line-01")
    token = capability(gateway)
    result = gateway.call(token, {"name": "read_research_evidence", "arguments": {"artifact_id": artifact["id"]}})
    assert '"mse":0.2' in result["text"]
    with pytest.raises(PermissionError):
        gateway.call(capability(gateway, line_id="line-02"), {"name": "read_research_evidence", "arguments": {"artifact_id": artifact["id"]}})
    _, backing = gateway.store.artifact(artifact["id"])
    backing.write_text("tampered", encoding="utf-8")
    with pytest.raises(PermissionError, match="integrity"):
        gateway.call(token, {"name": "read_research_evidence", "arguments": {"artifact_id": artifact["id"]}})


def test_role_and_stop_gates_cannot_be_bypassed(gateway):
    token = capability(gateway, role="boss")
    with pytest.raises(PermissionError):
        gateway.call(token, {"name": "run_light_cpu", "arguments": {"code": "result=1"}})
    token = capability(gateway, role="exploration")
    with pytest.raises(PermissionError):
        gateway.call(token, {"name": "send_research_message", "call_id": "call", "arguments": {"recipient_ids": ["line-02/critic"], "content_markdown": "cross-line"}})
    gateway.control.terminate(gateway.project["id"])
    with pytest.raises(PermissionError):
        gateway.call(token, {"name": "read_shared_memory", "arguments": {}})


def test_light_cpu_http_rejection_explains_code_rule_instead_of_claiming_no_permission(gateway):
    import httpx
    server = ToolGateway(gateway.store, gateway.control, None)
    server.project = gateway.project
    token = capability(server, role="exploration")
    try:
        response = httpx.post(server.url, headers={"Authorization": "Bearer " + token}, json={
            "name": "run_light_cpu", "arguments": {"code": "result ="}})
        assert response.status_code == 422
        assert "轻量计算 Python 语法错误" in response.json()["message"]
        assert "支持普通 Python" in response.json()["message"]
        assert not response.json()["retryable"]
    finally:
        server.close()


def test_control_transport_loss_is_retryable_http_but_stop_is_not(gateway, monkeypatch):
    import httpx
    from backend.runtime.control import ControlUnavailable
    server = ToolGateway(gateway.store, gateway.control, None)
    server.project = gateway.project
    token = capability(server, role="exploration")
    payload = {"name": "record_stream_event", "arguments": {"stream_id": 1, "cursor": 1,
        "chunk": {"type": "text-delta", "text": "hello"}}}
    def unreachable(*args):
        raise ControlUnavailable("one lost Control response")
    try:
        monkeypatch.setattr(server.control, "allows_progress", unreachable)
        response = httpx.post(server.url, json=payload, headers={"Authorization": "Bearer " + token})
        assert response.status_code == 503 and response.json()["retryable"]
        monkeypatch.setattr(server.control, "allows_progress", lambda *args: False)
        response = httpx.post(server.url, json=payload, headers={"Authorization": "Bearer " + token})
        assert response.status_code == 403 and not response.json()["retryable"]
    finally:
        server.close()


def test_stream_cursor_replay_never_duplicates_the_cumulative_buffer(gateway):
    token = capability(gateway, role="exploration")
    gateway.store.create_conversation(gateway.project["id"], "conversation", title="Stream cursor fixture")
    first = {"name": "record_stream_event", "arguments": {"stream_id": 1, "cursor": 1,
        "chunk": {"type": "text-delta", "text": '{"content_markdown":"hello '}}}
    receipt = gateway.call(token, first)
    assert gateway.call(token, first) == receipt
    gateway.call(token, {"name": "record_stream_event", "arguments": {"stream_id": 1, "cursor": 2,
        "chunk": {"type": "text-delta", "text": 'world"}'}}})
    assert gateway.store.messages("conversation")["items"][0]["content_markdown"] == "hello world"
    assert not any(event["type"] == "agent.output_delta" for event in gateway.store.events(gateway.project["id"]))
    with pytest.raises(ValueError, match="different content"):
        gateway.call(token, {"name": "record_stream_event", "arguments": {"stream_id": 1, "cursor": 1,
            "chunk": {"type": "text-delta", "text": "changed"}}})


def test_private_scheduling_cannot_publish_chat_or_call_research_tools(gateway):
    token = capability(gateway, role="boss")
    gateway.capabilities[token]["suppress_chat"] = True
    before = gateway.store.events(gateway.project["id"])
    gateway.call(token, {"name": "record_stream_event", "arguments": {"stream_id": 1, "cursor": 1,
        "chunk": {"type": "text-delta", "text": '{"content_markdown":"must remain private","score":99}'}}})
    assert not gateway.store.messages("conversation")["items"]
    assert gateway.last_message(token) is None
    assert gateway.store.events(gateway.project["id"]) == before
    with pytest.raises(PermissionError, match="Private scheduling"):
        gateway.call(token, {"name": "send_research_message", "call_id": "private",
            "arguments": {"recipient_ids": ["boss"], "content_markdown": "unwanted score"}})


def test_god_reads_only_its_frozen_snapshot_with_explicit_pagination(gateway):
    from packages.contracts.research import digest
    rid = gateway.project["id"]
    sections = {"discussion": "原始讨论" * 6000, "candidates": {"a": {"method": "one"}}}
    snapshot_hash = digest(sections)
    gateway.store.put("arbitration_snapshot", "snapshot", {"sections": sections, "snapshot_hash": snapshot_hash}, rid)
    gateway.store.put("arbitration_request", "arb", {"snapshot_id": "snapshot", "snapshot_hash": snapshot_hash}, rid)
    token = gateway.register(research_id=rid, role="god", agent_id="god/arb", control_generation=1,
        execution_id="god-run", task_id="arb:judge", conversation_id=rid + ":god:arb",
        arbitration_id="arb", snapshot_hash=snapshot_hash)
    first = gateway.call(token, {"name": "read_arbitration_context", "arguments": {"section": "discussion", "offset": 0}})
    second = gateway.call(token, {"name": "read_arbitration_context", "arguments": {"section": "discussion", "offset": first["next_offset"]}})
    assert first["text"] + second["text"] == sections["discussion"]
    assert first["sha256"] == second["sha256"] and first["total_length"] == len(sections["discussion"])
    for name in ("read_shared_memory", "read_research_evidence", "send_research_message", "run_light_cpu", "read_research_input"):
        with pytest.raises(PermissionError, match="only read"):
            gateway.call(token, {"name": name, "arguments": {}})
    with pytest.raises(PermissionError):
        gateway.call(token, {"name": "read_arbitration_context", "arguments": {"section": "other-project"}})
    gateway.capabilities[token]["snapshot_hash"] = "tampered"
    with pytest.raises(PermissionError, match="binding mismatch"):
        gateway.call(token, {"name": "read_arbitration_context", "arguments": {"section": "discussion"}})
    with pytest.raises(PermissionError, match="Only God"):
        gateway.call(capability(gateway, role="boss"), {"name": "read_arbitration_context", "arguments": {"section": "discussion"}})


def test_gateway_busy_response_preserves_cursor_and_hides_database_exception(gateway, monkeypatch):
    import json
    import urllib.request
    import urllib.error
    from backend.persistence.store import StoreError
    server = ToolGateway(gateway.store, gateway.control, None)
    token = server.register(research_id=gateway.project["id"], role="exploration", agent_id="line-01/exploration",
        control_generation=1, execution_id="busy-stream", task_id="stream", conversation_id="busy-conversation")
    original = gateway.store.emit
    attempts = []
    def emit(*args, **kwargs):
        if not attempts:
            attempts.append(1)
            raise StoreError("storage_busy", "PRIVATE_DATABASE_DETAILS", 503)
        return original(*args, **kwargs)
    monkeypatch.setattr(gateway.store, "emit", emit)
    body = json.dumps({"name": "record_stream_event", "arguments": {"stream_id": 1, "cursor": 1,
        "chunk": {"type": "usage", "output_tokens": 1}}}).encode()
    request = urllib.request.Request(server.url, body, headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"})
    try:
        with pytest.raises(urllib.error.HTTPError) as failure:
            urllib.request.urlopen(request, timeout=5)
        response = failure.value
        value = response.read().decode()
        assert response.code == 503 and json.loads(value)["code"] == "storage_busy"
        assert json.loads(value)["retryable"] and "PRIVATE" not in value
        with urllib.request.urlopen(request, timeout=5) as result:
            accepted = json.load(result)
        with urllib.request.urlopen(request, timeout=5) as result:
            assert json.load(result) == accepted
        assert len([e for e in gateway.store.events(gateway.project["id"]) if e["type"] == "agent.stream_event"]) == 1
    finally:
        server.close()
