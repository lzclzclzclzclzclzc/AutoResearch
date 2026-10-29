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
