from fastapi.testclient import TestClient
import threading
from types import SimpleNamespace

from apps.api.main import create_app
from backend.adapters.gateway import ToolGateway


def setup(tmp_path):
    app = create_app(tmp_path, control_client=object(), testing=True)
    client = TestClient(app)
    token = client.get("/api/session").json()["csrf_token"]
    client.headers["X-CSRF-Token"] = token
    project = client.post("/api/research", json={"title": "仲裁回放", "topic": "test",
        "mode": "demo", "client_request_id": "arbitration-test"}).json()
    return client, app.state.store, project["id"]


def seed(store, rid, aid="arb-a"):
    request = {"arbitration_id": aid, "scope_kind": "discussion", "scope_id": "boss",
        "reason_code": "decision_blocked", "snapshot_id": aid + ":snapshot", "snapshot_hash": "hash",
        "source_task_id": "boss-task", "status": "applied", "created_at": "2026-10-29T00:00:00Z",
        "decision_id": aid + ":decision", "decision_hash": "decision-hash", "effect_id": aid + ":effect"}
    store.put("arbitration_request", aid, request, rid)
    store.put("arbitration_snapshot", aid + ":snapshot", {"sections": {"discussion": [{"message_id": "m", "text": "facts"}]}}, rid)
    store.put("arbitration_decision", aid + ":decision", {"arbitration_id": aid, "decision_hash": "decision-hash",
        "decision": {"action": "select_candidate", "selected_candidate_id": "boss/member-01", "content_markdown": "future decision"},
        "capability": "must-never-return", "source_execution_id": "god-execution"}, rid)
    store.put("arbitration_effect", aid + ":effect", {"arbitration_id": aid, "decision_hash": "decision-hash",
        "payload": {"winner": "boss/member-01", "api_key": "must-never-return"}}, rid)
    refs = {"arbitration_id": aid, "scope_id": "boss"}
    requested = store.emit(rid, "arbitration.requested", refs)
    started = store.emit(rid, "arbitration.started", {**refs, "attempt": 1, "agent_id": "god/" + aid})
    decided = store.emit(rid, "arbitration.decided", {**refs, "decision_id": aid + ":decision", "decision_hash": "decision-hash"})
    applied = store.emit(rid, "arbitration.applied", {**refs, "effect_id": aid + ":effect", "decision_hash": "decision-hash", "applied_action": "select_candidate"})
    store.patch("arbitration_request", aid, requested_event_seq=requested["seq"], decided_event_seq=decided["seq"], applied_event_seq=applied["seq"])
    return requested["seq"], started["seq"], decided["seq"], applied["seq"]


def test_arbitration_read_views_do_not_expose_future_decisions_or_effects(tmp_path):
    client, store, rid = setup(tmp_path)
    requested, started, decided, applied = seed(store, rid)
    url = f"/api/research/{rid}/arbitrations/arb-a"
    assert client.get(url + f"?at_seq={requested - 1}").status_code == 404
    running = client.get(url + f"?at_seq={started}").json()
    assert running["status"] == "running"
    assert "decision" not in running and "effects" not in running
    assert running["attempts"][0]["agent_id"] == "god/arb-a"
    gateway = ToolGateway.__new__(ToolGateway)
    gateway.store, gateway.lock, gateway.capabilities = store, threading.RLock(), {}
    gateway.control = SimpleNamespace(allows_progress=lambda *args: True)
    token = gateway.register(research_id=rid, role="god", agent_id="god/arb-a", control_generation=1,
        arbitration_id="arb-a", snapshot_hash="hash")
    page = gateway.call(token, {"name": "read_arbitration_context", "arguments": {"section": "discussion"}})
    assert running["snapshot_manifest"][0]["sha256"] == page["sha256"]
    assert running["snapshot_manifest"][0]["length"] == page["total_length"]
    selected = client.get(url + f"?at_seq={decided}").json()
    assert selected["status"] == "decided" and selected["decision"]["content_markdown"] == "future decision"
    assert "effects" not in selected
    result = client.get(url + f"?at_seq={applied}")
    assert result.status_code == 200 and result.json()["status"] == "applied"
    assert len(result.json()["effects"]) == 1
    assert "must-never-return" not in result.text
    rows = client.get(f"/api/research/{rid}/arbitrations?status=running&at_seq={started}").json()["items"]
    assert len(rows) == 1 and rows[0]["status"] == "running"


def test_arbitration_pagination_ownership_and_god_topology(tmp_path):
    client, store, rid = setup(tmp_path)
    seed(store, rid, "arb-a")
    seed(store, rid, "arb-b")
    url = f"/api/research/{rid}/arbitrations"
    first = client.get(url + "?scope_kind=discussion&scope_id=boss&limit=1").json()
    assert first["items"][0]["arbitration_id"] == "arb-a" and first["next_cursor"] == "arb-a"
    second = client.get(url + "?limit=1&after=" + first["next_cursor"]).json()
    assert second["items"][0]["arbitration_id"] == "arb-b" and second["next_cursor"] is None
    assert client.get(url + "?at_seq=-1").status_code == 422
    another = client.post("/api/research", json={"title": "other", "topic": "test", "mode": "demo", "client_request_id": "other"}).json()
    assert client.get(f"/api/research/{another['id']}/arbitrations/arb-a").status_code == 404
    snapshot = client.get(f"/api/research/{rid}/snapshot").json()
    god = [node for node in snapshot["topology"]["nodes"] if node["id"] == "god"]
    assert len(god) == 1 and not god[0].get("parentId") and not god[0]["data"].get("member_count")
    assert len(snapshot["arbitrations"]) == 2


def test_god_conversations_include_independent_identities_and_historical_last_message(tmp_path):
    client, store, rid = setup(tmp_path)
    requested, started, decided, applied = seed(store, rid)
    conversation = f"{rid}:god:arb-a"
    store.create_conversation(rid, conversation, title="God 仲裁", kind="arbitration", participants=[{"type": "agent", "id": "god/arb-a"}])
    store.create_message(rid, conversation, "god/arb-a", "future response", source_system="test", source_event_id="god-response")
    live = client.get(f"/api/research/{rid}/conversations?agent_id=god").json()["items"]
    assert len(live) == 1 and live[0]["last_message"]["content_markdown"] == "future response"
    old = client.get(f"/api/research/{rid}/conversations?agent_id=god&at_seq={requested - 1}").json()["items"]
    assert old == []


def test_waiting_recovery_is_cursor_based_and_snapshot_manifest_checks_project(tmp_path):
    client, store, rid = setup(tmp_path)
    _, _, _, applied = seed(store, rid)
    waiting = store.emit(rid, "task.waiting_recovery", {"task_id": "boss-task", "arbitration_id": "arb-a"})
    ready = store.emit(rid, "task.recovery_ready", {"task_id": "boss-task"})
    url = f"/api/research/{rid}/arbitrations"
    assert "resolution_status" not in client.get(url + f"?at_seq={applied}").json()["items"][0]
    assert client.get(url + f"?at_seq={waiting['seq']}").json()["items"][0]["resolution_status"] == "waiting_children"
    assert client.get(url + f"?at_seq={ready['seq']}").json()["items"][0]["resolution_status"] == "fulfilled"
    other = client.post("/api/research", json={"title": "other", "topic": "test", "mode": "demo", "client_request_id": "other-snapshot"}).json()
    store.put("arbitration_snapshot", "foreign-snapshot", {"sections": {"private discussion": "do not disclose"}}, other["id"])
    store.patch("arbitration_request", "arb-a", snapshot_id="foreign-snapshot")
    detail = client.get(url + "/arb-a").json()
    assert detail["snapshot_manifest"] == []
    assert "private discussion" not in str(detail)


def test_capacity_events_only_change_execution_phase_in_live_and_historical_views(tmp_path):
    client, store, rid = setup(tmp_path)
    aid = "arb-capacity"
    store.put("arbitration_request", aid, {"arbitration_id": aid, "status": "running", "scope_id": "boss",
        "scope_kind": "discussion", "source_task_id": "root"}, rid)
    refs = {"arbitration_id": aid}
    store.emit(rid, "arbitration.requested", refs)
    started = store.emit(rid, "arbitration.started", refs)
    waiting = store.emit(rid, "arbitration.capacity_wait", refs)
    store.emit(rid, "arbitration.context_read", refs)
    url = f"/api/research/{rid}/arbitrations/{aid}"
    live = client.get(url).json()
    assert live["status"] == "running" and live["execution_phase"] == "waiting_capacity"
    acquired = store.emit(rid, "arbitration.capacity_acquired", refs)
    assert client.get(url).json()["execution_phase"] == "judging"
    old = client.get(url + f"?at_seq={waiting['seq']}").json()
    assert old["status"] == "running" and old["execution_phase"] == "waiting_capacity"
    assert client.get(url + f"?at_seq={started['seq']}").json()["execution_phase"] == "judging"
    assert client.get(url + f"?at_seq={acquired['seq']}").json()["status"] == "running"
    rows = client.get(f"/api/research/{rid}/arbitrations?status=running&at_seq={waiting['seq']}").json()["items"]
    assert len(rows) == 1
    store.emit(rid, "arbitration.decided", refs)
    store.patch("arbitration_request", aid, status="decided")
    late = store.emit(rid, "arbitration.capacity_wait", refs)
    decided = client.get(url + f"?at_seq={late['seq']}").json()
    assert decided["status"] == "decided" and "execution_phase" not in decided


def test_internal_dependency_status_keeps_historical_external_judgment_and_wait_timing(tmp_path):
    client, store, rid = setup(tmp_path)
    _, _, _, applied = seed(store, rid)
    store.emit(rid, "arbitration.needs_external_input", {"arbitration_id": "arb-a"})
    store.patch("arbitration_request", "arb-a", status="needs_external_input", resolution_status="waiting_dependencies")
    waiting = store.emit(rid, "dependency.waiting", {"arbitration_id": "arb-a", "task_id": "boss-task"})
    ready = store.emit(rid, "dependency.ready", {"arbitration_id": "arb-a", "task_id": "boss-task"})
    cancelled = store.emit(rid, "dependency.cancelled", {"arbitration_id": "arb-a", "task_id": "boss-task"})
    url = f"/api/research/{rid}/arbitrations/arb-a"
    assert "resolution_status" not in client.get(url + f"?at_seq={applied}").json()
    view = client.get(url + f"?at_seq={waiting['seq']}").json()
    assert view["status"] == "needs_external_input" and view["resolution_status"] == "waiting_dependencies"
    assert client.get(url + f"?at_seq={ready['seq']}").json()["resolution_status"] == "fulfilled"
    assert client.get(url + f"?at_seq={cancelled['seq']}").json()["resolution_status"] == "cancelled"
