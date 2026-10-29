import json

import pytest
from fastapi.testclient import TestClient

from apps.api.main import create_app
from backend.persistence.event_views import tool_event_type
from backend.persistence.store import Store


@pytest.fixture
def store(tmp_path):
    return Store(tmp_path)


def notification(kind, call_id="call", *, result="fixture result", failed=False):
    data = {"callId": call_id, "name": "read_research_input", "arguments": '{"relative_path":"input.txt"}'} if kind == "tool/call" else {
        "message": {"toolCallId": call_id, "isError": failed, "content": [{"type": "text", "text": result}]}}
    return {"type": "notification", "method": "session.event", "payload": {"event": {"type": kind, "data": data}}}


def emit_tool(store, project, kind, call_id="call", *, execution="e-1", legacy=False, conversation="chat", **kwargs):
    item = notification(kind, call_id, **kwargs)
    return store.emit(project["id"], "agent.runtime_event" if legacy else tool_event_type(item),
        {"execution_attempt_id": execution, "notification": item}, agent_id="boss/member-01",
        **({"conversation_id": conversation} if conversation else {}))


def test_compact_activity_pages_skip_deltas_without_changing_the_audit_log(store):
    project = store.create_project({"client_request_id": "events", "topic": "events"})
    for index in range(120):
        store.emit(project["id"], "agent.output_delta", {"delta": str(index)})
    important = store.emit(project["id"], "agent.failed", {"message": "provider failed"})
    for index in range(70):
        store.emit(project["id"], "message.delta", {"delta": str(index)})
    snapshot = store.snapshot(project["id"])
    assert snapshot["snapshot_seq"] == 192
    assert snapshot["activity_event_count"] == 2
    assert snapshot["recent_events"][-1] == important
    first = store.events(project["id"], limit=1, detail=False)
    second = store.events(project["id"], after=first[-1]["seq"], limit=1, detail=False)
    assert second == [important]
    assert len(store.events(project["id"], limit=2000)) == 192


@pytest.mark.parametrize("legacy", [True, False])
def test_tools_replay_pairs_results_and_respects_search_and_attempt_identity(store, legacy):
    project = store.create_project({"client_request_id": "tools", "topic": "tools"})
    store.create_conversation(project["id"], "chat")
    call = emit_tool(store, project, "tool/call", legacy=legacy, conversation=None if legacy else "chat")
    store.create_message(project["id"], "chat", "boss/member-01", "visible text", execution_id="e-1")
    result = emit_tool(store, project, "tool/result", legacy=legacy, result='{"value":"only-in-result"}')
    emit_tool(store, project, "tool/call", execution="e-2", legacy=legacy)
    emit_tool(store, project, "tool/result", execution="e-2", legacy=legacy, failed=True, result="tool failure")
    old = store.tool_calls(project["id"], "chat", at_seq=call["seq"])["items"]
    assert len(old) == 1 and old[0]["status"] == "pending" and "result" not in old[0]
    after = store.tool_calls(project["id"], "chat", at_seq=result["seq"])["items"]
    assert after[0]["arguments"] == {"relative_path": "input.txt"}
    assert after[0]["result"] == {"value": "only-in-result"}
    calls = store.tool_calls(project["id"], "chat", limit=1)
    assert calls["items"][0]["status"] == "failed"
    assert calls["next_cursor"]
    earlier = store.tool_calls(project["id"], "chat", before=calls["next_cursor"], limit=1)
    assert earlier["items"][0]["status"] == "complete" and earlier["next_cursor"] is None
    assert len(store.tool_calls(project["id"], "chat", q="ONLY-IN-RESULT")["items"]) == 1
    assert not store.tool_calls(project["id"], "chat", q="only-in-result", at_seq=call["seq"])["items"]


def test_tool_api_keeps_conversations_inside_their_project_and_defaults_to_compact_timeline(tmp_path):
    app = create_app(tmp_path, testing=True)
    store = Store(tmp_path)
    a = store.create_project({"client_request_id": "a", "topic": "a"})
    b = store.create_project({"client_request_id": "b", "topic": "b"})
    store.create_conversation(b["id"], "other-chat")
    store.emit(a["id"], "agent.output_delta", {"delta": "hidden from activity"})
    with TestClient(app) as client:
        client.get("/api/session")
        compact = client.get(f'/api/research/{a["id"]}/timeline').json()["items"]
        assert [event["type"] for event in compact] == ["research.created"]
        assert len(client.get(f'/api/research/{a["id"]}/timeline?detail=true').json()["items"]) == 2
        assert client.get(f'/api/research/{a["id"]}/tool-calls?conversation_id=other-chat').status_code == 404
        assert not client.get(f'/api/research/{a["id"]}/tool-calls').json()["items"]
