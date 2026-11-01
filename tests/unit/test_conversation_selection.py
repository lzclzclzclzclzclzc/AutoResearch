from backend.persistence import Store
from backend.persistence.conversation_views import conversation_list
from backend.persistence import store as persistence


def project(tmp_path, monkeypatch):
    clock = {"time": "2026-11-01T01:00:00Z"}
    monkeypatch.setattr(persistence, "now", lambda: clock["time"])
    store = Store(tmp_path)
    rid = store.create_project({"topic": "conversation ordering", "client_request_id": "chat"})["id"]
    return store, rid, clock


def conversation(store, rid, cid, actor="line-01/critic/member-1"):
    return store.create_conversation(rid, cid, participants=[{"type": "agent", "id": actor}],
        line_id="line-01", kind="group_discussion", summary_metadata={"role": "critic", "task_id": cid})


def test_active_session_first_then_latest_after_completion_and_stable_ties(tmp_path, monkeypatch):
    store, rid, clock = project(tmp_path, monkeypatch)
    for index in [2, 0, 1]:
        clock["time"] = f"2026-11-01T01:0{index}:00Z"
        conversation(store, rid, f"group-{index}")
    actor = "line-01/critic/member-1"
    clock["time"] = "2026-11-01T01:03:00Z"
    store.put("execution", "exe-0", {"conversation_id": "group-0", "status": "running", "agent_id": actor,
        "started_at": clock["time"], "execution_attempt_id": "attempt-0", "task_id": "group-0"}, rid)
    store.emit(rid, "agent.started", {}, conversation_id="group-0", agent_id=actor, execution_id="attempt-0")
    clock["time"] = "2026-11-01T01:05:00Z"
    store.create_message(rid, "group-2", actor, "more recent but finished")
    for identity in [actor, "line-01/critic"]:
        views = conversation_list(store, rid, identity=identity)
        assert [view["conversation_id"] for view in views] == ["group-0", "group-2", "group-1"]
        assert views[0]["is_active"]
    store.patch("execution", "exe-0", status="completed")
    clock["time"] = "2026-11-01T01:04:00Z"
    store.emit(rid, "agent.completed", {}, execution_id="attempt-0", agent_id=actor)
    assert conversation_list(store, rid, identity=actor)[0]["conversation_id"] == "group-2"
    # Same wall-clock activity is ordered by committed sequence, then stable ID.
    clock["time"] = "2026-11-01T01:06:00Z"
    store.create_message(rid, "group-0", actor, "first")
    store.create_message(rid, "group-1", actor, "second")
    assert [view["conversation_id"] for view in conversation_list(store, rid, identity=actor)][:2] == ["group-1", "group-0"]


def test_history_rebuilds_participants_before_filtering_and_hides_future_vote(tmp_path, monkeypatch):
    store, rid, clock = project(tmp_path, monkeypatch)
    actor, future_actor = "line-01/critic/member-1", "line-01/critic/member-2"
    item = conversation(store, rid, "old")
    assert conversation_list(store, rid, identity=actor, at_seq=item["created_event_seq"] - 1) == []
    started = store.emit(rid, "agent.started", {}, execution_id="exe-1", agent_id=actor)
    first = store.create_message(rid, "old", actor, "first speech", discussion_round=1)
    clock["time"] = "2026-11-01T01:10:00Z"
    store.create_message(rid, "old", future_actor, "future ballot", kind="discussion_vote",
        vote={"type": "end", "phase": "passed", "ballots": [{"approve": True}]}, discussion_round=4)
    store.put("execution", "exe", {"conversation_id": "old", "status": "completed", "agent_id": actor,
        "execution_attempt_id": "exe-1", "task_id": "old", "started_at": started["occurred_at"]}, rid)
    store.emit(rid, "agent.completed", {}, execution_id="exe-1", agent_id=actor)
    store.put("discussion_state", "old", {"mode": "ended"}, rid)
    assert conversation_list(store, rid, identity=future_actor, at_seq=first["event_seq"]) == []
    past = conversation_list(store, rid, identity=actor, at_seq=first["event_seq"])[0]
    assert past["activity_status"] == "running"
    assert "发言第 1 轮" in past["summary"] and "结束投票" not in past["summary"]
    assert past["last_message"]["content_markdown"] == "first speech"
    assert past["last_activity_seq"] == first["event_seq"]
    assert {p["id"] for p in past["participants"]} == {actor}
    assert "结束投票" in conversation_list(store, rid, identity=future_actor)[0]["summary"]


def test_history_activity_order_and_legacy_participation_are_cursor_bound(tmp_path, monkeypatch):
    store, rid, clock = project(tmp_path, monkeypatch)
    actor = "line-02/implementation"
    # Legacy conversations were implicitly created from messages.
    first = store.create_message(rid, "legacy-old", actor, "older")
    clock["time"] = "2026-11-01T01:01:00Z"
    last_visible = store.create_message(rid, "legacy-new", actor, "newer")
    clock["time"] = "2026-11-01T01:02:00Z"
    store.create_message(rid, "legacy-old", "line-01/verify", "future handoff")
    assert conversation_list(store, rid, identity="line-01/verify", at_seq=last_visible["event_seq"]) == []
    past = conversation_list(store, rid, identity=actor, at_seq=last_visible["event_seq"])
    assert [view["conversation_id"] for view in past] == ["legacy-new", "legacy-old"]
    assert [view["conversation_id"] for view in conversation_list(store, rid, identity=actor)] == ["legacy-old", "legacy-new"]
    assert [view["conversation_id"] for view in conversation_list(store, rid, at_seq=first["event_seq"])] == ["legacy-old"]
    assert conversation_list(store, rid, identity=actor, at_seq=0) == []


def test_creation_membership_and_group_recipient_are_visible_before_member_speech(tmp_path, monkeypatch):
    store, rid, clock = project(tmp_path, monkeypatch)
    actor = "line-01/critic/member-1"
    initial = conversation(store, rid, "assigned", actor)
    assert conversation_list(store, rid, identity=actor, at_seq=initial["created_event_seq"])[0]["conversation_id"] == "assigned"
    first = store.create_message(rid, "group-handoff", "line-01/verify", "needs revision",
        recipients=[{"type": "group", "id": "line-01/critic"}])
    assert conversation_list(store, rid, identity=actor, at_seq=first["event_seq"])[0]["conversation_id"] == "group-handoff"
    added_actor = "line-01/critic/member-2"
    updated = store.create_conversation(rid, "assigned", participants=[{"type": "agent", "id": added_actor}])
    assert conversation_list(store, rid, identity=added_actor, at_seq=initial["created_event_seq"]) == []
    assert any(view["conversation_id"] == updated["conversation_id"] for view in conversation_list(store, rid, identity=added_actor,
        at_seq=store.events(rid)[-1]["seq"]))


def test_other_member_execution_does_not_activate_unrelated_member_session(tmp_path, monkeypatch):
    store, rid, clock = project(tmp_path, monkeypatch)
    a, b = "line-01/critic/member-1", "line-01/critic/member-2"
    store.create_conversation(rid, "shared", participants=[{"type": "agent", "id": a}, {"type": "agent", "id": b}])
    store.put("execution", "exe", {"conversation_id": "shared", "status": "running", "agent_id": b}, rid)
    assert not conversation_list(store, rid, identity=a)[0]["is_active"]
    assert conversation_list(store, rid, identity="line-01/critic")[0]["is_active"]
