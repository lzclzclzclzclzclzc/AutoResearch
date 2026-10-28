from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from backend.persistence import Store, StoreError


class ConfirmingControl:
    def __init__(self):
        self.stopped = False
        self.seq = 0
        self.receipts = {}

    def confirm_operation(self, research_id, operation_id, content_hash, control_generation=1):
        if operation_id in self.receipts:
            return self.receipts[operation_id]
        if self.stopped:
            return None
        self.seq += 1
        receipt = {"research_id": research_id, "operation_id": operation_id, "content_hash": content_hash,
                "control_epoch": "test-epoch", "control_seq": self.seq, "control_generation": control_generation, "persisted": True}
        self.receipts[operation_id] = receipt
        return receipt

    def allows_progress(self, research_id, control_generation=None):
        return not self.stopped

    def get_confirmation(self, operation_id):
        return self.receipts.get(operation_id)


@pytest.fixture
def business(tmp_path):
    s = Store(tmp_path)
    p = s.create_project({"topic": "Evidence test", "client_request_id": "first", "team": {"k": 3}})
    s.start_project(p["id"])
    cycle = s.put("cycle", "cycle-1", {"line_id": "line-01", "k": 3, "status": "verifying",
        "completed_verify_rounds": 0}, p["id"])
    return s, p["id"], cycle


def submit(s, research_id, number, passed=False):
    sid = f"submission-{number}"
    s.put("submission", sid, {"cycle_id": "cycle-1", "spec_id": "spec-1", "code_version": "code-1", "run_ids": ["run-1"]}, research_id)
    report = {"passed": passed, "checks": ["Compared plan and saved metrics"], "spec_id": "spec-1",
        "code_version": "code-1", "run_ids": ["run-1"], "supported_conclusions": ["negative result within tested range"]}
    return sid, report


def test_verify_failure_counts_once_and_k_routes_only_boss(business):
    s, rid, cycle = business
    control = ConfirmingControl()
    for number in (1, 2, 3):
        sid, report = submit(s, rid, number)
        result = s.commit_verification("cycle-1", sid, report, 1, control)
        assert result["round_no"] == number
        assert result["passed"] is False
        assert result["route"] == ("boss" if number == 3 else "exploration_and_critic")
        assert s.commit_verification("cycle-1", sid, report, 1, control) == result
    assert s.get("cycle", "cycle-1")["completed_verify_rounds"] == 3
    assert len(s.list("verification", rid)) == 3
    assert len(s.list("outbox", rid)) == 3
    assert s.list("outbox", rid)[-1]["recipients"] == [{"type": "group", "id": "boss"}]


def test_unconfirmed_report_cannot_increment_or_dispatch(business):
    s, rid, _ = business
    control = ConfirmingControl()
    control.stopped = True
    sid, report = submit(s, rid, 1)
    result = s.commit_verification("cycle-1", sid, report, 1, control)
    assert result["route"] == "awaiting_confirmation_sync"
    assert s.get("cycle", "cycle-1")["completed_verify_rounds"] == 0
    assert not s.list("outbox", rid)
    assert not s.list("verification", rid)


def test_verified_negative_result_can_pass_early(business):
    s, rid, _ = business
    sid, report = submit(s, rid, 1, True)
    result = s.commit_verification("cycle-1", sid, report, 1, ConfirmingControl())
    assert result["route"] == "boss"
    assert result["round_no"] == 1
    assert result["reason"] == "verification_passed"


def test_stopped_project_preserves_late_report_without_round(business):
    s, rid, _ = business
    sid, report = submit(s, rid, 1)
    s.update_project(rid, status="terminated", control_generation=2)
    assert s.commit_verification("cycle-1", sid, report, 1, ConfirmingControl())["route"] == "halted"
    assert s.get("cycle", "cycle-1")["completed_verify_rounds"] == 0
    assert s.list("verification_archive", rid)[0]["status"] == "verification_pending"


def test_non_boss_cannot_write_memory_and_version_fences(business):
    s, rid, _ = business
    ctrl = ConfirmingControl()
    with pytest.raises(StoreError, match="只有 Boss"):
        s.write_memory(rid, "missing", "batch-1", [], 0, ctrl, role="verify")
    decision = s.commit_boss_decision(rid, "boss-1", {"action": "continue"}, ctrl)
    assert decision["status"] == "applied"
    result = s.write_memory(rid, "boss-1", "batch-1", [{"node_id": "index", "content": "negative result", "status": "passed"}], 0, ctrl)
    assert result["memory_version"] == 1
    assert s.write_memory(rid, "boss-1", "batch-1", [{"node_id": "index", "content": "negative result", "status": "passed"}], 0, ctrl) == result
    with pytest.raises(StoreError, match="共享记忆已更新"):
        s.write_memory(rid, "boss-1", "batch-2", [], 0, ctrl)


def test_message_dedup_cursor_and_project_isolation(business):
    s, rid, _ = business
    for number in range(5):
        s.create_message(rid, "conversation-1", "agent-1", f"message {number}", source_event_id=f"sdk-{number}")
    repeated = s.create_message(rid, "conversation-1", "agent-1", "changed duplicate", source_event_id="sdk-1")
    assert repeated["content_markdown"] == "message 1"
    page1 = s.messages("conversation-1", limit=2)
    page2 = s.messages("conversation-1", before=page1["next_cursor"], limit=2)
    assert [m["content_markdown"] for m in page1["items"]] == ["message 3", "message 4"]
    assert [m["content_markdown"] for m in page2["items"]] == ["message 1", "message 2"]
    other = s.create_project({"topic": "Other", "client_request_id": "second"})
    with pytest.raises(StoreError):
        s.create_message(other["id"], "conversation-1", "agent-1", "cross-project")


def test_event_sequences_are_atomic_across_writers(business):
    s, rid, _ = business
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda n: s.emit(rid, "test.event", {"number": n}), range(50)))
    sequences = [x["seq"] for x in s.events(rid)]
    assert sequences == list(range(1, len(sequences)+1))
    snap = s.snapshot(rid)
    assert snap["snapshot_seq"] == sequences[-1]


def test_deadline_does_not_change_on_restart_or_new_direction(business):
    s, rid, _ = business
    original = s.project(rid)["deadline_at"]
    s.update_line(rid, "line-01", direction_epoch=2)
    s.update_project(rid, status="paused")
    reopened = Store(s.data_dir)
    assert reopened.start_project(rid)["deadline_at"] == original


def test_stop_sync_idempotent_and_artifact_path_confined(business, tmp_path):
    s, rid, _ = business
    status = {"stop_id": "stop-1", "reason": "user_requested", "execution_stopped": False}
    s.sync_stop(rid, status)
    s.sync_stop(rid, status)
    assert s.project(rid)["control_generation"] == 2
    outside = tmp_path.parent / "not-an-artifact.txt"
    outside.write_text("outside")
    with pytest.raises(StoreError):
        s.register_artifact(rid, outside)
    artifact_file = s.data_dir / "artifacts" / "metrics.txt"
    artifact_file.write_text("value=1")
    artifact = s.register_artifact(rid, artifact_file, "text/plain")
    preserved = s.artifact(artifact["id"])[1]
    artifact_file.write_text("value=2")
    assert preserved.read_text() == "value=1"


def test_stop_completion_emits_once_after_resources_reconcile_and_replay_preserves_time(business):
    s, rid, _ = business
    status = {"stop_id": "stop-final", "reason": "human_terminate", "execution_stopped": True, "unfinished": []}
    s.put("allocation", "allocation-final", {"state": "running"}, rid)
    s.put("light_cpu", "light-final", {"state": "reconciling"}, rid)
    s.sync_stop(rid, status)
    assert s.project(rid)["status"] == "terminating"
    assert not [event for event in s.events(rid) if event["type"] == "research.terminated"]
    s.patch("allocation", "allocation-final", state="released")
    s.sync_stop(rid, status)
    assert s.project(rid)["status"] == "terminating"
    s.patch("light_cpu", "light-final", state="interrupted")
    s.sync_stop(rid, status)
    final = s.project(rid)
    assert final["status"] == "terminated" and final["terminated_at"]
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda _: Store(s.data_dir).sync_stop(rid, status), range(8)))
    assert s.project(rid) == final
    events = s.events(rid)
    terminated = [event for event in events if event["type"] == "research.terminated"]
    assert len(terminated) == 1
    assert terminated[0]["payload"] == {"stop_id": "stop-final", "reason": "human_terminate", "terminated_at": final["terminated_at"]}
    assert len([event for event in events if event["type"] == "research.terminating"]) == 1


def test_stop_final_state_and_event_rollback_together(business, monkeypatch):
    s, rid, _ = business
    status = {"stop_id": "stop-atomic", "reason": "human_terminate", "execution_stopped": False}
    s.sync_stop(rid, status)
    original = s._emit
    def fail_final_event(c, research_id, event_type, payload=None, **refs):
        if event_type == "research.terminated":
            raise OSError("Injected final stop event failure")
        return original(c, research_id, event_type, payload, **refs)
    monkeypatch.setattr(s, "_emit", fail_final_event)
    with pytest.raises(OSError):
        s.sync_stop(rid, {**status, "execution_stopped": True})
    assert s.project(rid)["status"] == "terminating"
    assert not s.project(rid).get("terminated_at")
    monkeypatch.setattr(s, "_emit", original)
    s.sync_stop(rid, {**status, "execution_stopped": True})
    assert s.project(rid)["status"] == "terminated"
    assert len([event for event in s.events(rid) if event["type"] == "research.terminated"]) == 1


def test_default_topology_has_seventeen_real_agent_identities(business):
    s, rid, _ = business
    snapshot = s.snapshot(rid)
    assert snapshot["topology"]["agent_count"] == 17
    assert len(snapshot["agents"]) == 17
    assert len({x["agent_id"] for x in snapshot["agents"]}) == 17


def test_boss_effect_replay_never_resets_cycle_or_increments_epoch_twice(business):
    s, rid, _ = business
    ctrl = ConfirmingControl()
    decision = {"action": "change_direction", "assignments": [{"line_id": "line-01", "objective": "A new direction"}],
                "request_report": True, "rationale": "Evidence requires a new hypothesis"}
    s.commit_boss_decision(rid, "decision-1", decision, ctrl)
    result = s.apply_boss_effects(rid, "decision-1", snapshot={"decision": decision}, control=ctrl)
    cycle_id = result["cycle_ids"][0]
    s.patch("cycle", cycle_id, completed_verify_rounds=2)
    s.update_line(rid, "line-01", completed_verify_rounds=2)
    assert s.apply_boss_effects(rid, "decision-1", control=ctrl) == result
    assert s.lines(rid)[0]["direction_epoch"] == 2
    assert s.get("cycle", cycle_id)["completed_verify_rounds"] == 2
    assert s.lines(rid)[0]["completed_verify_rounds"] == 2
    assert len([t for t in s.list("task", rid) if t.get("boss_decision_id") == "decision-1"]) == 2


def test_boss_effect_projection_transaction_rolls_back_and_recovers(business, monkeypatch):
    s, rid, _ = business
    ctrl = ConfirmingControl()
    s.commit_boss_decision(rid, "decision-crash", {"action": "change_direction", "assignments": [
        {"line_id": "line-01", "objective": "New"}], "request_report": True}, ctrl)
    original = s._put
    def disk_error(c, kind, object_id, data, research_id=None):
        if kind == "report_snapshot":
            raise OSError("Injected failure after line and cycle writes")
        return original(c, kind, object_id, data, research_id)
    monkeypatch.setattr(s, "_put", disk_error)
    with pytest.raises(OSError):
        s.apply_boss_effects(rid, "decision-crash", control=ctrl)
    assert s.lines(rid)[0]["direction_epoch"] == 1
    assert not s.get("cycle", "decision-crash:line-01:cycle")
    assert not s.get("boss_effects", "decision-crash")
    monkeypatch.setattr(s, "_put", original)
    assert s.apply_boss_effects(rid, "decision-crash", control=ctrl)["status"] == "applied"
    assert s.lines(rid)[0]["direction_epoch"] == 2


def test_boss_receipt_before_stop_cannot_dispatch_after_stop(business):
    s, rid, _ = business
    ctrl = ConfirmingControl()
    s.commit_boss_decision(rid, "decision-stop", {"action": "continue", "assignments": [
        {"line_id": "line-01", "objective": "Continue"}], "request_report": True}, ctrl)
    original = ctrl.confirm_operation
    def confirm_then_stop(*args):
        receipt = original(*args)
        ctrl.stopped = True
        return receipt
    ctrl.confirm_operation = confirm_then_stop
    result = s.apply_boss_effects(rid, "decision-stop", control=ctrl)
    assert result["status"] == "applied"
    assert result["dispatch_allowed"] is False
    assert all(s.get("task", task_id)["status"] == "cancelled" for task_id in result["task_ids"])
    assert s.get("cycle", result["cycle_ids"][0])["status"] == "archived"
    assert s.lines(rid)[0]["cycle_id"] is None


def test_memory_completed_batch_replay_accepts_current_projection_version(business):
    s, rid, _ = business
    ctrl = ConfirmingControl()
    s.commit_boss_decision(rid, "decision-memory", {"action": "continue"}, ctrl)
    update = [{"node_id": "index", "content": "Preserved evidence"}]
    first = s.write_memory(rid, "decision-memory", "batch", update, 0, ctrl)
    assert s.write_memory(rid, "decision-memory", "batch", update, 1, ctrl) == first
    assert s.project(rid)["memory_version"] == 1


def test_recovery_applies_pre_stop_receipt_but_never_dispatches(business, monkeypatch):
    s, rid, _ = business
    ctrl = ConfirmingControl()
    sid, report = submit(s, rid, 1)
    original = s._put
    def fail_projection(c, kind, object_id, data, research_id=None):
        if kind == "verification":
            raise OSError("Database write failed after independent durable confirmation")
        return original(c, kind, object_id, data, research_id)
    monkeypatch.setattr(s, "_put", fail_projection)
    with pytest.raises(OSError):
        s.commit_verification("cycle-1", sid, report, 1, ctrl)
    assert s.get("cycle", "cycle-1")["completed_verify_rounds"] == 0
    assert len(ctrl.receipts) == 1
    ctrl.stopped = True
    monkeypatch.setattr(s, "_put", original)
    s.sync_stop(rid, {"stop_id": "stop-recovery", "stopped": True, "unfinished": []})
    first = s.recover_confirmed_operations(ctrl)
    assert len(first["applied"]) == 1
    assert s.get("cycle", "cycle-1")["completed_verify_rounds"] == 1
    assert s.get("cycle", "cycle-1")["status"] == "stopped"
    assert s.list("outbox", rid)[0]["status"] == "cancelled"
    assert s.project(rid)["status"] == "terminated"
    assert s.recover_confirmed_operations(ctrl)["applied"] == []
    assert len(s.list("verification", rid)) == 1


def test_recovery_never_confirms_a_pre_stop_unconfirmed_candidate(business):
    s, rid, _ = business
    ctrl = ConfirmingControl()
    ctrl.stopped = True
    sid, report = submit(s, rid, 1)
    s.commit_verification("cycle-1", sid, report, 1, ctrl)
    recovered = s.recover_confirmed_operations(ctrl)
    assert recovered["applied"] == []
    assert len(recovered["pending"]) == 1
    assert not ctrl.receipts
    assert s.get("cycle", "cycle-1")["completed_verify_rounds"] == 0


def test_report_requires_confirmation_and_publication_is_idempotent(business):
    s, rid, _ = business
    ctrl = ConfirmingControl()
    ctrl.stopped = True
    report = {"title": "Stage report", "artifact_id": "artifact-1", "snapshot_id": "snapshot-1"}
    assert s.commit_report(rid, "report-1", report, ctrl)["status"] == "awaiting_confirmation_sync"
    assert not s.list("report", rid)
    ctrl.stopped = False
    first = s.commit_report(rid, "report-1", report, ctrl)
    assert first["status"] == "completed"
    assert s.commit_report(rid, "report-1", report, ctrl) == first
    assert len([e for e in s.events(rid) if e["type"] == "report.completed"]) == 1


def test_snapshot_evidence_dates_come_from_durable_events(business):
    s, rid, _ = business
    sid, _ = submit(s, rid, 1)
    manifest = s.get("submission", sid)
    assert "created_at" not in manifest
    ctrl = ConfirmingControl()
    s.commit_report(rid, "dated-report", {"title": "Report", "artifact_id": "evidence-1"}, ctrl)
    snapshot = s.snapshot(rid)
    assert snapshot["submissions"][0]["created_at"]
    assert snapshot["submissions"][0]["created_event_seq"] > 0
    assert snapshot["reports"][0]["created_at"]
    assert "created_at" not in s.get("submission", sid)


def test_conversation_collects_all_members_and_real_group_recipients(business):
    s, rid, _ = business
    for i in range(1, 4):
        sender = f"line-01/exploration/member-{i:02d}"
        s.create_conversation(rid, "all-members", participants=[{"type": "agent", "id": sender}])
        s.create_message(rid, "all-members", sender, f"Member {i}", recipients=[{"type": "group", "id": "line-01/critic"}])
    conversation = s.get("conversation", "all-members")
    assert len(conversation["participants"]) == 4
    assert set(conversation["group_ids"]) == {"line-01/exploration", "line-01/critic"}


def test_full_history_sql_search_paginates_before_filtering_page(business):
    s, rid, _ = business
    edge_id = "line-01/exploration->line-01/critic"
    for number in range(15):
        s.create_message(rid, "search-all", "line-01/exploration/member-01", f"{'Target 证据' if number < 5 else 'Other'} {number}", edge_id=edge_id,
            recipients=[{"type": "group", "id": "line-01/critic"}])
    page = s.messages("search-all", limit=2, q="target 证据")
    assert [m["content_markdown"] for m in page["items"]] == ["Target 证据 3", "Target 证据 4"]
    older = s.messages("search-all", before=page["next_cursor"], limit=2, q="target 证据")
    assert [m["content_markdown"] for m in older["items"]] == ["Target 证据 1", "Target 证据 2"]
    assert s.edge_messages(rid, edge_id, limit=2, q="target 证据")["items"] == page["items"]
    snapshot = s.snapshot(rid)
    nodes = {n["id"]: n for n in snapshot["topology"]["nodes"]}
    assert nodes["line-01/exploration"]["data"]["message_count"] == 15
    assert nodes["line-01/critic"]["data"]["message_count"] == 15
    edge = next(e for e in snapshot["topology"]["edges"] if e["id"] == edge_id)
    assert edge["data"]["message_count"] == 15
    assert edge["data"]["latest_message_seq"] == s.messages("search-all", limit=1)["items"][0]["event_seq"]


def test_real_stream_is_one_durable_row_and_final_body_replaces_deltas(business):
    s, rid, _ = business
    fields = {"source_event_id": "execution-1:stream:1", "execution_id": "execution-1", "edge_id": "line-01/exploration->line-01/critic"}
    before = len(s.events(rid))
    assert s.upsert_stream_message(rid, "stream-1", "stream-conversation", "line-01/exploration/member-01", "", chunk_id="0", **fields) is None
    assert s.messages("stream-conversation")["items"] == []
    assert len(s.events(rid)) == before
    first = s.upsert_stream_message(rid, "stream-1", "stream-conversation", "line-01/exploration/member-01", "真实", chunk_id="1", **fields)
    second = s.upsert_stream_message(rid, "stream-1", "stream-conversation", "line-01/exploration/member-01", "真实正文", chunk_id="2", **fields)
    assert first["cursor"] == second["cursor"]
    reopened = Store(s.data_dir)
    assert reopened.upsert_stream_message(rid, "stream-1", "stream-conversation", "line-01/exploration/member-01", "真实正文", chunk_id="2", **fields) == second
    completed = reopened.complete_stream_message("stream-1", "最终核准正文", source_event_id="logical-turn-1",
        kind="decision", structured={"accepted": True}, recipients=[{"type": "group", "id": "line-01/critic"}])
    assert completed["cursor"] == first["cursor"]
    assert completed["content_markdown"] == "最终核准正文"
    assert completed["status"] == "complete"
    assert completed["partial"] is False
    assert reopened.complete_stream_message("stream-1", "最终核准正文", source_event_id="logical-turn-1",
        kind="decision", structured={"accepted": True}, recipients=[{"type": "group", "id": "line-01/critic"}]) == completed
    assert len(s.messages("stream-conversation")["items"]) == 1
    stream_events = [event for event in s.events(rid) if event["payload"].get("message_id") == "stream-1"]
    assert [event["type"] for event in stream_events] == ["message.started", "message.delta", "message.delta", "message.completed"]
    assert [event["payload"]["delta"] for event in stream_events if event["type"] == "message.delta"] == ["真实", "正文"]
    assert stream_events[-1]["payload"]["full_content"] == "最终核准正文"
    assert stream_events[-1]["payload"]["message"]["source_event_id"] == "logical-turn-1"
    with pytest.raises(StoreError, match="已经绑定"):
        reopened.upsert_stream_message(rid, "different-message-id", "stream-conversation", "line-01/exploration/member-01", "Cannot rebind stream source", chunk_id="1", **fields)
    edge = next(edge for edge in s.snapshot(rid)["topology"]["edges"] if edge["id"] == fields["edge_id"])
    assert edge["data"]["message_count"] == 1
    assert edge["data"]["latest_message_seq"] == completed["latest_event_seq"]


def test_stop_interrupts_real_partial_text_and_late_success_cannot_reopen_it(business):
    s, rid, _ = business
    arguments = (rid, "stream-stop", "stop-stream-conversation", "line-01/verify")
    partial = s.upsert_stream_message(*arguments, "仅已收到的片段", source_event_id="execution-stop:stream:1", chunk_id="1")
    s.sync_stop(rid, {"stop_id": "stop-stream-1", "stopped": True, "unfinished": []})
    interrupted = s.messages("stop-stream-conversation")["items"][0]
    assert interrupted["status"] == "interrupted"
    assert interrupted["partial"] is True
    assert interrupted["content_markdown"] == "仅已收到的片段"
    assert interrupted["cursor"] == partial["cursor"]
    assert s.upsert_stream_message(*arguments, "仅已收到的片段+迟到内容", source_event_id="execution-stop:stream:1", chunk_id="2")["status"] == "interrupted"
    assert s.complete_stream_message("stream-stop", "迟到完整结果", status="complete")["content_markdown"] == "仅已收到的片段"
    assert s.get("cycle", "cycle-1")["completed_verify_rounds"] == 0
    assert len([event for event in s.events(rid) if event["type"] == "message.interrupted"]) == 1
    assert not [event for event in s.events(rid) if event["type"] == "message.completed"]


def test_stream_chunk_replay_and_out_of_order_updates_never_duplicate_or_shrink_text(business):
    s, rid, _ = business
    arguments = (rid, "stream-order", "order-stream-conversation", "line-01/critic/member-01")
    s.upsert_stream_message(*arguments, "Prefix", source_event_id="stream-order-source", chunk_id="1")
    latest = s.upsert_stream_message(*arguments, "Prefix suffix", source_event_id="stream-order-source", chunk_id="3")
    old = s.upsert_stream_message(*arguments, "Prefix s", source_event_id="stream-order-source", chunk_id="2")
    assert old == latest
    assert len([event for event in s.events(rid) if event["type"] == "message.delta"]) == 2
    with pytest.raises(StoreError, match="分片 ID"):
        s.upsert_stream_message(*arguments, "Conflicting", source_event_id="stream-order-source", chunk_id="3")
    with pytest.raises(StoreError, match="内容版本"):
        s.upsert_stream_message(*arguments, "Corrected", source_event_id="stream-order-source", chunk_id="4")
    corrected = s.upsert_stream_message(*arguments, "Corrected", source_event_id="stream-order-source", chunk_id="4", content_version=2)
    assert corrected["content_markdown"] == "Corrected"
    event = s.events(rid)[-1]
    assert event["payload"]["replace"] is True
    assert event["payload"]["full_content"] == "Corrected"


def test_stream_insert_and_delta_event_are_atomic(business, monkeypatch):
    s, rid, _ = business
    original = s._emit
    def fail_delta(c, research_id, event_type, payload=None, **refs):
        if event_type == "message.delta":
            raise OSError("Injected event persistence failure")
        return original(c, research_id, event_type, payload, **refs)
    monkeypatch.setattr(s, "_emit", fail_delta)
    with pytest.raises(OSError):
        s.upsert_stream_message(rid, "stream-atomic", "atomic-stream-conversation", "agent-1", "Actual content", source_event_id="atomic-source", chunk_id="1")
    assert s.messages("atomic-stream-conversation")["items"] == []
    assert not s.get("conversation", "atomic-stream-conversation")
    assert not [event for event in s.events(rid) if event["type"].startswith("message.")]
    monkeypatch.setattr(s, "_emit", original)
    assert s.upsert_stream_message(rid, "stream-atomic", "atomic-stream-conversation", "agent-1", "Actual content", source_event_id="atomic-source", chunk_id="1")["status"] == "streaming"


def test_concurrent_duplicate_stream_chunks_create_one_message_and_one_delta(business):
    s, rid, _ = business
    def deliver(_):
        return s.upsert_stream_message(rid, "stream-concurrent", "concurrent-stream", "agent-1", "Actual received text", source_event_id="concurrent-source", chunk_id="one")
    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(deliver, range(12)))
    assert len({result["cursor"] for result in results}) == 1
    assert len(s.messages("concurrent-stream")["items"]) == 1
    assert len([event for event in s.events(rid) if event["type"] == "message.started"]) == 1
    assert len([event for event in s.events(rid) if event["type"] == "message.delta"]) == 1


def test_history_paginates_before_future_rows_and_reconstructs_only_actual_past_text(business):
    s, rid, _ = business
    edge = "line-01/exploration->line-01/critic"
    s.create_message(rid, "temporal-stream", "agent-1", "Older completed message", edge_id=edge)
    first = s.upsert_stream_message(rid, "stream-history", "temporal-stream", "agent-1", "已知", source_event_id="history-source", chunk_id="1", edge_id=edge)
    second = s.upsert_stream_message(rid, "stream-history", "temporal-stream", "agent-1", "已知 补充", source_event_id="history-source", chunk_id="2", edge_id=edge)
    final = s.complete_stream_message("stream-history", "最终正文 FUTURE_ONLY", structured={"future": "not visible before completion"}, kind="decision")
    for number in range(50):
        s.create_message(rid, "temporal-stream", "agent-1", f"Future message {number}", edge_id=edge)
    page = s.messages("temporal-stream", limit=1, at_seq=first["latest_event_seq"])
    assert len(page["items"]) == 1
    past = page["items"][0]
    assert past["message_id"] == "stream-history"
    assert past["content_markdown"] == "已知"
    assert past["status"] == "streaming"
    assert past["kind"] == "discussion"
    assert "structured" not in past
    assert page["next_cursor"]
    older = s.messages("temporal-stream", before=page["next_cursor"], limit=1, at_seq=first["latest_event_seq"])
    assert older["items"][0]["content_markdown"] == "Older completed message"
    assert s.edge_messages(rid, edge, limit=1, at_seq=first["latest_event_seq"])["items"] == page["items"]
    assert s.messages("temporal-stream", q="已知", at_seq=first["latest_event_seq"])["items"][0]["content_markdown"] == "已知"
    assert not s.messages("temporal-stream", q="FUTURE_ONLY", at_seq=first["latest_event_seq"])["items"]
    assert s.messages("temporal-stream", limit=1, at_seq=second["latest_event_seq"])["items"][0]["content_markdown"] == "已知 补充"
    assert s.messages("temporal-stream", limit=1, at_seq=final["latest_event_seq"])["items"][0]["content_markdown"] == "最终正文 FUTURE_ONLY"
    assert not s.messages("temporal-stream", at_seq=0)["items"]


def test_verify_outbox_delivers_real_message_and_replayable_group_inboxes(business):
    s, rid, _ = business
    ctrl = ConfirmingControl()
    sid, report = submit(s, rid, 1)
    transition = s.commit_verification("cycle-1", sid, report, 1, ctrl)
    oid = transition["operation_id"]
    assert s.dispatch_outbox(ctrl)["delivered"] == [oid]
    assert s.get("outbox", oid)["status"] == "delivered"
    entries = s.list("inbox", rid)
    assert {entry["recipient_id"] for entry in entries} == {"line-01/exploration", "line-01/critic"}
    assert len({entry["message_id"] for entry in entries}) == 1
    assert len(s.list("message_delivery", rid)) == 2
    messages = s.messages("cycle-1:feedback")["items"]
    assert len(messages) == 1 and messages[0]["source_system"] == "verification_dispatch"
    assert messages[0]["source_event_id"] == sid
    assert messages[0]["submission_id"] == sid and messages[0]["artifact_refs"] == []
    assert messages[0]["structured"]["verification"]["checks"] == report["checks"]
    assert not [task for task in s.list("task", rid) if task["kind"] in {"line", "boss"}]  # The original serial line graph owns revision.
    count = len(s.events(rid))
    assert s.dispatch_outbox(ctrl)["delivered"] == []
    assert len(s.events(rid)) == count
    consumed = s.consume_inbox(rid, "line-01/exploration", "revision-1:exploration-inbox", "cycle-1", ctrl)
    assert len(consumed) == 1
    assert consumed[0]["verification"]["checks"] == report["checks"]
    assert consumed[0]["message"]["message_id"] == messages[0]["message_id"]
    count = len(s.events(rid))
    assert s.consume_inbox(rid, "line-01/exploration", "revision-1:exploration-inbox", "cycle-1", ctrl) == consumed
    assert len(s.events(rid)) == count
    assert s.consume_inbox(rid, "line-01/exploration", "other-consumer", "cycle-1", ctrl) == []
    assert len(s.consume_inbox(rid, "line-01/critic", "revision-1:critic-inbox", "cycle-1", ctrl)) == 1
    assert all(item["status"] == "acknowledged" for item in s.list("message_delivery", rid))


def test_boss_handoff_crash_before_ack_reuses_message_and_one_task(business, monkeypatch):
    s, rid, _ = business
    s.update_line(rid, "line-01", cycle_id="cycle-1")
    ctrl = ConfirmingControl()
    sid, report = submit(s, rid, 1, passed=True)
    oid = s.commit_verification("cycle-1", sid, report, 1, ctrl)["operation_id"]
    original = s._put
    def fail_ack(c, kind, identity, value, research_id=None):
        if kind == "inbox":
            raise OSError("Injected acknowledgement failure after the message commit")
        return original(c, kind, identity, value, research_id)
    monkeypatch.setattr(s, "_put", fail_ack)
    assert s.dispatch_outbox(ctrl)["pending"] == [oid]
    message = s.messages("cycle-1:feedback")["items"][0]
    assert not s.list("inbox", rid) and not s.list("message_delivery", rid) and not s.get("task", sid + ":boss")
    assert s.get("outbox", oid)["status"] == "pending"
    monkeypatch.setattr(s, "_put", original)
    assert Store(s.data_dir).dispatch_outbox(ctrl)["delivered"] == [oid]
    assert s.messages("cycle-1:feedback")["items"] == [message]
    assert len(s.list("inbox", rid)) == len(s.list("message_delivery", rid)) == 1
    task = s.get("task", sid + ":boss")
    assert task["kind"] == "boss" and task["status"] == "pending"
    assert task["payload"] == {"line_id": "line-01", "cycle_id": "cycle-1", "submission_id": sid}
    assert s.lines(rid)[0]["phase"] == "waiting_boss"
    assert s.dispatch_outbox(ctrl)["delivered"] == []
    assert len([task for task in s.list("task", rid) if task["kind"] == "boss"]) == 1
    assert len([event for event in s.events(rid) if event["type"] == "message.completed"]) == 1


@pytest.mark.parametrize("stop_stage", ["before_message", "after_message", "before_task_commit"])
def test_outbox_stop_fences_cancel_without_recipient_receipts_or_task(business, monkeypatch, stop_stage):
    s, rid, _ = business
    ctrl = ConfirmingControl()
    sid, report = submit(s, rid, 1, passed=True)
    oid = s.commit_verification("cycle-1", sid, report, 1, ctrl)["operation_id"]
    if stop_stage == "before_message":
        ctrl.stopped = True
    elif stop_stage == "after_message":
        original = s.create_message
        def message_then_stop(*args, **kwargs):
            result = original(*args, **kwargs)
            ctrl.stopped = True
            return result
        monkeypatch.setattr(s, "create_message", message_then_stop)
    else:
        original = s._put
        def stop_during_ack(c, kind, identity, value, research_id=None):
            result = original(c, kind, identity, value, research_id)
            if kind == "message_delivery":
                ctrl.stopped = True
            return result
        monkeypatch.setattr(s, "_put", stop_during_ack)
    assert s.dispatch_outbox(ctrl)["cancelled"] == [oid]
    assert s.get("outbox", oid)["status"] == "cancelled"
    assert not s.get("task", sid + ":boss") and not s.list("inbox", rid) and not s.list("message_delivery", rid)
    assert len(s.messages("cycle-1:feedback")["items"]) == (stop_stage != "before_message")
    assert not [event for event in s.events(rid) if event["type"] in {"handoff.accepted", "message.delivered"}]


@pytest.mark.parametrize("changes", [{"control_generation": 2}, {"deadline_at": "2000-01-01T00:00:00Z"}, {"status": "completed"}])
def test_stale_outbox_is_cancelled_before_delivery(business, changes):
    s, rid, _ = business
    ctrl = ConfirmingControl()
    sid, report = submit(s, rid, 1, passed=True)
    oid = s.commit_verification("cycle-1", sid, report, 1, ctrl)["operation_id"]
    s.update_project(rid, **changes)
    assert s.dispatch_outbox(ctrl)["cancelled"] == [oid]
    assert not s.messages("cycle-1:feedback")["items"] and not s.get("task", sid + ":boss")


def test_bounded_outbox_rotates_unavailable_projects_and_isolates_ack_failure(tmp_path, monkeypatch):
    s, ctrl, projects = Store(tmp_path), ConfirmingControl(), []
    for number in range(3):
        p = s.create_project({"topic": f"Project {number}", "client_request_id": str(number)})
        rid = p["id"]
        s.start_project(rid)
        cycle_id, sid = f"fair-cycle-{number}", f"fair-submission-{number}"
        s.put("cycle", cycle_id, {"line_id": "line-01", "k": 3, "status": "verifying", "completed_verify_rounds": 0}, rid)
        s.put("submission", sid, {"cycle_id": cycle_id}, rid)
        transition = s.commit_verification(cycle_id, sid, {"passed": True, "checks": ["Actual saved evidence"]}, 1, ctrl)
        projects.append((rid, transition["operation_id"]))
    # A paused project remains queued. A different project's acknowledgement
    # failure must not abort delivery to the healthy one in the same pass.
    s.update_project(projects[0][0], status="paused")
    original = s._put
    def fail_one(c, kind, identity, value, research_id=None):
        if kind == "inbox" and research_id == projects[1][0]:
            raise OSError("One destination's transaction failed")
        return original(c, kind, identity, value, research_id)
    monkeypatch.setattr(s, "_put", fail_one)
    first = s.dispatch_outbox(ctrl, limit=1)
    assert first["pending"] == [projects[0][1]]
    second = s.dispatch_outbox(ctrl, limit=2)
    assert second["pending"] == [projects[1][1]]
    assert second["delivered"] == [projects[2][1]]
    monkeypatch.setattr(s, "_put", original)
    s.update_project(projects[0][0], status="running")
    assert set(s.dispatch_outbox(ctrl)["delivered"]) == {projects[0][1], projects[1][1]}


def test_unavailable_control_cannot_replace_existing_handoff_with_empty_context(business, monkeypatch):
    s, rid, _ = business
    ctrl = ConfirmingControl()
    sid, report = submit(s, rid, 1)
    oid = s.commit_verification("cycle-1", sid, report, 1, ctrl)["operation_id"]
    original = ctrl.allows_progress
    monkeypatch.setattr(ctrl, "allows_progress", lambda *args: False)
    assert s.dispatch_outbox(ctrl)["pending"] == [oid]
    assert not s.messages("cycle-1:feedback")["items"]
    monkeypatch.setattr(ctrl, "allows_progress", original)
    assert s.dispatch_outbox(ctrl)["delivered"] == [oid]
    monkeypatch.setattr(ctrl, "allows_progress", lambda *args: False)
    with pytest.raises(StoreError, match="控制门禁暂不可用"):
        s.consume_inbox(rid, "line-01/exploration", "stable-consumer", "cycle-1", ctrl)
    assert all(item["status"] == "delivered" for item in s.list("inbox", rid))
    monkeypatch.setattr(ctrl, "allows_progress", original)
    assert len(s.consume_inbox(rid, "line-01/exploration", "stable-consumer", "cycle-1", ctrl)) == 1


def test_failed_outbox_does_not_starve_other_line_in_same_project(business, monkeypatch):
    s, rid, _ = business
    ctrl = ConfirmingControl()
    sid, report = submit(s, rid, 1, passed=True)
    first = s.commit_verification("cycle-1", sid, report, 1, ctrl)["operation_id"]
    s.put("cycle", "cycle-2", {"line_id": "line-02", "k": 3, "status": "verifying", "completed_verify_rounds": 0}, rid)
    s.put("submission", "submission-other-line", {"cycle_id": "cycle-2"}, rid)
    second = s.commit_verification("cycle-2", "submission-other-line", {"passed": True, "checks": ["Saved evidence"]}, 1, ctrl)["operation_id"]
    original = s._put
    def fail_first(c, kind, identity, value, research_id=None):
        if kind == "inbox" and value.get("outbox_id") == first:
            raise OSError("One line's acknowledgement is unavailable")
        return original(c, kind, identity, value, research_id)
    monkeypatch.setattr(s, "_put", fail_first)
    assert s.dispatch_outbox(ctrl, limit=1)["pending"] == [first]
    assert s.dispatch_outbox(ctrl, limit=1)["delivered"] == [second]
