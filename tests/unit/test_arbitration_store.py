from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from backend.persistence import Store, StoreError
from packages.contracts.research import DecompositionPlan, GodDecision, digest


@pytest.fixture
def records(tmp_path):
    store = Store(tmp_path)
    project = store.create_project({"topic": "Arbitration fixture", "client_request_id": "arb-test"})
    project = store.start_project(project["id"])
    sections = {"candidates": {"boss/member-01": {"rationale": "same constraints"}}, "votes": {"a": "a", "b": "b"}}
    current = datetime.now(timezone.utc)
    request = {"arbitration_id": "arb-1", "research_id": project["id"], "source_task_id": "task-root",
        "scope_kind": "discussion", "scope_id": "boss-discussion", "origin_node": "discussion.collect",
        "reason_code": "decision_blocked", "failure_fingerprint": digest(sections), "scope_revision": "revision-1",
        "direction_epoch": None, "control_generation": project["control_generation"], "snapshot_id": "snapshot-1",
        "snapshot_hash": digest(sections), "target_schema_name": "Synthesis", "target_schema_hash": "schema-hash",
        "allowed_actions": ["select_candidate", "needs_external_input"], "resume_point": "discussion.apply_resolution",
        "provider_config_id": project["provider_config_id"], "credential_version": 0, "model": "fixture-model",
        "recovery_chain_id": "chain-1", "created_at": current.isoformat(),
        "judge_deadline_at": (current + timedelta(seconds=600)).isoformat(), "expires_at": project["deadline_at"]}
    return store, request, {"sections": sections, "snapshot_hash": digest(sections)}


def decision(request):
    return {"arbitration_id": request["arbitration_id"], "snapshot_hash": request["snapshot_hash"],
        "action": "select_candidate", "content_markdown": "采用已有候选，保留真实票型。", "rationale": "两项执行约束等价",
        "selected_candidate_id": "boss/member-01"}


def decided(records):
    store, request, snapshot = records
    store.create_arbitration(request, snapshot)
    claim = store.claim_arbitration(request["arbitration_id"], "worker-1")
    store.transition_arbitration(claim["id"], "running", owner="worker-1", lease_epoch=claim["lease_epoch"], generation_attempts=1)
    result = store.decide_arbitration(claim["id"], decision(request), owner="worker-1", lease_epoch=claim["lease_epoch"])
    return store, result


def test_same_scope_concurrent_capture_creates_one_request_snapshot_and_round(records):
    store, request, snapshot = records
    def capture(index):
        return store.create_arbitration({**request, "arbitration_id": f"arb-{index}"}, snapshot)
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(capture, range(8)))
    assert len({item["id"] for item in results}) == 1
    assert len(store.list("arbitration_snapshot", request["research_id"])) == 1
    assert store.get("recovery_chain", "chain-1")["rounds"] == 1


def test_snapshot_cannot_be_modified_or_forged(records):
    store, request, snapshot = records
    with pytest.raises(StoreError, match="哈希"):
        store.create_arbitration(request, {"sections": {"forged": True}})
    store.create_arbitration(request, snapshot)
    with pytest.raises(StoreError, match="不可修改"):
        store.patch("arbitration_snapshot", request["snapshot_id"], sections={"forged": True})


def test_model_action_union_is_flat_and_disallows_unrelated_fields(records):
    _, request, _ = records
    value = GodDecision.model_validate(decision(request)).model_dump()
    assert value["action"] == "select_candidate"
    assert "root" not in value
    with pytest.raises(ValidationError):
        GodDecision.model_validate({**decision(request), "output": {"score": 100}})


def test_expired_lease_requires_exit_reconciliation(records):
    store, request, snapshot = records
    store.create_arbitration(request, snapshot)
    first = store.claim_arbitration(request["arbitration_id"], "worker-1")
    store.patch("arbitration_request", first["id"], lease_expires_at="2000-01-01T00:00:00Z")
    assert store.claim_arbitration(first["id"], "worker-2") is None
    second = store.claim_arbitration(first["id"], "worker-2", processes_reconciled=True)
    assert second["lease_epoch"] == first["lease_epoch"] + 1
    with pytest.raises(StoreError, match="认领|代次"):
        store.decide_arbitration(first["id"], decision(request), owner="worker-1", lease_epoch=first["lease_epoch"])


def test_decision_and_effect_have_separate_events_and_idempotent_resume(records):
    store, request = decided(records)
    assert request["status"] == "decided"
    assert not store.list("arbitration_effect", request["research_id"])
    effect = store.apply_arbitration_effect(request["id"], "resume", {"selected_candidate_id": "boss/member-01"})
    assert store.apply_arbitration_effect(request["id"], "resume", effect["payload"]) == effect
    assert len(store.list("arbitration_effect", request["research_id"])) == 1
    updated = store.get("arbitration_request", request["id"])
    assert updated["status"] == "applied"
    assert updated["decided_event_seq"] < updated["applied_event_seq"]
    assert snapshot_votes(store, request) == {"a": "a", "b": "b"}
    with pytest.raises(StoreError, match="不同载荷"):
        store.apply_arbitration_effect(request["id"], "resume", {"selected_candidate_id": "other"})


def snapshot_votes(store, request):
    return store.get("arbitration_snapshot", request["snapshot_id"])["sections"]["votes"]


def test_decided_response_can_apply_after_judge_deadline(records):
    store, request = decided(records)
    store.patch("arbitration_request", request["id"], judge_deadline_at="2000-01-01T00:00:00Z")
    assert store.apply_arbitration_effect(request["id"], "resume", {"accepted": True})["payload"]["accepted"]


def test_pause_retains_decision_and_stop_prevents_late_effect(records):
    store, request = decided(records)
    store.update_project(request["research_id"], status="paused")
    with pytest.raises(StoreError, match="暂停"):
        store.apply_arbitration_effect(request["id"], "resume", {})
    assert store.get("arbitration_request", request["id"])["status"] == "decided"
    store.update_project(request["research_id"], status="terminated")
    with pytest.raises(StoreError, match="停止"):
        store.apply_arbitration_effect(request["id"], "resume", {})
    assert not store.list("arbitration_effect", request["research_id"])


def test_invalid_transition_and_scope_change_do_not_apply(records):
    store, request, snapshot = records
    store.create_arbitration(request, snapshot)
    with pytest.raises(StoreError, match="非法仲裁状态"):
        store.transition_arbitration(request["arbitration_id"], "applied")
    store, request = decided(records)
    with pytest.raises(StoreError, match="修订"):
        store.apply_arbitration_effect(request["id"], "resume", {}, expected_scope_revision="other-revision")


def test_recovery_chain_budget_survives_new_versions(records):
    store, request, snapshot = records
    for index in range(3):
        store.create_arbitration({**request, "arbitration_id": f"arb-{index}", "snapshot_id": f"snapshot-{index}",
            "scope_revision": f"revision-{index}"}, snapshot)
    with pytest.raises(StoreError, match="三轮"):
        store.create_arbitration({**request, "arbitration_id": "arb-extra", "snapshot_id": "snapshot-extra",
            "scope_revision": "revision-extra"}, snapshot)
    assert store.get("recovery_chain", "chain-1")["rounds"] == 3


def test_retry_backoff_and_attempt_count_persist_across_store_restart(records):
    store, request, snapshot = records
    store.create_arbitration(request, snapshot)
    first = store.claim_arbitration(request["arbitration_id"], "worker-1")
    retry_at = (datetime.now(timezone.utc) + timedelta(seconds=60)).isoformat()
    store.transition_arbitration(first["id"], "retry_wait", owner="worker-1", lease_epoch=first["lease_epoch"],
        generation_attempts=2, retry_not_before=retry_at, diagnostic="format error")
    restarted = Store(store.data_dir)
    assert restarted.claim_arbitration(first["id"], "worker-2", processes_reconciled=True) is None
    assert restarted.get("arbitration_request", first["id"])["generation_attempts"] == 2
    assert restarted.get("arbitration_request", first["id"])["retry_not_before"] == retry_at


def test_resume_outbox_wakes_matching_parent_once(records):
    store, request, _ = records
    task = store.add_task(request["research_id"], "line", {}, "task-root")
    store.claim_task(task["id"], "worker-1")
    store.suspend_recovery_task(task["id"], "token-1", arbitration_id="arb-1", graph_thread_id="thread-1",
        waiting_node="line.implement", recovery_chain_id="chain-1")
    outcome = {"status": "accepted", "result_ref": "result-1"}
    first = store.enqueue_recovery_resume(task["id"], "token-1", outcome)
    assert store.enqueue_recovery_resume(task["id"], "token-1", outcome) == first
    assert store.dispatch_recovery_outbox() == [task["id"]]
    assert store.dispatch_recovery_outbox() == []
    assert store.get("task", task["id"])["status"] == "pending"
    assert store.get("task", task["id"])["recovery_outcome"] == outcome


def plan(parts):
    return {"target_schema_hash": "hash", "parts": parts,
        "merge_plan": {"strategy": "json_fields", "order": [p["part_id"] for p in parts]}, "acceptance_checks": ["original schema"]}


def part(identity, depends=(), path=None):
    return {"part_id": identity, "target_role": "paper", "instructions": "Write evidence-backed section",
        "output_schema": {"type": "object"}, "allowed_write_paths": [path or f"/section_{identity}"], "depends_on": list(depends)}


@pytest.mark.parametrize("parts", [
    [part("a", ["b"]), part("b", ["a"])],
    [part("a"), part("b", ["a"]), part("c", ["b"]), part("d", ["c"])],
    [part("a", path="/files/a.py"), part("b", path="/files/a.py")],
    [part("a", path="/fixed")],
    [part("a", path="/files"), part("b", path="/files/a.py")],
])
def test_decomposition_rejects_cycles_depth_collisions_and_fixed_field_writes(parts):
    with pytest.raises(ValidationError):
        DecompositionPlan.model_validate(plan(parts))


def test_decomposition_accepts_bounded_independent_sections():
    assert len(DecompositionPlan.model_validate(plan([part("a"), part("b")])).parts) == 2


def test_scope_revision_is_rechecked_in_effect_transaction(records):
    store, request = decided(records)
    key = digest([request["research_id"], request["scope_kind"], request["scope_id"]])
    store.put("arbitration_scope", key, {"scope_revision": "new-version"}, request["research_id"])
    with pytest.raises(StoreError, match="修订"):
        store.apply_arbitration_effect(request["id"], "resume", {})
    assert not store.list("arbitration_effect", request["research_id"])


def test_same_status_update_cannot_rewrite_frozen_request(records):
    store, request, snapshot = records
    store.create_arbitration(request, snapshot)
    with pytest.raises(StoreError, match="冻结身份"):
        store.transition_arbitration(request["arbitration_id"], "requested", scope_revision="forged-version")


def test_direction_closure_cancels_only_its_recovery_work(records):
    store, request, snapshot = records
    rid = request["research_id"]
    for number in (1, 2):
        root = f"root-{number}"
        store.add_task(rid, "line", {"line_id": f"line-{number:02d}"}, root)
        store.patch("task", root, status="waiting_recovery", resume_token=f"token-{number}")
        captured = {**request, "arbitration_id": f"arb-{number}", "line_id": f"line-{number:02d}",
            "direction_epoch": 1, "scope_id": f"scope-{number}", "snapshot_id": f"snap-{number}",
            "source_task_id": root, "recovery_chain_id": f"chain-{number}"}
        store.create_arbitration(captured, snapshot)
        store.put("decomposition_plan", f"plan-{number}", {"root_task_id": root, "arbitration_id": f"arb-{number}",
            "line_id": f"line-{number:02d}", "status": "waiting_children"}, rid)
        store.put("decomposition_part", f"part-{number}", {"root_task_id": root, "plan_id": f"plan-{number}", "status": "pending"}, rid)
        store.put("recovery_resume_outbox", f"outbox-{number}", {"root_task_id": root, "status": "pending"}, rid)
        store.add_task(rid, "decomposition_part", {"line_id": f"line-{number:02d}"}, f"part-task-{number}")
    store.add_task(rid, "paper", {}, "paper-report")
    with store.connect(True) as c:
        store._cancel_recovery(c, rid, "line-01", reason="direction_finished")
    for kind, identity in (("task", "root-1"), ("task", "part-task-1"), ("arbitration_request", "arb-1"),
                           ("decomposition_plan", "plan-1"), ("decomposition_part", "part-1"),
                           ("recovery_resume_outbox", "outbox-1"), ("recovery_chain", "chain-1")):
        assert store.get(kind, identity)["status"] == "cancelled"
    assert store.get("task", "root-2")["status"] == "waiting_recovery"
    assert store.get("arbitration_request", "arb-2")["status"] == "requested"
    assert store.get("decomposition_plan", "plan-2")["status"] == "waiting_children"
    assert store.get("recovery_resume_outbox", "outbox-2")["status"] == "pending"
    assert store.get("task", "paper-report")["status"] == "pending"


def test_sync_stop_cancels_waiting_parent_and_resume_notifications(records):
    store, request, snapshot = records
    rid = request["research_id"]
    store.create_arbitration(request, snapshot)
    store.add_task(rid, "line", {"line_id": "line-01"}, "parent")
    store.patch("task", "parent", status="waiting_recovery", resume_token="token")
    store.put("recovery_wait", "token", {"root_task_id": "parent", "status": "waiting_children"}, rid)
    store.put("recovery_resume_outbox", "wake", {"root_task_id": "parent", "status": "pending"}, rid)
    store.sync_stop(rid, {"stop_id": "stop-1", "stopped": True, "execution_stopped": True})
    assert store.get("task", "parent")["status"] == "cancelled"
    assert store.get("recovery_wait", "token")["status"] == "cancelled"
    assert store.get("recovery_resume_outbox", "wake")["status"] == "cancelled"
    assert store.get("arbitration_request", request["arbitration_id"])["status"] == "cancelled"


def test_third_completed_response_can_be_decided_after_judge_deadline_without_fourth_generation(records):
    store, request, snapshot = records
    current = datetime.now(timezone.utc)
    request = {**request, "created_at": (current - timedelta(seconds=700)).isoformat(),
        "judge_deadline_at": (current - timedelta(seconds=100)).isoformat()}
    store.create_arbitration(request, snapshot)
    output = decision(request)
    marker = {"output": output, "output_hash": digest(output), "source_execution_id": "completed-judge-3",
        "attempt": 3, "completed_at": (current - timedelta(seconds=101)).isoformat()}
    store.patch("arbitration_request", request["arbitration_id"], status="retry_wait", attempt=3,
        generation_attempts=3, recovered_response=marker)
    claimed = store.claim_arbitration(request["arbitration_id"], "new-owner", processes_reconciled=True)
    assert claimed["generation_attempts"] == claimed["attempt"] == 3
    assert claimed["recovered_response"] == marker
    assert datetime.fromisoformat(claimed["lease_expires_at"].replace("Z", "+00:00")) > current
    accepted = store.decide_arbitration(claimed["id"], output, owner="new-owner", lease_epoch=claimed["lease_epoch"])
    assert accepted["status"] == "decided"
    assert store.apply_arbitration_effect(claimed["id"], "resume", {"accepted": True})["payload"]["accepted"]


@pytest.mark.parametrize("corruption", ["late", "hash", "wrong_generation"])
def test_cached_response_cannot_bypass_deadline_with_invalid_proof(records, corruption):
    store, request, snapshot = records
    current = datetime.now(timezone.utc)
    request = {**request, "judge_deadline_at": (current - timedelta(seconds=100)).isoformat()}
    store.create_arbitration(request, snapshot)
    output = decision(request)
    marker = {"output": output, "output_hash": digest(output), "source_execution_id": "completed-judge-3",
        "attempt": 3, "completed_at": (current - timedelta(seconds=101)).isoformat()}
    if corruption == "late":
        marker["completed_at"] = current.isoformat()
    elif corruption == "hash":
        marker["output_hash"] = "forged"
    else:
        marker["attempt"] = 2
    store.patch("arbitration_request", request["arbitration_id"], status="retry_wait", attempt=3,
        generation_attempts=3, recovered_response=marker)
    assert store.claim_arbitration(request["arbitration_id"], "new-owner", processes_reconciled=True) is None
    assert store.get("arbitration_request", request["arbitration_id"])["status"] == "exhausted"
