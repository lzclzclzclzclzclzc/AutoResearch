import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from backend.adapters.dsh import ResearchStopped, RuntimeBlocked
from backend.graphs.arbitration import ArbitrationService
from backend.persistence import Store
from packages.contracts.research import Verification, digest


class Control:
    stopped = False

    def allows_progress(self, *args):
        return not self.stopped


class Engine:
    def __init__(self, store, callback=None):
        self.store = store
        self.control = Control()
        self.callback = callback
        self.calls = []

    async def guard(self, research_id):
        project = self.store.project(research_id)
        if project["status"] in {"paused", "pausing", "blocked"}:
            raise RuntimeBlocked("research_paused", "paused")
        if project["status"] in {"terminated", "terminating", "completed", "quarantined"} or self.control.stopped:
            raise ResearchStopped("stopped")
        return project

    async def execute_god_turn(self, request, context, attempt, **kwargs):
        self.calls.append({"id": request["id"], "attempt": attempt, "context": context})
        if self.callback:
            return await self.callback(request, context, attempt)
        return choose(request, context)

    def validate_output(self, research_id, agent_id, schema, output, context, validator=None):
        value = schema.model_validate(output).model_dump()
        if validator:
            validator(value)
        return value


def choose(request, context):
    return {"arbitration_id": request["id"], "snapshot_hash": request["snapshot_hash"],
        "action": "select_candidate", "selected_candidate_id": sorted(context["candidates"])[0],
        "content_markdown": "采用已有候选", "rationale": "约束等价，保留原票"}


@pytest.fixture
def fixture(tmp_path):
    store = Store(tmp_path)
    project = store.create_project({"topic": "God protocol fixture", "client_request_id": "fixture"})
    project = store.start_project(project["id"])
    return store, project


def arguments(project, scope="discussion-a", **changes):
    return {"research_id": project["id"], "source_task_id": "root-task", "scope_kind": "discussion",
        "scope_id": scope, "reason_code": "decision_blocked", "origin_node": "discussion.collect",
        "context": {"role": "boss", "candidates": {scope + "/member-01": {"objective": "same constraints"}},
                    "votes": {"member-01": "member-01", "member-02": "member-02"}, "blockers": []},
        "allowed_actions": ["select_candidate", "request_revision", "needs_external_input"], **changes}


async def eventually(check):
    for _ in range(100):
        if check():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("Expected asynchronous state did not arrive")


async def test_different_groups_enter_god_concurrently_with_isolated_snapshots(fixture):
    store, project = fixture
    entered, release = set(), asyncio.Event()
    async def callback(request, context, attempt):
        entered.add(request["id"])
        if len(entered) == 2:
            release.set()
        await asyncio.wait_for(release.wait(), 2)
        return choose(request, context)
    engine = Engine(store, callback)
    service = ArbitrationService(engine)
    first, second = await asyncio.gather(service.resolve(**arguments(project, "group-a")),
                                         service.resolve(**arguments(project, "group-b")))
    assert first["arbitration_id"] != second["arbitration_id"]
    assert first["selected_candidate_id"].startswith("group-a/")
    assert second["selected_candidate_id"].startswith("group-b/")
    requests = store.list("arbitration_request", project["id"])
    assert len({r["snapshot_id"] for r in requests}) == 2
    assert all(r["status"] == "decided" for r in requests)


async def test_two_services_same_scene_share_one_judgment_and_effect(fixture):
    store, project = fixture
    entered, release = asyncio.Event(), asyncio.Event()
    async def callback(request, context, attempt):
        entered.set()
        await release.wait()
        return choose(request, context)
    engine = Engine(store, callback)
    first, second = ArbitrationService(engine), ArbitrationService(engine)
    a = asyncio.create_task(first.resolve(**arguments(project)))
    await entered.wait()
    b = asyncio.create_task(second.resolve(**arguments(project)))
    await asyncio.sleep(0.15)
    release.set()
    results = await asyncio.wait_for(asyncio.gather(a, b), 3)
    assert results[0] == results[1]
    assert len(engine.calls) == 1
    effects = await asyncio.gather(first.apply(results[0]["arbitration_id"], results[0]),
                                   second.apply(results[1]["arbitration_id"], results[1]))
    assert effects[0] == effects[1]
    assert len(store.list("arbitration_effect", project["id"])) == 1


async def test_recovery_after_decided_never_calls_model_again(fixture):
    store, project = fixture
    engine = Engine(store)
    before = ArbitrationService(engine)
    decision = await before.resolve(**arguments(project))
    restarted = ArbitrationService(engine)
    assert await restarted.resolve(**arguments(project)) == decision
    await restarted.apply(decision["arbitration_id"], decision)
    assert await restarted.resolve(**arguments(project)) == decision
    assert len(engine.calls) == 1


async def test_explicit_external_retry_gets_new_arbitration_without_rewriting_old_request(fixture):
    from backend.graphs.tasks import current_task
    store, project = fixture
    supplied = False
    async def callback(request, context, attempt):
        if supplied:
            return choose(request, context)
        return {"arbitration_id": request["id"], "snapshot_hash": request["snapshot_hash"],
            "action": "needs_external_input", "content_markdown": "missing input", "rationale": "needs data",
            "missing_items": ["dataset"], "why_unavailable": "not supplied", "operation": "supply and retry"}
    engine = Engine(store, callback)
    service = ArbitrationService(engine)
    root = store.add_task(project["id"], "paper", {}, "root-task")
    token = current_task.set(root)
    try:
        with pytest.raises(RuntimeBlocked, match="missing input"):
            await service.resolve(**arguments(project))
    finally:
        current_task.reset(token)
    before = store.list("arbitration_request", project["id"])[0]
    store.finish_task(root["id"], error={"code": "arbitration_needs_external_input"})
    supplied = True
    store.retry_failed_tasks(project["id"])
    token = current_task.set(store.get("task", root["id"]))
    try:
        decision = await service.resolve(**arguments(project))
    finally:
        current_task.reset(token)
    assert decision["arbitration_id"] != before["id"]
    assert store.get("arbitration_request", before["id"])["status"] == "needs_external_input"
    after = store.get("arbitration_request", decision["arbitration_id"])
    assert after["snapshot_hash"] != before["snapshot_hash"] and after["recovery_chain_id"] != before["recovery_chain_id"]
    assert len(engine.calls) == 2


async def test_god_cannot_select_through_must_fix_blockers(fixture):
    store, project = fixture
    engine = Engine(store)
    service = ArbitrationService(engine)
    args = arguments(project)
    args["context"]["blockers"] = [{"issue_id": "must-fix-1", "category": "must_fix", "resolved": False}]
    with pytest.raises(RuntimeBlocked) as error:
        await service.resolve(**args)
    assert error.value.code == "arbitration_exhausted"
    assert len(engine.calls) == 3
    assert not store.list("arbitration_decision", project["id"])


async def test_stop_during_god_archives_late_response_without_applying(fixture):
    store, project = fixture
    async def callback(request, context, attempt):
        store.update_project(project["id"], status="terminated")
        return choose(request, context)
    service = ArbitrationService(Engine(store, callback))
    with pytest.raises(ResearchStopped):
        await service.resolve(**arguments(project))
    request = store.list("arbitration_request", project["id"])[0]
    assert request["status"] == "cancelled"
    assert not store.list("arbitration_effect", project["id"])


async def test_scope_change_during_god_stays_stale(fixture):
    store, project = fixture
    async def callback(request, context, attempt):
        store.put("arbitration_scope", digest([project["id"], request["scope_kind"], request["scope_id"]]),
            {"scope_revision": "new-version"}, project["id"])
        return choose(request, context)
    service = ArbitrationService(Engine(store, callback))
    with pytest.raises(ResearchStopped):
        await service.resolve(**arguments(project))
    assert store.list("arbitration_request", project["id"])[0]["status"] == "stale"
    assert not store.list("arbitration_decision", project["id"])


async def test_unrelated_line_progress_does_not_expire_scope(fixture):
    store, project = fixture
    async def callback(request, context, attempt):
        store.update_project(project["id"], memory_version=10)
        store.update_line(project["id"], "line-02", phase="verify")
        return choose(request, context)
    engine = Engine(store, callback)
    decision = await ArbitrationService(engine).resolve(**arguments(project))
    assert store.get("arbitration_request", decision["arbitration_id"])["status"] == "decided"


async def test_pause_before_actual_generation_preserves_retry_without_spending_attempt(fixture):
    store, project = fixture
    async def callback(request, context, attempt):
        store.update_project(project["id"], status="paused")
        raise RuntimeBlocked("research_paused", "paused during admission")
    engine = Engine(store, callback)
    with pytest.raises(RuntimeBlocked) as error:
        await ArbitrationService(engine).resolve(**arguments(project))
    assert error.value.code == "research_paused"
    request = store.list("arbitration_request", project["id"])[0]
    assert request["status"] == "retry_wait"
    assert request["generation_attempts"] == 0
    original_deadline = request["judge_deadline_at"]
    store.update_project(project["id"], status="running")
    engine.callback = None
    result = await ArbitrationService(engine).resolve(**arguments(project))
    updated = store.get("arbitration_request", result["arbitration_id"])
    assert updated["generation_attempts"] == 1
    assert updated["judge_deadline_at"] == original_deadline


def test_verification_repair_requires_complete_unchanged_verify_facts(fixture):
    store, project = fixture
    engine = Engine(store)
    service = ArbitrationService(engine)
    request = {"id": "arb-v", "research_id": project["id"], "snapshot_hash": "hash", "scope_kind": "turn_output",
        "allowed_actions": ["repair_output"], "target_schema_hash": digest(Verification.model_json_schema())}
    output = {"content_markdown": "Observed valid negative result", "passed": True, "scientific_observation": "negative",
        "evidence": [], "limitations": ["single run"], "required_changes": []}
    decision = {"arbitration_id": "arb-v", "snapshot_hash": "hash", "action": "repair_output", "output": output,
        "target_schema_hash": request["target_schema_hash"], "content_markdown": "format repaired", "rationale": "format only"}
    with pytest.raises(ValueError, match="complete verification facts"):
        service.validate_decision(request, {"role": "verify", "diagnostic": {}}, decision, Verification)
    context = {"role": "verify", "diagnostic": {"source_output": output}}
    service.validate_decision(request, context, decision, Verification)
    with pytest.raises(ValueError, match="cannot change verification facts"):
        service.validate_decision(request, context, {**decision, "output": {**output, "passed": False}}, Verification)
    with pytest.raises(ValueError, match="cannot change verification facts"):
        service.validate_decision(request, context, {**decision, "output": {**output, "required_changes": ["invented change"]}}, Verification)


def test_revision_cannot_expand_role_and_evidence_refs_are_snapshot_scoped(fixture):
    store, project = fixture
    service = ArbitrationService(Engine(store))
    request = {"id": "arb-v", "snapshot_hash": "hash", "scope_kind": "turn_output",
        "allowed_actions": ["request_revision", "request_evidence"]}
    decision = {"arbitration_id": "arb-v", "snapshot_hash": "hash", "action": "request_revision", "target_role": "boss"}
    with pytest.raises(ValueError, match="role permissions"):
        service.validate_decision(request, {"role": "implementation"}, decision)
    with pytest.raises(ValueError, match="outside"):
        service.validate_decision(request, {"role": "implementation"}, {**decision,
            "action": "request_evidence", "target_role": "implementation", "source_refs": ["other-project-artifact"]})


def test_only_advertised_critic_discussion_can_request_same_line_exploration_revision(fixture):
    store, project = fixture
    service = ArbitrationService(Engine(store))
    request = {"id": "arb-route", "snapshot_hash": "hash", "scope_kind": "discussion",
        "reason_code": "design_deadlock", "line_id": "line-01", "allowed_actions": ["request_revision", "request_evidence", "select_candidate"]}
    context = {"role": "critic", "task": {"exploration": {"selected_payload": {"method": "old"}}},
        "revision_routes": {"exploration": "revise_exploration"}, "blockers": [{"issue_id": "cmf-004"}],
        "candidates": {"member": {}}}
    decision = {"arbitration_id": request["id"], "snapshot_hash": "hash", "action": "request_revision", "target_role": "exploration"}
    service.validate_decision(request, context, decision)
    for target in ("implementation", "boss", "verify"):
        with pytest.raises(ValueError, match="role permissions"):
            service.validate_decision(request, context, {**decision, "target_role": target})
    with pytest.raises(ValueError, match="role permissions"):
        service.validate_decision(request, {**context, "revision_routes": {}}, decision)
    with pytest.raises(ValueError, match="role permissions"):
        service.validate_decision({**request, "scope_kind": "turn_output"}, context, decision)
    with pytest.raises(ValueError, match="role permissions"):
        service.validate_decision({**request, "reason_code": "invalid_group_decision"}, context, decision)
    with pytest.raises(ValueError, match="role permissions"):
        service.validate_decision(request, context, {**decision, "action": "request_evidence"})
    with pytest.raises(ValueError, match="unresolved blockers"):
        service.validate_decision(request, context, {**decision, "action": "select_candidate", "selected_candidate_id": "member"})


async def test_hard_restart_reconciles_unexpired_running_god_lease(fixture):
    from backend.runtime.worker import recover_arbitration_leases
    store, project = fixture
    class HardCrash(BaseException):
        pass
    async def crash(request, context, attempt):
        raise HardCrash()
    engine = Engine(store, crash)
    with pytest.raises(HardCrash):
        await ArbitrationService(engine).resolve(**arguments(project))
    before = store.list("arbitration_request", project["id"])[0]
    assert before["status"] == "running"
    assert datetime.fromisoformat(before["lease_expires_at"].replace("Z", "+00:00")) > datetime.now(timezone.utc)
    await recover_arbitration_leases(store, engine.control)
    engine.callback = None
    decision = await asyncio.wait_for(ArbitrationService(engine).resolve(**arguments(project)), 1)
    after = store.get("arbitration_request", decision["arbitration_id"])
    assert after["status"] == "decided"
    assert after["lease_epoch"] == before["lease_epoch"] + 1
    assert after["generation_attempts"] == before["generation_attempts"] + 1
    assert after["judge_deadline_at"] == before["judge_deadline_at"]


@pytest.mark.parametrize("generation", [1, 3])
async def test_restart_reuses_complete_god_response_before_decision_commit(fixture, generation):
    from backend.runtime.worker import recover_arbitration_leases
    store, project = fixture
    class HardCrash(BaseException):
        pass
    async def response_then_crash(request, context, attempt):
        # Inject the ledger state at the crash boundary, including the final
        # permitted generation; no fourth model call may be needed to recover.
        store.patch("arbitration_request", request["id"], attempt=generation, generation_attempts=generation)
        store.put("execution", "completed-god-execution", {"agent_id": "god/" + request["id"],
            "turn_id": f"judge:{generation}", "status": "completed", "execution_attempt_id": "source-god-execution",
            "completed_at": datetime.now(timezone.utc).isoformat(), "output": choose(request, context)}, project["id"])
        raise HardCrash()
    engine = Engine(store, response_then_crash)
    with pytest.raises(HardCrash):
        await ArbitrationService(engine).resolve(**arguments(project))
    before = store.list("arbitration_request", project["id"])[0]
    if generation == 3:
        completed_at = store.get("execution", "completed-god-execution")["completed_at"]
        store.patch("arbitration_request", before["id"], judge_deadline_at=completed_at)
        before = store.get("arbitration_request", before["id"])
    await recover_arbitration_leases(store, engine.control)
    engine.callback = None
    result = await ArbitrationService(engine).resolve(**arguments(project))
    after = store.get("arbitration_request", result["arbitration_id"])
    assert after["status"] == "decided"
    assert len(engine.calls) == 1, "A complete durable God response must be validated and reused before creating a new generation"
    assert after["generation_attempts"] == before["generation_attempts"]
    assert after["judge_deadline_at"] == before["judge_deadline_at"]


async def test_finish_scope_commits_god_effect_without_cancelling_its_own_request(fixture, monkeypatch):
    from backend.graphs.engine import ResearchEngine
    store, project = fixture
    class ConfirmingControl(Control):
        def confirm_operation(self, research_id, operation_id, content_hash, control_generation=1):
            return {"research_id": research_id, "operation_id": operation_id, "content_hash": content_hash,
                "control_generation": control_generation, "persisted": True, "control_epoch": "fixture", "control_seq": 1}
    async def finish(request, context, attempt):
        return {"arbitration_id": request["id"], "snapshot_hash": request["snapshot_hash"], "action": "finish_scope",
            "line_id": "line-01", "content_markdown": "封存当前方向", "rationale": "冻结方案无法在现有条件下实现"}
    fake = Engine(store, finish)
    fake.control = ConfirmingControl()
    service = ArbitrationService(fake)
    args = arguments(project, "implementation-cycle", scope_kind="implementation", origin_node="line.implement",
        line_id="line-01", context={"role": "implementation", "spec": {}}, allowed_actions=["finish_scope"])
    decision = await service.resolve(**args)
    request = store.get("arbitration_request", decision["arbitration_id"])
    engine = ResearchEngine.__new__(ResearchEngine)
    engine.store, engine.control, engine.arbitration = store, fake.control, service
    original_put = store._put
    def fail_scope_write(c, kind, identity, value, research_id=None):
        if kind == "line" and value.get("finish_requested"):
            raise OSError("Injected crash after the God effect but before the line projection")
        return original_put(c, kind, identity, value, research_id)
    monkeypatch.setattr(store, "_put", fail_scope_write)
    with pytest.raises(OSError, match="Injected crash"):
        await engine.recover_action(request, decision, context={}, role="implementation", line_id="line-01")
    assert store.get("arbitration_request", request["id"])["status"] == "decided"
    assert not store.list("arbitration_effect", project["id"])
    assert not store.get("line", project["id"] + ":line-01").get("finish_requested")
    monkeypatch.setattr(store, "_put", original_put)
    with pytest.raises(RuntimeBlocked) as error:
        await engine.recover_action(request, decision, context={}, role="implementation", line_id="line-01")
    assert error.value.code == "line_finished"
    assert store.get("line", project["id"] + ":line-01")["finish_requested"]
    assert store.get("arbitration_request", request["id"])["status"] == "applied"
    effects = store.list("arbitration_effect", project["id"])
    assert len(effects) == 1 and effects[0]["effect_kind"] == "finish_scope"
    assert effects[0]["control_receipt"]["persisted"]
    with pytest.raises(RuntimeBlocked) as replay:
        await engine.recover_action(request, decision, context={}, role="implementation", line_id="line-01")
    assert replay.value.code == "line_finished"
    assert store.list("arbitration_effect", project["id"]) == effects
    assert len([event for event in store.events(project["id"]) if event["type"] == "arbitration.applied"]) == 1
