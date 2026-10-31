from types import SimpleNamespace

import pytest
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from backend.adapters.dsh import ResearchStopped, RuntimeBlocked
from backend.graphs.engine import ResearchEngine
from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor
from backend.runtime.worker import migrate_arbitrable_tasks


@pytest.fixture
async def scene(tmp_path):
    store = Store(tmp_path)
    project = store.start_project(store.create_project({"client_request_id": "god-discussion-edges", "topic": "fixture",
        "mode": "demo", "team": {"m1": 2}, "max_runtime_seconds": 120})["id"])
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(project["id"], project["deadline_at"])
    async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "cp.sqlite")) as saver:
        engine = ResearchEngine(store, control, None, None, saver)
        try:
            yield store, project, engine
        finally:
            await engine.close()
    control.close()


def summary_state(store, project, engine):
    members = engine.members(project, "exploration", "line-01")
    winner = members[0]
    discussion_id = "summary-edge:serial-v2"
    store.create_conversation(project["id"], discussion_id, kind="group_discussion")
    store.put("recovery_chain", "chain-edge", {"status": "recovering"}, project["id"])
    state = {"research_id": project["id"], "discussion_id": discussion_id, "role": "exploration",
        "members": members, "context": {}, "line_id": "line-01", "phase": "vote", "round": 1,
        "winner": winner, "candidates": {winner: {"method": "frozen-original"}},
        "records": {f"vote:1:{member}": {"candidate_id": winner} for member in members},
        "decision_source": "majority", "arbitration_reason": "invalid_group_decision",
        "arbitration_id": "arb-edge", "recovery_chain_id": "chain-edge"}
    return state


async def test_summary_revision_preserves_selected_candidate_and_returns_to_synthesis(scene):
    store, project, engine = scene
    state = summary_state(store, project, engine)
    async def apply(*args, **kwargs):
        return {}
    engine.arbitration.apply = apply
    state["resolution"] = {"action": "request_revision", "target_role": "exploration", "instructions": "仅修复总结",
        "allowed_changes": ["content_markdown"], "acceptance_checks": ["保留既有候选"]}
    result = await engine.discussions.apply_resolution(state)
    assert result.get("winner", state["winner"]) == state["winner"]
    assert result.get("phase", "synthesis") == "synthesis"
    assert result.get("decision_source", state["decision_source"]) == "majority"


async def test_summary_format_repair_keeps_majority_provenance_separate_from_god_authorship(scene):
    store, project, engine = scene
    state = summary_state(store, project, engine)
    async def apply(*args, **kwargs):
        return {}
    engine.arbitration.apply = apply
    state["resolution"] = {"action": "repair_output", "content_markdown": "修复说明", "rationale": "不改变业务选择",
        "output": {"content_markdown": "总结", "selected_candidate_id": state["winner"], "decision": {}, "issues": []}}
    result = await engine.discussions.apply_resolution(state)
    assert result["result"]["decision_source"] == "majority"
    assert result["result"]["selected_payload"] == {"method": "frozen-original"}
    messages = store.messages(state["discussion_id"])["items"]
    assert messages[-1]["sender"]["id"] == "god"
    assert messages[-1]["arbitration_id"] == "arb-edge"


async def test_summary_repair_cannot_bypass_new_unresolved_blocker(scene):
    store, project, engine = scene
    state = summary_state(store, project, engine)
    applied = []
    async def apply(*args, **kwargs):
        applied.append(args)
        return {}
    engine.arbitration.apply = apply
    state["resolution"] = {"action": "repair_output", "content_markdown": "修复说明", "rationale": "保留问题",
        "output": {"content_markdown": "总结", "selected_candidate_id": state["winner"], "decision": {},
            "issues": [{"issue_id": "new-blocker", "category": "must_fix", "description": "核查未完成",
                "resolved": False, "resolution": "", "experiment_mapping": "", "reclassification": None}]}}
    try:
        result = await engine.discussions.apply_resolution(state)
    except RuntimeBlocked:
        assert not applied
    else:
        assert not result.get("result")
        assert not applied


async def test_legacy_serial_v2_collect_checkpoint_resumes_real_votes_without_new_member_turns(scene):
    store, project, engine = scene
    members = engine.members(project, "exploration", "line-01")
    did = "legacy-tie:serial-v2"
    config = {"configurable": {"thread_id": f"research/{project['id']}/discussion/{did}"}}
    votes = {f"vote:2:{member}": {"candidate_id": member, "content_markdown": "真实票", "rationale": "偏好"} for member in members}
    old_state = {"research_id": project["id"], "discussion_id": did, "role": "exploration", "members": members,
        "line_id": "line-01", "context": {}, "phase": "vote", "round": 2, "focused": True,
        "records": votes, "candidates": {member: {"method": member} for member in members}}
    store.create_conversation(project["id"], did, kind="group_discussion")
    await engine.discussions.graph.aupdate_state(config, old_state, as_node="member_turn")
    async def unexpected(**kwargs):
        raise AssertionError("Legacy votes must not be replayed as new member turns")
    engine.demo._produce_turn = unexpected
    async def judge(request, context, attempt, **kwargs):
        return {"arbitration_id": request["id"], "snapshot_hash": request["snapshot_hash"],
            "action": "select_candidate", "selected_candidate_id": members[0], "content_markdown": "继续",
            "rationale": "保留旧票，解决僵局"}
    engine.execute_god_turn = judge
    result = await engine.discussions.run(project["id"], "legacy-tie", "exploration", members, {}, "line-01")
    assert result["decision_source"] == "god"
    assert store.get("discussion", did)["records"] == votes


async def test_migration_does_not_clear_another_failed_task_without_recoverable_checkpoint(scene):
    store, project, engine = scene
    store.update_project(project["id"], status="blocked", error={"code": "decision_blocked", "message": "two failures"})
    for identity in ("recoverable", "missing-checkpoint"):
        store.add_task(project["id"], "boss", {}, idempotency_key=identity)
        store.finish_task(identity, error={"code": "decision_blocked", "message": identity})
    class Graph:
        async def aget_state(self, config):
            exists = config["configurable"]["thread_id"].endswith("/recoverable")
            return SimpleNamespace(values={"task": "present"} if exists else {}, next=("execute",) if exists else (),
                config={"configurable": {"checkpoint_id": "old-checkpoint"}})
    proposals = await migrate_arbitrable_tasks(engine, {"boss": SimpleNamespace(graph=Graph())})
    assert len(proposals) == 1
    assert store.get("task", "recoverable")["status"] == "pending"
    assert store.get("task", "missing-checkpoint")["status"] == "failed"
    assert store.project(project["id"])["status"] == "blocked"


async def test_replayed_old_arbitration_does_not_restore_an_outdated_scope_revision(scene):
    store, project, engine = scene
    calls = []
    async def judge(request, context, attempt, **kwargs):
        calls.append(request["id"])
        return {"arbitration_id": request["id"], "snapshot_hash": request["snapshot_hash"], "action": "select_candidate",
            "selected_candidate_id": "candidate", "content_markdown": "选择", "rationale": "当前版本"}
    engine.execute_god_turn = judge
    args = {"research_id": project["id"], "source_task_id": "same-root", "scope_kind": "discussion",
        "scope_id": "same-discussion", "reason_code": "decision_blocked", "origin_node": "discussion.collect",
        "allowed_actions": ["select_candidate"]}
    old = {**args, "scope_revision": "old", "context": {"candidates": {"candidate": {"method": "old"}}, "blockers": []}}
    new = {**args, "scope_revision": "new", "context": {"candidates": {"candidate": {"method": "new"}}, "blockers": []}}
    first = await engine.arbitration.resolve(**old)
    await engine.arbitration.resolve(**new)
    with pytest.raises((ResearchStopped, RuntimeBlocked)):
        await engine.arbitration.resolve(**old)
    assert len(calls) == 2
    assert store.get("arbitration_request", first["arbitration_id"])["status"] == "stale"


async def test_god_final_reply_reuses_stream_message_and_supplies_exact_recovery_constraints(scene):
    store, project, engine = scene
    rid, aid, message_id = project["id"], "arb-stream", "stream-god-reply"
    output = {"arbitration_id": aid, "snapshot_hash": "frozen", "action": "request_revision",
        "target_role": "critic", "instructions": "区分验收阶段", "allowed_changes": ["acceptance_stage"],
        "acceptance_checks": ["保留问题"], "content_markdown": "修订条件", "rationale": "执行结果尚未产生"}
    async def execute(**args):
        assert '"target_role": "critic"' in args["prompt"]
        assert '"select_candidate_allowed": false' in args["prompt"]
        store.upsert_stream_message(rid, message_id, f"{rid}:god:{aid}", "god/" + aid, "修订条件",
            source_event_id="stream-source")
        store.complete_stream_message(message_id, "修订条件")
        return output
    engine.demo.execute_turn = execute
    engine.demo.message_reference = lambda *args: message_id
    provider = project["config_snapshot"]["provider"]
    request = {"id": aid, "arbitration_id": aid, "research_id": rid, "snapshot_hash": "frozen", "line_id": "line-01",
        "provider_config_id": project["provider_config_id"], "credential_version": project.get("credential_version", 0), "model": provider["model"]}
    await engine.execute_god_turn(request, {"role": "critic", "blockers": [{"issue_id": "runtime-result"}]}, 1)
    messages = store.messages(f"{rid}:god:{aid}")["items"]
    assert len(messages) == 1 and messages[0]["message_id"] == message_id
    assert messages[0]["source_system"] == "god_generation"
    assert messages[0]["validation_status"] == "pending"


async def test_runtime_result_deadlock_revises_acceptance_stage_then_votes_without_fabricated_metrics(scene, tmp_path):
    store, project, engine = scene
    rid = project["id"]
    protocol = tmp_path / "protocol.txt"
    protocol.write_text("先运行固定折基线，再记录逐折 F1 与标准差。设计时不声称这些数字已存在。", encoding="utf-8")
    artifact = store.register_artifact(rid, protocol, line_id="line-01")
    original = engine.demo._produce_turn
    async def produce(**args):
        if args["schema"].__name__ == "GodDecision":
            request = args["context"]["arbitration_request"]
            return {"arbitration_id": request["id"], "snapshot_hash": request["snapshot_hash"], "action": "request_revision",
                "target_role": "critic", "instructions": "保留基线验收项，依据协议把阶段改为 execution",
                "allowed_changes": ["acceptance_stage", "stage_review"], "acceptance_checks": ["不伪造数字，不删除问题"],
                "content_markdown": "基线指标应在运行后核查", "rationale": "协议已明确生成和验收步骤", "evidence_refs": [artifact["id"]]}
        output = await original(**args)
        if args["schema"].__name__ == "Review":
            revised = bool(args["context"].get("god_revision"))
            issue = {"issue_id": "baseline-f1", "category": "must_fix", "description": "缺少逐折基线 F1 与标准差",
                "resolved": False, "experiment_mapping": "固定折 Runner 产出逐折 F1/std，Verify 核查实际 metrics.json"}
            if revised:
                issue.update(acceptance_stage="execution", stage_review={"from_stage": "design", "rationale": "指标只能在运行后产生，协议已完整",
                    "evidence": [{"source_id": artifact["id"], "claim": "固定协议规定了生成和核查顺序", "status": "observed"}]})
            output.update(ready_for_vote=revised, issues=[issue], new_evidence=[])
        return output
    engine.demo._produce_turn = produce
    result = await engine.discussions.run(rid, "runtime-metrics-design", "critic", engine.members(project, "critic", "line-01"),
        {"discussion_protocol": 1, "evidence": [{"source_id": artifact["id"]}]}, "line-01")
    assert result["issues"][0]["acceptance_stage"] == "execution"
    assert result["issues"][0]["category"] == "must_fix" and not result["issues"][0]["resolved"]
    requests = store.list("arbitration_request", rid)
    assert len(requests) == 1 and requests[0]["status"] == "applied"
    assert store.get("arbitration_decision", requests[0]["decision_id"])["decision"]["action"] == "request_revision"
    assert not store.list("run", rid) and not store.list("verification", rid)
