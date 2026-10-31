import pytest
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from backend.graphs.engine import ResearchEngine
from backend.graphs.tasks import PaperGraph, TaskSuspended, current_task
from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor


@pytest.mark.asyncio
async def test_group_evidence_child_releases_group_lock_and_resumes_review(tmp_path):
    store = Store(tmp_path)
    p = store.start_project(store.create_project({"topic": "evidence fixture", "client_request_id": "evidence", "mode": "demo",
        "team": {"m1": 2}})["id"])
    source = tmp_path / "measurement.json"
    source.write_text('{"observed": 7}', encoding="utf-8")
    artifact = store.register_artifact(p["id"], source, line_id="line-01")
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(p["id"], p["deadline_at"])
    try:
        async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "cp.sqlite")) as saver:
            engine = ResearchEngine(store, control, None, None, saver)
            original = engine.demo._produce_turn
            async def produce(**args):
                name, context = args["schema"].__name__, args["context"]
                if name == "GodDecision":
                    req = context["arbitration_request"]
                    return {"arbitration_id": req["id"], "snapshot_hash": req["snapshot_hash"], "action": "request_evidence",
                        "content_markdown": "核验已登记测量", "rationale": "保留必改项并取得真实来源",
                        "target_role": "exploration", "question": "核验测量", "source_refs": [artifact["id"]],
                        "steps": ["读取已有测量快照"], "result_requirements": ["提供来源及观测状态"]}
                if name == "EvidenceRecoveryOutput":
                    _, path = store.artifact(artifact["id"])
                    assert '"observed": 7' in path.read_text(encoding="utf-8")
                    return {"content_markdown": "已读取实际测量快照", "evidence": [{"source_id": artifact["id"],
                        "claim": "observed=7", "status": "observed"}], "limitations": ["本地夹具"]}
                result = await original(**args)
                if name == "Review":
                    recovered = bool(context.get("recovery_evidence"))
                    result["ready_for_vote"] = recovered
                    result["issues"] = [{"issue_id": "measurement", "category": "must_fix", "description": "需要核验测量",
                        "resolved": recovered, "resolution": artifact["id"] if recovered else "", "experiment_mapping": ""}]
                return result
            engine.demo._produce_turn = produce
            async def operation(task):
                return await engine.discussions.run(p["id"], "evidence-discussion", "exploration",
                    engine.members(p, "exploration", "line-01"), {"discussion_protocol": 1, "evidence": [{"source_id": artifact["id"]}]}, "line-01")
            graph = PaperGraph(operation, saver)
            parent = store.add_task(p["id"], "paper", {}, "evidence-parent")
            token = current_task.set(parent)
            try:
                with pytest.raises(TaskSuspended) as suspended:
                    await graph.run(parent)
            finally:
                current_task.reset(token)
            assert not engine.discussions.locks[(p["id"], "exploration", "line-01")].locked()
            store.patch("task", parent["id"], status="waiting_recovery", resume_token=suspended.value.resume_token)
            child = next(t for t in store.pending_tasks() if t["kind"] == "recovery_role")
            result = await engine.decompositions.execute_evidence(child)
            store.finish_task(child["id"], result=result)
            await engine.decompositions.dispatch(p["id"])
            resumed = store.get("task", parent["id"])
            assert resumed["status"] == "pending"
            token = current_task.set(resumed)
            try:
                decision = await graph.run(resumed)
            finally:
                current_task.reset(token)
            assert decision["decision_source"] == "majority"
            requests = store.list("arbitration_request", p["id"])
            assert len(requests) == 1 and requests[0]["status"] == "applied"
            assert store.get("recovery_chain", requests[0]["recovery_chain_id"])["status"] == "fulfilled"
            await engine.close()
    finally:
        control.close()
