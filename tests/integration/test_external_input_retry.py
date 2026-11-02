import asyncio

import pytest
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from backend.adapters.dsh import RuntimeBlocked
from backend.graphs.engine import ResearchEngine
from backend.graphs.tasks import PaperGraph, current_task
from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor
from packages.contracts.research import MessageReply


@pytest.mark.asyncio
@pytest.mark.parametrize("scope", ["turn", "discussion"])
async def test_explicit_retry_reads_new_input_in_fresh_scene_and_preserves_history(tmp_path, scope):
    store = Store(tmp_path)
    p = store.start_project(store.create_project({"topic": "external input", "mode": "demo",
        "client_request_id": "external", "team": {"m1": 1}})["id"])
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(p["id"], p["deadline_at"])
    supplied = tmp_path / "dataset.csv"
    calls = []
    engine = None
    try:
        async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "cp.sqlite")) as saver:
            engine = ResearchEngine(store, control, None, None, saver)
            original = engine.demo._produce_turn
            async def produce(**args):
                name = args["schema"].__name__
                calls.append((name, args["turn_id"]))
                if name == "GodDecision":
                    req = args["context"]["arbitration_request"]
                    return {"arbitration_id": req["id"], "snapshot_hash": req["snapshot_hash"],
                        "action": "needs_external_input", "content_markdown": "缺少输入数据",
                        "rationale": "必须先补齐实际数据", "missing_items": ["dataset.csv"],
                        "why_unavailable": "尚未提供", "operation": "补齐数据后重试"}
                if name == "MessageReply":
                    return {"content_markdown": supplied.read_text(encoding="utf-8") if supplied.exists() else "缺少 dataset.csv，无法读取实际输入"}
                output = await original(**args)
                if name == "Review" and not supplied.exists():
                    output.update(ready_for_vote=False, issues=[{"issue_id": "data", "category": "must_fix",
                        "description": "缺少实际数据", "resolved": False}])
                return output
            engine.demo._produce_turn = produce
            async def operation(task):
                if scope == "turn":
                    context = {"required_inputs": ["dataset.csv"], "input_available": supplied.exists()}
                    reply = await engine.turn(p["id"], "paper", "paper", task["id"], "read", MessageReply, context)
                    if not supplied.exists():
                        # A valid reply cannot satisfy a task whose required
                        # data is missing. Arbitrate that business blocker;
                        # malformed replies have unlimited local corrections.
                        return await engine.arbitration.repair_turn(p["id"], "paper", "paper",
                            task["id"], "read", MessageReply, context, reason_code="decision_blocked",
                            diagnostic={"code": "required_input_missing", "missing_items": ["dataset.csv"],
                                "message": "需要用户补齐实际数据后才能完成读取", "output": reply})
                    return reply
                return await engine.discussions.run(p["id"], "design", "exploration",
                    engine.members(p, "exploration", "line-01"), {"discussion_protocol": 1}, "line-01")
            graph = PaperGraph(operation, saver)
            root = store.add_task(p["id"], "paper", {}, "external-root")
            token = current_task.set(root)
            try:
                with pytest.raises(RuntimeBlocked) as blocked:
                    await asyncio.wait_for(graph.run(root), timeout=10)
            finally:
                current_task.reset(token)
            assert blocked.value.code == "arbitration_needs_external_input"
            old_request = store.list("arbitration_request", p["id"])[0]
            old_snapshot = store.get("arbitration_snapshot", old_request["snapshot_id"])
            assert old_request["reason_code"] == ("decision_blocked" if scope == "turn" else "design_deadlock")
            store.finish_task(root["id"], error={"code": blocked.value.code, "message": str(blocked.value)})
            supplied.write_text("actual new input", encoding="utf-8")
            store.retry_failed_tasks(p["id"])
            resumed = store.get("task", root["id"])
            assert resumed["recovery_generation"] == 1
            token = current_task.set(resumed)
            try:
                result = await asyncio.wait_for(graph.run(resumed), timeout=10)
            finally:
                current_task.reset(token)
            assert result
            if scope == "turn":
                assert result["content_markdown"] == "actual new input"
                turns = [turn_id for name, turn_id in calls if name == "MessageReply"]
                assert len(turns) == 2 and turns[0] != turns[1]
                assert turns[1].endswith(":recovery-1")
            assert store.get("arbitration_request", old_request["id"])["status"] == "needs_external_input"
            assert store.get("arbitration_snapshot", old_request["snapshot_id"]) == old_snapshot
            assert len([c for c in calls if c[0] == "GodDecision"]) == 1
            assert store.project(p["id"])["deadline_at"] == p["deadline_at"]
    finally:
        try:
            if engine is not None:
                await asyncio.wait_for(engine.close(), timeout=5)
        finally:
            control.close()
