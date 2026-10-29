import asyncio

import pytest
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from backend.graphs.engine import ResearchEngine
from backend.graphs.tasks import PaperGraph, TaskSuspended, current_task
from backend.persistence.store import Store, StoreError
from backend.runtime.control import ControlSupervisor
from backend.runtime.worker import run_worker
from backend.adapters.dsh import RuntimeBlocked
from packages.contracts.research import MessageReply


@pytest.mark.asyncio
async def test_internal_dependency_runs_boss_after_parent_releases_slot_and_resumes_review(tmp_path):
    store = Store(tmp_path)
    p = store.start_project(store.create_project({"topic": "dependency", "client_request_id": "dependency", "mode": "demo",
        "team": {"m1": 1, "boss_members": 1}})["id"])
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(p["id"], p["deadline_at"])
    source = tmp_path / "baseline.json"
    source.write_text('{"f1":0.7}', encoding="utf-8")
    artifact = store.register_artifact(p["id"], source, line_id="line-01")
    try:
        async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "cp.sqlite")) as saver:
            engine = ResearchEngine(store, control, None, None, saver)
            original = engine.demo._produce_turn
            calls = []
            async def produce(**args):
                name, context = args["schema"].__name__, args["context"]
                calls.append(name)
                if name == "GodDecision":
                    req = context["arbitration_request"]
                    return {"arbitration_id": req["id"], "snapshot_hash": req["snapshot_hash"], "action": "needs_external_input",
                        "content_markdown": "等待 line-01 基线", "rationale": "内部依赖应交 Boss",
                        "missing_items": ["line-01 基线 F1"], "why_unavailable": "上游本研究线尚未交接", "operation": "Boss 协调",
                        "dependency_line_ids": ["line-01"]}
                result = await original(**args)
                if name == "Review" and args["role"] == "exploration":
                    ready = bool(context.get("recovery_evidence"))
                    result["ready_for_vote"] = ready
                    result["issues"] = [{"issue_id": "baseline", "category": "must_fix", "description": "基线交接",
                        "resolved": ready, "resolution": artifact["id"] if ready else "", "experiment_mapping": ""}]
                if name == "BossProposal":
                    result["candidate"].update(action="report", assignments=[], request_report=False,
                        memory_updates=[{"node_id": "dependency/baseline", "title": "基线交接", "status": "observed",
                            "content_markdown": "本地夹具已登记基线，F1=0.7", "source_ids": [artifact["id"]]}])
                return result
            engine.demo._produce_turn = produce
            async def operation(task):
                return await engine.discussions.run(p["id"], "dependent", "exploration",
                    engine.members(p, "exploration", "line-02"), {"topic": "baseline"}, "line-02")
            graph = PaperGraph(operation, saver)
            root = store.add_task(p["id"], "line", {"line_id": "line-02"}, "dependent-root")
            token = current_task.set(root)
            try:
                with pytest.raises(TaskSuspended) as suspended:
                    await graph.run(root)
            finally:
                current_task.reset(token)
            assert not engine.discussions.locks[(p["id"], "exploration", "line-02")].locked()
            store.patch("task", root["id"], status="waiting_recovery", resume_token=suspended.value.resume_token)
            requests = store.list("arbitration_request", p["id"])
            assert len(requests) == 1 and requests[0]["status"] == "applied"
            await engine.dependencies.dispatch(p["id"])
            bosses = [task for task in store.pending_tasks() if task.get("payload", {}).get("dependency_wait_id")]
            assert len(bosses) == 1
            token = current_task.set(bosses[0])
            try:
                decision = await engine.boss(bosses[0])
            finally:
                current_task.reset(token)
            store.finish_task(bosses[0]["id"], result=decision)
            await engine.dependencies.dispatch(p["id"])
            resumed = store.get("task", root["id"])
            assert resumed["status"] == "pending"
            token = current_task.set(resumed)
            try:
                result = await graph.run(resumed)
            finally:
                current_task.reset(token)
            assert result["decision_source"] == "majority"
            assert calls.count("GodDecision") == 1
            assert len(store.list("dependency_wait", p["id"])) == 1
            assert store.get("recovery_chain", requests[0]["recovery_chain_id"])["status"] == "fulfilled"
            assert not store.list("run", p["id"])
            await engine.close()
    finally:
        control.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_point", ["completion", "failure_handler", "control"])
async def test_worker_retries_failed_completion_without_repeating_completed_model(tmp_path, monkeypatch, failure_point):
    import backend.graphs.engine as engine_module
    import backend.runtime.control as control_module
    import backend.runtime.runner as runner_module
    store = Store(tmp_path)
    p = store.start_project(store.create_project({"topic": "retry", "client_request_id": "retry", "mode": "demo"})["id"])
    for initial in store.pending_tasks():
        store.finish_task(initial["id"], result={})
    root = store.add_task(p["id"], "paper", {}, "retry-parent")
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(p["id"], p["deadline_at"])
    calls = []
    class Engine(ResearchEngine):
        async def paper(self, task):
            calls.append(task["id"])
            if failure_point == "control" and len(calls) == 1:
                raise control_module.ControlUnavailable("one lost admission response")
            result = await self.turn(p["id"], "paper", "paper", task["id"], "reply", MessageReply, {})
            if failure_point == "failure_handler" and len(calls) == 1:
                raise RuntimeBlocked("fixture_failure", "fixture failure before recovery bookkeeping")
            return result
    original_finish = Store.finish_task
    failures = []
    def finish(self, task_id, *args, **kwargs):
        if failure_point != "control" and task_id == root["id"] and not failures:
            failures.append(task_id)
            raise StoreError("storage_busy", "fixture lock", 503)
        return original_finish(self, task_id, *args, **kwargs)
    monkeypatch.setattr(Store, "finish_task", finish)
    monkeypatch.setattr(engine_module, "ResearchEngine", Engine)
    monkeypatch.setattr(control_module, "ControlClient", lambda _: control)
    monkeypatch.setattr(runner_module, "RunnerClient", lambda _: None)
    monkeypatch.setenv("AUTORESEARCH_WORKER_CONCURRENCY", "1")
    worker = asyncio.create_task(run_worker(tmp_path, "fixture", "fixture"))
    try:
        async with asyncio.timeout(12):
            while store.get("task", root["id"])["status"] != "completed":
                if worker.done():
                    await worker
                await asyncio.sleep(.05)
        assert calls == [root["id"]] * (2 if failure_point in {"failure_handler", "control"} else 1)
        assert store.get("task", root["id"])["infrastructure_retries"] == 1
        assert len(store.list("execution", p["id"])) == 1
        assert any(e["type"] == "task.retry_wait" for e in store.events(p["id"]))
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
        control.close()
