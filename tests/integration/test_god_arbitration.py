import asyncio

import pytest
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from backend.adapters.dsh import ResearchStopped, RuntimeBlocked
from backend.graphs.engine import ResearchEngine
from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor


@pytest.fixture
def setup_project(tmp_path):
    store = Store(tmp_path)
    project = store.start_project(store.create_project({"client_request_id": "god", "topic": "arbitration fixture",
        "mode": "demo", "team": {"boss_members": 2, "m1": 2, "m2": 2}, "max_runtime_seconds": 120})["id"])
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(project["id"], project["deadline_at"])
    yield store, project, control
    control.close()


def selection(request, context):
    return {"arbitration_id": request["id"], "snapshot_hash": request["snapshot_hash"], "action": "select_candidate",
        "content_markdown": "演示仲裁：保留真实平票，选定既有候选继续。", "rationale": "两方案等价，按稳定 ID 消歧",
        "selected_candidate_id": min(context["candidates"]), "minority_views": ["保留另一票"],
        "evidence_refs": [], "limitations": ["fixture"], "unresolved_issue_ids": []}


@pytest.mark.asyncio
@pytest.mark.parametrize("role", ["boss", "exploration", "critic"])
async def test_tied_group_continues_without_fabricating_votes(setup_project, tmp_path, role):
    store, p, control = setup_project
    async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "cp.sqlite")) as saver:
        engine = ResearchEngine(store, control, None, None, saver)
        original = engine.demo._produce_turn
        calls = []
        async def produce(**args):
            if args["schema"].__name__ == "Vote":
                calls.append(args["agent_id"])
                return {"content_markdown": "真实成员票", "candidate_id": args["agent_id"], "rationale": "支持自己的方案"}
            return await original(**args)
        engine.demo._produce_turn = produce
        god_calls = []
        async def judge(request, context, attempt, **kwargs):
            god_calls.append(request["id"])
            return selection(request, context)
        engine.execute_god_turn = judge
        line_id = None if role == "boss" else "line-01"
        try:
            args = (p["id"], "tie-" + role, role, engine.members(p, role, line_id),
                {"initial": True, "lines": store.lines(p["id"]), "topic": p["topic"]})
            result = await engine.discussions.run(*args, line_id=line_id)
            assert result["decision_source"] == "god"
            assert len(calls) == 4 and len(god_calls) == 1
            discussion = store.get("discussion", "tie-" + role + ":serial-v2")
            votes = {key: value["candidate_id"] for key, value in discussion["records"].items() if key.startswith("vote:")}
            assert len(votes) == 4
            assert all(key.endswith(value) for key, value in votes.items())
            assert store.list("arbitration_request", p["id"])[0]["status"] == "applied"
            assert p["status"] == "running"
            assert await engine.discussions.run(*args, line_id=line_id) == result
            assert len(god_calls) == 1
        finally:
            await engine.close()


@pytest.mark.asyncio
async def test_separate_groups_judge_concurrently_and_same_request_deduplicates(setup_project, tmp_path):
    store, p, control = setup_project
    async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "cp.sqlite")) as saver:
        engine = ResearchEngine(store, control, None, None, saver)
        both_started = asyncio.Event()
        started = []
        async def judge(request, context, attempt, **kwargs):
            started.append((request["id"], context["candidates"]))
            if len(started) == 2:
                both_started.set()
            await asyncio.wait_for(both_started.wait(), 3)
            return selection(request, context)
        engine.execute_god_turn = judge
        async def resolve(scope):
            return await engine.arbitration.resolve(research_id=p["id"], source_task_id=scope,
                scope_kind="discussion", scope_id=scope, reason_code="decision_blocked", origin_node="discussion.collect",
                context={"candidates": {scope + "/member": {"method": scope}}, "blockers": []},
                allowed_actions=["select_candidate"])
        try:
            first, duplicate, other = await asyncio.wait_for(asyncio.gather(resolve("a"), resolve("a"), resolve("b")), 5)
            assert first == duplicate
            assert first["arbitration_id"] != other["arbitration_id"]
            assert len(started) == 2
            assert len(store.list("arbitration_request", p["id"])) == 2
        finally:
            await engine.close()


@pytest.mark.asyncio
async def test_late_god_response_never_applies_after_stop(setup_project, tmp_path):
    store, p, control = setup_project
    async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "cp.sqlite")) as saver:
        engine = ResearchEngine(store, control, None, None, saver)
        async def judge(request, context, attempt, **kwargs):
            control.terminate(p["id"])
            return selection(request, context)
        engine.execute_god_turn = judge
        try:
            with pytest.raises(ResearchStopped):
                decision = await engine.arbitration.resolve(research_id=p["id"], source_task_id="late",
                    scope_kind="discussion", scope_id="late", reason_code="decision_blocked", origin_node="discussion.collect",
                    context={"candidates": {"a": {}}, "blockers": []}, allowed_actions=["select_candidate"])
                await engine.arbitration.apply(decision["arbitration_id"], decision)
            assert not store.list("arbitration_effect", p["id"])
        finally:
            await engine.close()
