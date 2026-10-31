"""Offline control-flow replay of saved Tweets5/6 disputes, never a scientific rerun.

The fixture contains exact historical blocker text and the final two complete
discussion messages, identified by their original snapshot and content hashes.
All new votes and God products below are explicitly scripted test decisions.
Only the temporary test project is writable; no original project is opened.
"""
import json
from pathlib import Path

import pytest
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from backend.graphs.engine import ResearchEngine
from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor
from packages.contracts.research import digest


CASES = json.loads((Path(__file__).parents[1] / "fixtures" / "disaster_discussion_replay.json").read_text(encoding="utf-8"))


@pytest.mark.asyncio
@pytest.mark.parametrize("case", CASES, ids=[case["name"] for case in CASES])
async def test_saved_dispute_third_failed_end_vote_allows_god_new_exploration(case, tmp_path):
    assert case["source_status"] == "terminated"
    assert case["blockers"] and all(message["content_hash"] == digest(message["content_markdown"])
        for message in case["messages"])
    store = Store(tmp_path)
    project = store.start_project(store.create_project({"client_request_id": case["name"], "topic": "离线讨论协议回放",
        "mode": "demo", "team": {"n": 1, "m1": 1, "m2": 1, "boss_members": 1},
        "max_runtime_seconds": 120})["id"])
    rid = project["id"]
    assert rid != case["source_project_id"]
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(rid, project["deadline_at"])
    calls = []
    try:
        async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "checkpoints.sqlite")) as saver:
            engine = ResearchEngine(store, control, None, None, saver)
            try:
                async def replay(**arguments):
                    schema, context = arguments["schema"], arguments["context"]
                    calls.append((schema.__name__, context))
                    assert context["saved_dispute"] == case
                    if schema.__name__ == "DiscussionTurn":
                        if context["role"] == "exploration":
                            return {"content_markdown": "离线回放：按 God 给出的任务修订方案，未执行实验。",
                                "end_vote": {"final_output": {"content_markdown": "新的修订方案：先固定折分配、处理重复文本，再由真实执行产出评测证据。", "offline_replay": True}}}
                        seen_poll = any(message.get("vote", {}).get("type") == "ordinary"
                            for message in context["conversation_snapshot"])
                        if not seen_poll:
                            return {"content_markdown": "离线回放：审议原始分歧；以下为历史材料，不声称已运行。",
                                "ordinary_vote": {"question": "原始争议应如何继续处理？", "options": ["先修改完整设计", "按现有设计执行再验收"]}}
                        return {"content_markdown": "离线回放普通发言：仍有设计选择分歧，需明确先后顺序与完整产物。",
                            "end_vote": {"final_output": {"content_markdown": "供成员审议的回放方案：" + case["blockers"][0]["description"],
                                "offline_replay": True}}}
                    if schema.__name__ in {"PollBallot", "OrdinaryBallot"}:
                        return {"option": context["vote"]["options"][0]["label"], "reason": "离线测试票：保留原始争议并先修订设计。"}
                    if schema.__name__ == "EndBallot":
                        return {"approve": context["role"] == "exploration", **({"next_step": "implementation"}
                            if context["role"] == "critic" else {})}
                    if schema.__name__ == "DiscussionFinal":
                        assert context["failed_end_votes"] == 3
                        assert len([vote for vote in context["votes"] if vote["type"] == "end"]) == 3
                        return {"next_step": "exploration", "final_output": {
                            "content_markdown": "离线脚本 God 新定稿：把尚无运行数字的要求留给后续执行；先修订重复文本与折分配设计，保留真实代码问题。未产生实验结果。",
                            "offline_replay": True, "new_product": "god-new-design", "source_snapshot": case["snapshot_id"]}}
                    raise AssertionError("Offline replay must not request any execution contract: " + schema.__name__)
                engine.demo._produce_turn = replay
                context = {"saved_dispute": case, "deadline_at": project["deadline_at"],
                    "instruction": "离线回放保存的争议，只验证协议；不得恢复或改写原项目。"}
                result = await engine.discussions.run(rid, "offline-critic", "critic", ["line-01/critic/member-01"], context, "line-01")
                assert result["next_step"] == "exploration" and result["decision_source"] == "god"
                assert result["final_output"]["new_product"] == "god-new-design"
                source_state = store.get("discussion_state", result["discussion_id"])
                assert source_state["failed_end_votes"] == 3 and source_state["freeze_remaining"] == []
                assert [vote["type"] for vote in source_state["votes"]] == ["ordinary", "end", "end", "end"]
                assert sum(name == "DiscussionFinal" for name, _ in calls) == 1
                ended_calls = len(calls)
                # Restart/replay reuses the God product without a fourth end vote.
                assert await engine.discussions.run(rid, "offline-critic", "critic", ["line-01/critic/member-01"], context, "line-01") == result
                assert len(calls) == ended_calls
                next_result = await engine.discussions.run(rid, "offline-new-exploration", "exploration",
                    ["line-01/exploration/member-01"], {**context, "critic_handoff": result["final_output"],
                        "handoff_from_discussion": result["discussion_id"]}, "line-01")
                assert next_result["next_step"] == "critic" and next_result["decision_source"] == "unanimous"
                assert next_result["discussion_id"] != result["discussion_id"]
                assert store.get("discussion_state", next_result["discussion_id"])["failed_end_votes"] == 0
                assert store.get("discussion_handoff", result["discussion_id"])["status"] == "delivered"
                assert store.get("discussion_state", result["discussion_id"]) == source_state
                assert not store.list("run", rid) and not store.list("submission", rid) and not store.list("verification", rid)
            finally:
                await engine.close()
    finally:
        control.close()
