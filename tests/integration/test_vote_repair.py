import pytest
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from backend.adapters.dsh import RuntimeBlocked
from backend.graphs.engine import ResearchEngine
from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor
from packages.contracts.research import Vote


@pytest.mark.asyncio
@pytest.mark.parametrize("role", ["boss", "exploration", "critic"])
@pytest.mark.parametrize("failure", ["unknown", "member_without_candidate", "other_group", "repeated"])
async def test_invalid_vote_is_repaired_before_recording_or_counting(tmp_path, role, failure):
    store = Store(tmp_path)
    project = store.start_project(store.create_project({"client_request_id": "votes", "topic": "votes", "mode": "demo",
        "team": {"boss_members": 2, "m1": 2, "m2": 2}, "max_runtime_seconds": 120})["id"])
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(project["id"], project["deadline_at"])
    try:
        async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "checkpoints.sqlite")) as saver:
            engine = ResearchEngine(store, control, None, None, saver)
            try:
                line_id = None if role == "boss" else "line-01"
                member, peer = engine.members(project, role, line_id)
                invalid = peer if failure == "member_without_candidate" else "line-02/exploration/member-01" if failure == "other_group" else "nonexistent-candidate"
                valid = {"content_markdown": "corrected vote", "candidate_id": member, "rationale": "current proposal"}
                bad = {**valid, "content_markdown": "invalid vote", "candidate_id": invalid}
                # These are deliberately schema-valid, so only contextual checks can catch the error.
                Vote.model_validate(bad)
                calls = []
                async def produce(**kwargs):
                    calls.append(kwargs)
                    if kwargs["role"] == "god":
                        request = kwargs["context"]["arbitration_request"]
                        return {"arbitration_id": request["id"], "snapshot_hash": request["snapshot_hash"],
                            "action": "request_revision", "content_markdown": "要求原成员重答有效投票", "rationale": "不代替成员投票",
                            "target_role": role, "instructions": "仅投本轮真实候选", "allowed_changes": ["candidate_id"],
                            "acceptance_checks": ["candidate belongs to this group and round"]}
                    return dict(bad if len(calls) <= (5 if failure == "repeated" else 1) else valid)
                engine.demo._produce_turn = produce
                state = {"research_id": project["id"], "discussion_id": "vote-discussion", "role": role,
                    "line_id": line_id, "members": [member, peer], "member": member, "phase": "vote", "round": 3,
                    "context": {}, "history_snapshot": [], "candidates": {member: {"method": "current"}},
                    "records": {f"vote:3:{peer}": valid}}
                update = await engine.discussions.member_turn(state)
                assert update["records"] == {f"vote:3:{member}": valid}
                complete = {**state, **update, "records": {**state["records"], **update["records"]}}
                assert await engine.discussions.collect(complete) == {"winner": member}
                messages = store.messages(state["discussion_id"])["items"]
                assert len(messages) == 1 and messages[0]["structured"] == valid
                # Replaying adapter-cached outputs still goes through the contextual validator.
                assert (await engine.discussions.member_turn(state))["records"] == update["records"]
                assert len(store.messages(state["discussion_id"])["items"]) == 1
                assert len(calls) == (6 if failure == "repeated" else 2)
                assert [call["turn_id"] for call in calls[:2]] == ["vote:3", "vote:3:repair"]
                assert all(call["agent_id"] == member and call["schema"] is Vote for call in calls if call["role"] != "god")
                assert all(call["role"] != "god" for call in calls)
                assert "allowed IDs: " + member in calls[1]["prompt"]
                assert "上次输出校验失败" in calls[1]["prompt"]
                assert not any(event["type"] in {"vote.recorded", "group.decision_created"} for event in store.events(project["id"]))
            finally:
                await engine.close()
    finally:
        control.close()
