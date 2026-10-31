"""Behavioral checks for the new protocol, independent of legacy candidate voting."""
from collections import Counter
from copy import deepcopy

import pytest

from backend.adapters.dsh import ResearchStopped, RuntimeBlocked
from backend.graphs.discussion_v2 import MemberDiscussion
from backend.persistence.store import Store


MEMBERS = ["A", "B", "C"]
PRODUCT = {"content_markdown": "完整实验计划；投票过程中保持同一版本。", "method": "grouped validation"}


class ScriptedEngine:
    def __init__(self, store, script):
        self.store, self.script = store, script
        self.calls = []
        self.stopped = False

    async def guard(self, rid):
        if self.stopped:
            raise ResearchStopped("stopped")
        return self.store.project(rid)

    def validate_discussion_output(self, role, output, context, next_step=None):
        if not output.get("content_markdown"):
            raise ValueError("missing product")

    async def turn(self, rid, member, role, did, turn_id, schema, context, **options):
        self.calls.append((member, schema.__name__, deepcopy(context)))
        output = self.script(member, schema.__name__, context)
        if isinstance(output, BaseException):
            raise output
        output = schema.model_validate(output).model_dump()
        if options.get("output_validator"):
            options["output_validator"](output)
        return output


def project(store):
    return store.start_project(store.create_project({"client_request_id": "new-poll", "topic": "fixture",
        "mode": "demo", "max_runtime_seconds": 120})["id"])


async def run(engine, p, *, role="critic", members=MEMBERS):
    return await MemberDiscussion(engine).run(p["id"], "fixture:group-v3", role, members,
        {"topic": "fixture", "issues": [{"issue_id": "old", "category": "must_fix", "resolved": False}]}, None, members)


def end_speech(text="正常讨论，提出完整方案。"):
    return {"content_markdown": text, "end_vote": {"final_output": PRODUCT}}


@pytest.mark.asyncio
async def test_ordinary_unanimity_returns_to_next_speaker_and_end_binds_exact_product(tmp_path):
    store = Store(tmp_path)
    p = project(store)
    def script(member, schema, context):
        if schema == "DiscussionTurn":
            if member == "A":
                return {"content_markdown": "先讨论验证方法", "ordinary_vote": {
                    "question": "如何选择验证方式？", "options": ["分组划分", "普通划分"]}}
            return end_speech()
        if schema == "PollBallot":
            return {"option": "分组划分", "reason": "避免跨折重复文本泄露"}
        assert schema == "EndBallot"
        return {"approve": True, "next_step": "implementation", "reason": "额外说明被忽略"}
    engine = ScriptedEngine(store, script)
    result = await run(engine, p)
    assert result["final_output"] == PRODUCT
    assert result["next_step"] == "implementation"
    assert result["decision_source"] == "unanimous"
    assert [m for m, name, ctx in engine.calls if name == "DiscussionTurn"] == ["A", "B"]
    state = store.get("discussion_state", "fixture:group-v3")
    assert state["failed_end_votes"] == 0 and len(state["votes"]) == 2
    messages = store.messages("fixture:group-v3", limit=100)["items"]
    records = [m["vote"] for m in messages if m["kind"] == "discussion_vote"]
    assert len([v for v in records if v["phase"] == "ballot"]) == 6
    assert records[0]["type"] == "ordinary" and records[-1]["status"] == "passed"
    # A started record never gains future ballots: replay reads immutable history.
    assert records[0]["ballots"] == []
    assert records[-1]["ballots"][0]["approve"] is True
    before = len(engine.calls)
    assert await run(engine, p) == result
    assert len(engine.calls) == before
    assert len(store.list("discussion_handoff", p["id"])) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("disagreement", ["no", "direction"])
async def test_three_failed_end_votes_freeze_full_rotation_then_god_new_product(tmp_path, disagreement):
    store = Store(tmp_path)
    p = project(store)
    speeches, end_ballots = [], []
    def script(member, schema, context):
        if schema == "DiscussionTurn":
            speeches.append((member, list(context["freeze_remaining"])))
            # Even premature requests preserve the full speech and do not cast
            # a ballot; the last frozen member may immediately open a new vote.
            return end_speech()
        if schema == "EndBallot":
            end_ballots.append((member, context["failed_end_votes"]))
            return {"approve": member != "C" if disagreement == "no" else True,
                "next_step": "exploration" if disagreement == "direction" and member == "C" else "implementation"}
        assert schema == "DiscussionFinal"
        assert len(context["votes"]) == 3
        assert len([m for m in context["conversation_snapshot"] if m["kind"] == "discussion_vote"]) >= 15
        return {"next_step": "exploration", "final_output": {"content_markdown": "God 提出的全新方案", "new_method": "new"}}
    engine = ScriptedEngine(store, script)
    result = await run(engine, p)
    assert [m for m, frozen in speeches] == ["A", "B", "C", "A", "B", "C", "A"]
    assert Counter(n for m, n in end_ballots) == {0: 3, 1: 3, 2: 3}
    assert result["decision_source"] == "god"
    assert result["final_output"]["new_method"] == "new"
    assert result["next_step"] == "exploration"
    assert sum(schema == "DiscussionFinal" for _, schema, _ in engine.calls) == 1
    assert not any(schema == "Synthesis" for _, schema, _ in engine.calls)
    state = store.get("discussion_state", "fixture:group-v3")
    assert state["failed_end_votes"] == 3 and state["mode"] == "ended"
    assert state["freeze_remaining"] == []
    assert len(store.list("discussion_finalization", p["id"])) == 1


@pytest.mark.asyncio
async def test_missing_ballot_resume_preserves_votes_and_does_not_consume_failure(tmp_path):
    store = Store(tmp_path)
    p = project(store)
    interrupted = False
    def script(member, schema, context):
        nonlocal interrupted
        if schema == "DiscussionTurn":
            return end_speech()
        if member == "B" and not interrupted:
            interrupted = True
            return RuntimeBlocked("model_timeout", "injected")
        return {"approve": True, "next_step": "implementation"}
    engine = ScriptedEngine(store, script)
    with pytest.raises(RuntimeBlocked, match="injected"):
        await run(engine, p)
    checkpoint = store.get("discussion_state", "fixture:group-v3")
    assert list(checkpoint["active_vote"]["ballots"]) == ["A"]
    assert checkpoint["failed_end_votes"] == 0
    result = await run(engine, p)
    assert result["decision_source"] == "unanimous"
    assert Counter(member for member, schema, _ in engine.calls if schema == "EndBallot") == {"A": 1, "B": 2, "C": 1}
    assert len(store.messages("fixture:group-v3", limit=100)["items"]) == 6


@pytest.mark.asyncio
async def test_ordinary_ballots_do_not_thaw_end_voting_and_empty_request_is_not_speech(tmp_path):
    store = Store(tmp_path)
    p = project(store)
    step = 0
    def script(member, schema, context):
        nonlocal step
        if schema == "DiscussionTurn":
            step += 1
            if step == 2:
                return {"content_markdown": "", "ordinary_vote": {"question": "选什么？", "options": ["一", "二"]}}
            if step == 3:
                # End-vote only, with no ordinary utterance, cannot thaw C.
                return end_speech("")
            return end_speech()
        if schema == "PollBallot":
            assert context["freeze_remaining"] == ["A", "B", "C"]
            return {"option": "一", "reason": "支持"}
        if schema == "EndBallot":
            if context["failed_end_votes"] == 0:
                return {"approve": False, "next_step": "exploration"}
            return {"approve": True, "next_step": "implementation"}
        raise AssertionError(schema)
    engine = ScriptedEngine(store, script)
    await run(engine, p)
    speeches = [context for _, schema, context in engine.calls if schema == "DiscussionTurn"]
    assert speeches[2]["freeze_remaining"] == ["A", "B", "C"]
    assert speeches[3]["freeze_remaining"] == ["A", "B", "C"]
    state = store.get("discussion_state", "fixture:group-v3")
    assert state["failed_end_votes"] == 1
    assert [v["type"] for v in state["votes"]] == ["end", "ordinary", "end"]


@pytest.mark.asyncio
async def test_single_member_end_ballot_fixed_direction_and_stop_late_reply(tmp_path):
    store = Store(tmp_path)
    p = project(store)
    def script(member, schema, context):
        return end_speech() if schema == "DiscussionTurn" else {"approve": True}
    engine = ScriptedEngine(store, script)
    result = await run(engine, p, role="exploration", members=["A"])
    assert result["next_step"] == "critic"
    assert len(engine.calls) == 2

    other_store = Store(tmp_path / "stopped")
    other = project(other_store)
    def late_reply(member, schema, context):
        if schema == "DiscussionTurn":
            return end_speech()
        other_engine.stopped = True
        return {"approve": True}
    other_engine = ScriptedEngine(other_store, late_reply)
    with pytest.raises(ResearchStopped):
        await run(other_engine, other, role="exploration", members=["A"])
    assert not other_store.list("discussion_handoff", other["id"])
    assert other_store.get("discussion_state", "fixture:group-v3")["mode"] == "end_vote"
    assert other_store.get("discussion_state", "fixture:group-v3")["active_vote"]["ballots"]["A"]["approve"] is True


@pytest.mark.asyncio
async def test_no_automatic_god_without_end_vote(tmp_path):
    store = Store(tmp_path)
    p = project(store)
    count = 0
    def script(member, schema, context):
        nonlocal count
        count += 1
        if count == 13:
            return RuntimeBlocked("fixture_interrupted", "pause after twelve unchanged utterances")
        assert schema == "DiscussionTurn"
        return {"content_markdown": "仍在自由讨论同一个问题"}
    engine = ScriptedEngine(store, script)
    with pytest.raises(RuntimeBlocked, match="twelve unchanged"):
        await run(engine, p)
    state = store.get("discussion_state", "fixture:group-v3")
    assert state["turn_index"] == 12 and state["failed_end_votes"] == 0
    assert not state["votes"] and not store.list("discussion_finalization", p["id"])


@pytest.mark.asyncio
async def test_graph_resume_technical_generation_keeps_same_discussion_instance(tmp_path):
    from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
    from backend.graphs.discussion import GroupDiscussionGraph
    from backend.graphs.tasks import current_task
    store = Store(tmp_path)
    p = project(store)
    failing = True
    def script(member, schema, context):
        if schema == "DiscussionTurn":
            return end_speech()
        if failing:
            return RuntimeBlocked("model_timeout", "resume this same vote")
        return {"approve": True}
    engine = ScriptedEngine(store, script)
    async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "checkpoint.sqlite")) as saver:
        graph = GroupDiscussionGraph(engine, saver)
        with pytest.raises(RuntimeBlocked):
            await graph.run(p["id"], "generation", "exploration", ["A"], {})
        saved = store.get("discussion_state", "generation:group-v3")
        assert saved["mode"] == "end_vote"
        token = current_task.set({"recovery_generation": 4})
        failing = False
        try:
            result = await graph.run(p["id"], "generation", "exploration", ["A"], {})
        finally:
            current_task.reset(token)
    assert result["discussion_id"] == "generation:group-v3"
    assert len(store.list("discussion_state", p["id"])) == 1
    assert sum(schema == "DiscussionTurn" for _, schema, _ in engine.calls) == 1


@pytest.mark.asyncio
async def test_malformed_ballot_retry_stays_with_member_and_preserves_prior_votes(tmp_path):
    store = Store(tmp_path)
    p = project(store)
    malformed = True
    def script(member, schema, context):
        if schema == "DiscussionTurn":
            return end_speech()
        if member == "B" and malformed:
            return RuntimeBlocked("invalid_output", "malformed member reply")
        return {"approve": True, "next_step": "implementation"}
    engine = ScriptedEngine(store, script)
    with pytest.raises(RuntimeBlocked) as error:
        await run(engine, p)
    assert error.value.code == "discussion_technical_failure"
    malformed = False
    await run(engine, p)
    state = store.get("discussion_state", "fixture:group-v3")
    assert state["failed_end_votes"] == 0
    assert Counter(member for member, schema, _ in engine.calls if schema == "EndBallot") == {"A": 1, "B": 2, "C": 1}
    assert not store.list("discussion_finalization", p["id"])


def test_final_result_messages_and_delivery_intent_commit_atomically(tmp_path, monkeypatch):
    store = Store(tmp_path)
    p = project(store)
    state = store.initialize_discussion("atomic", {"discussion_id": "atomic", "research_id": p["id"],
        "protocol_version": 2, "members": ["A"], "mode": "discussion"})
    final = {**state, "mode": "ended", "result": {"next_step": "critic", "final_output": PRODUCT, "decision_source": "unanimous"}}
    original = store._create_message
    def fail(*args, **kwargs):
        raise RuntimeError("disk failure")
    monkeypatch.setattr(store, "_create_message", fail)
    with pytest.raises(RuntimeError):
        store.commit_discussion_state(state, final, [{"content_markdown": "passed", "source_event_id": "atomic-final"}])
    assert store.get("discussion_state", "atomic")["mode"] == "discussion"
    assert not store.get("discussion_handoff", "atomic")
    monkeypatch.setattr(store, "_create_message", original)
    saved = store.commit_discussion_state(state, final)
    assert saved["mode"] == "ended"
    assert store.get("discussion_handoff", "atomic")["status"] == "pending"


@pytest.mark.asyncio
@pytest.mark.parametrize("late_stop", [False, True])
async def test_god_reply_reused_after_commit_crash_and_late_stop_does_not_deliver(tmp_path, monkeypatch, late_stop):
    store = Store(tmp_path)
    p = project(store)
    def script(member, schema, context):
        if schema == "DiscussionTurn":
            return end_speech()
        if schema == "EndBallot":
            return {"approve": False}
        assert schema == "DiscussionFinal"
        if late_stop:
            engine.stopped = True
        return {"next_step": "critic", "final_output": {"content_markdown": "全新定稿"}}
    engine = ScriptedEngine(store, script)
    original = store.commit_discussion_state
    crashed = False
    def commit(previous, state, messages=()):
        nonlocal crashed
        if not late_stop and state.get("mode") == "ended" and not crashed:
            crashed = True
            raise RuntimeError("crash before atomic finalization")
        return original(previous, state, messages)
    monkeypatch.setattr(store, "commit_discussion_state", commit)
    with pytest.raises(ResearchStopped if late_stop else RuntimeError):
        await run(engine, p, role="exploration", members=["A"])
    assert not store.list("discussion_handoff", p["id"])
    if late_stop:
        assert store.list("discussion_finalization", p["id"])[0]["status"] == "completed_after_stop"
        return
    result = await run(engine, p, role="exploration", members=["A"])
    assert result["decision_source"] == "god"
    assert sum(schema == "DiscussionFinal" for _, schema, _ in engine.calls) == 1
    assert len(store.list("discussion_handoff", p["id"])) == 1
