"""Replay the incident's real three-field product and crash boundaries."""
from collections import Counter
from copy import deepcopy

import pytest
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from backend.adapters.dsh import RuntimeBlocked
from backend.graphs.discussion_v2 import MemberDiscussion
from backend.graphs.engine import ResearchEngine
from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor
from backend.runtime.generation_budget import GenerationBudget
from packages.contracts.handoffs import validate_discussion_handoff
from packages.contracts.research import EndBallot, digest


INCOMPLETE = {"content_markdown": "请补充实验设计，核查重复文本泄露。", "config_fingerprint": "abc", "key_metrics": {"f1": 0.8}}
COMPLETE = {"content_markdown": "比较清洗策略", "experiment_spec": {"question": "清洗是否改善 F1？", "fixed": {}, "metrics": ["f1"]}}


class IncidentEngine:
    def __init__(self, store, callback):
        self.store, self.callback, self.calls = store, callback, []

    async def guard(self, research_id):
        return self.store.project(research_id)

    validate_discussion_output = staticmethod(validate_discussion_handoff)

    async def turn(self, rid, member, role, did, tid, schema, context, **options):
        self.calls.append((member, schema.__name__, deepcopy(context)))
        output = schema.model_validate(self.callback(member, schema.__name__, context)).model_dump()
        if options.get("output_validator"):
            options["output_validator"](output)
        return output


def make_project(store):
    return store.start_project(store.create_project({"client_request_id": "incident", "topic": "incident", "mode": "demo"})["id"])


async def discuss(engine, project):
    return await MemberDiscussion(engine).run(project["id"], "incident-discussion", "critic", ["A", "B", "C"],
        {"deadline_at": project["deadline_at"]}, None, ["A", "B", "C"])


@pytest.mark.asyncio
@pytest.mark.parametrize("crash_after_failure", [False, True])
async def test_three_yes_votes_for_incomplete_plan_return_to_authors_once(tmp_path, monkeypatch, crash_after_failure):
    store = Store(tmp_path)
    project = make_project(store)
    def reply(member, schema, context):
        if schema == "DiscussionTurn":
            if context["failed_end_votes"]:
                failure = context["last_end_vote_failure"]
                assert "question、fixed、metrics" in failure["error"]
                assert all(ballot["approve"] for ballot in failure["ballots"])
            return {"content_markdown": "补齐实际问题、配置与指标", "end_vote": {
                "final_output": COMPLETE if context["failed_end_votes"] else INCOMPLETE}}
        return {"approve": True, "next_step": "implementation"}
    engine = IncidentEngine(store, reply)
    original_commit, crashed = store.commit_discussion_state, False
    def commit(previous, state, messages=()):
        nonlocal crashed
        saved = original_commit(previous, state, messages)
        if crash_after_failure and state["failed_end_votes"] == 1 and previous["failed_end_votes"] == 0 and not crashed:
            crashed = True
            raise RuntimeError("crash after failure commit")
        return saved
    monkeypatch.setattr(store, "commit_discussion_state", commit)
    if crash_after_failure:
        with pytest.raises(RuntimeError, match="after failure"):
            await discuss(engine, project)
    result = await discuss(engine, project)
    assert result["final_output"] == COMPLETE
    state = store.get("discussion_state", "incident-discussion")
    assert state["failed_end_votes"] == 1
    first = state["votes"][0]
    assert first["failure_kind"] == "handoff_invalid"
    assert all(ballot["approve"] for ballot in first["ballots"].values())
    assert first["output_hash"] != state["votes"][1]["output_hash"]
    assert Counter(member for member, schema, _ in engine.calls if schema == "EndBallot") == {"A": 2, "B": 2, "C": 2}
    assert [member for member, schema, _ in engine.calls if schema == "DiscussionTurn"] == ["A", "B", "C", "A"]
    visible = store.messages("incident-discussion", limit=100)["items"]
    assert any("转 Implementation 缺少 question、fixed、metrics，已退回 Critic" in item["content_markdown"] for item in visible)
    assert len(store.list("discussion_handoff", project["id"])) == 1


@pytest.mark.asyncio
async def test_incomplete_plan_can_return_to_exploration_without_becoming_executable(tmp_path):
    store = Store(tmp_path)
    project = make_project(store)
    def reply(member, schema, context):
        return {"content_markdown": "请重新探索", "end_vote": {"final_output": {**INCOMPLETE, "experiment_spec": {"fixed": None}}}} if schema == "DiscussionTurn" else {
            "approve": True, "next_step": "exploration"}
    result = await discuss(IncidentEngine(store, reply), project)
    assert result["next_step"] == "exploration"
    assert store.get("discussion_state", "incident-discussion")["failed_end_votes"] == 0


@pytest.mark.asyncio
async def test_legacy_twenty_one_product_errors_do_not_erase_real_votes_or_consume_budget(tmp_path):
    store = Store(tmp_path)
    project = make_project(store)
    interrupted = False
    def reply(member, schema, context):
        nonlocal interrupted
        if schema == "DiscussionTurn":
            return {"content_markdown": "原组修订", "end_vote": {
                "final_output": COMPLETE if context["failed_end_votes"] else INCOMPLETE}}
        if member == "C" and not interrupted:
            interrupted = True
            raise RuntimeBlocked("model_timeout", "legacy worker stopped here")
        return {"approve": True, "next_step": "implementation"}
    engine = IncidentEngine(store, reply)
    with pytest.raises(RuntimeBlocked, match="legacy worker"):
        await discuss(engine, project)
    original = store.get("discussion_state", "incident-discussion")["active_vote"]
    assert list(original["ballots"]) == ["A", "B"]
    turn = "ballot:" + original["id"] + ":C"
    key = digest(["incident-discussion", "C", turn, "EndBallot"])
    store.put("discussion_model_call", key, {"technical_attempt": 21,
        "last_error": "旧产物缺少 question、fixed、metrics"}, project["id"])
    await discuss(engine, project)
    state = store.get("discussion_state", "incident-discussion")
    assert state["failed_end_votes"] == 1
    assert state["votes"][0]["id"] == original["id"]
    assert state["votes"][0]["output_hash"] == original["output_hash"]
    assert state["votes"][0]["ballots"]["A"] == original["ballots"]["A"]
    assert Counter(member for member, schema, _ in engine.calls if schema == "EndBallot") == {"A": 2, "B": 2, "C": 3}


@pytest.mark.asyncio
async def test_third_unusable_product_registers_one_finalization_in_same_commit(tmp_path, monkeypatch):
    store = Store(tmp_path)
    project = make_project(store)
    def reply(member, schema, context):
        if schema == "DiscussionTurn":
            return {"content_markdown": "审议修订", "end_vote": {"final_output": INCOMPLETE}}
        if schema == "EndBallot":
            return {"approve": True, "next_step": "implementation"}
        assert schema == "DiscussionFinal"
        return {"next_step": "implementation", "final_output": COMPLETE}
    engine = IncidentEngine(store, reply)
    original_commit, crashed = store.commit_discussion_state, False
    def commit(previous, state, messages=()):
        nonlocal crashed
        saved = original_commit(previous, state, messages)
        if state["mode"] == "god" and not crashed:
            crashed = True
            assert store.list("discussion_finalization", project["id"])[0]["status"] == "pending"
            raise RuntimeError("crash after third failure")
        return saved
    monkeypatch.setattr(store, "commit_discussion_state", commit)
    with pytest.raises(RuntimeError, match="third failure"):
        await discuss(engine, project)
    result = await discuss(engine, project)
    assert result["decision_source"] == "god"
    assert store.get("discussion_state", "incident-discussion")["failed_end_votes"] == 3
    assert len(store.list("discussion_finalization", project["id"])) == 1
    assert len(store.list("discussion_handoff", project["id"])) == 1
    assert sum(schema == "DiscussionFinal" for _, schema, _ in engine.calls) == 1


@pytest.mark.asyncio
async def test_generation_budget_covers_repeated_field_errors_and_restart(tmp_path):
    store = Store(tmp_path)
    project = make_project(store)
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(project["id"], project["deadline_at"])
    calls = []
    async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "cp.sqlite")) as saver:
        engine = ResearchEngine(store, control, None, None, saver)
        async def malformed(**arguments):
            calls.append(arguments["turn_id"])
            return {"approve": "true"}
        engine.demo._produce_turn = malformed
        context = {"no_arbitration": True, "discussion_protocol": 2,
            "generation_budget": {"kind": "test", "key": "fixed-vote", "limit": 3}}
        try:
            for _ in range(2):
                with pytest.raises(RuntimeBlocked) as error:
                    await engine.turn(project["id"], "A", "critic", "same-task", "same-turn", EndBallot, context, publish_message=False)
                assert error.value.code == "generation_budget_exhausted"
        finally:
            await engine.close()
    control.close()
    assert len(calls) == 3 and len(set(calls)) == 3
    budget = store.list("generation_budget", project["id"])[0]
    assert len(budget["attempts"]) == 3
    assert all(item["output"] == {"approve": "true"} and item["error"] for item in budget["attempts"])


def test_unconfirmed_occupied_slots_consume_budget_and_demo_receipt_is_reused(tmp_path):
    store = Store(tmp_path)
    project = make_project(store)
    budget = GenerationBudget(store, project["id"], {"kind": "test", "key": "lost-responses", "limit": 3})
    for expected in range(1, 4):
        attempt, output = budget.acquire("A", "task", "turn")
        assert attempt["number"] == expected and output is None
    restarted = GenerationBudget(store, project["id"], budget.binding)
    with pytest.raises(RuntimeBlocked) as error:
        restarted.acquire("A", "task", "turn:technical-99")
    assert error.value.code == "generation_budget_exhausted"
    output = {"approve": True}
    store.put("execution", "demo-" + attempt["execution_key"], {"status": "completed", "output": output}, project["id"])
    reused, answer = restarted.acquire("A", "task", "turn")
    assert reused["attempt_id"] == attempt["attempt_id"] and answer == output


def test_explicit_service_failure_before_registration_keeps_infrastructure_backoff(tmp_path):
    store = Store(tmp_path)
    project = make_project(store)
    budget = GenerationBudget(store, project["id"], {"kind": "test", "key": "service", "limit": 3})
    for _ in range(4):
        attempt, _ = budget.acquire("A", "task", "turn")
        assert attempt["number"] == 1
        assert budget.release_unstarted_service_failure(attempt, ConnectionError("service unavailable"))
    attempt, _ = budget.acquire("A", "task", "turn")
    store.put("execution", attempt["execution_key"], {"status": "unknown"}, project["id"])
    assert not budget.release_unstarted_service_failure(attempt, TimeoutError("response lost"))
    assert len(store.get("generation_budget", budget.key)["attempts"]) == 1
    with pytest.raises(RuntimeBlocked) as error:
        budget.acquire("A", "task", "turn")
    assert error.value.code == "execution_unknown"
