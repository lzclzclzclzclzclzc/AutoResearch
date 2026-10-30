import asyncio

import pytest
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from backend.adapters.dsh import RuntimeBlocked
from backend.graphs.discussion import speaking_order
from backend.graphs.engine import ResearchEngine
from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor
from backend.services.credentials import CredentialService


def test_discussion_order_is_a_reproducible_permutation():
    members = ["a", "b", "c"]
    assert sorted(speaking_order(members, "seed")) == members
    assert speaking_order(members, "seed") == speaking_order(members, "seed")


@pytest.mark.asyncio
@pytest.mark.parametrize("resolve", [True, False])
async def test_blockers_get_revision_opportunity_before_vote_and_cannot_be_omitted(tmp_path, resolve):
    store = Store(tmp_path)
    p = store.start_project(store.create_project({"client_request_id": "serial", "topic": "fixture", "mode": "demo",
        "team": {"boss_members": 3}, "max_runtime_seconds": 120})["id"])
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(p["id"], p["deadline_at"])
    try:
        async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "checkpoints.sqlite")) as saver:
            engine = ResearchEngine(store, control, None, CredentialService(tmp_path, store), saver)
            original = engine.demo._produce_turn
            in_speech, maximum, calls, observed_context = 0, 0, [], []
            async def fixture(**args):
                nonlocal in_speech, maximum
                public = args["schema"].__name__ != "Willingness"
                if public:
                    in_speech += 1
                    maximum = max(maximum, in_speech)
                    observed_context.append(args["context"].get("conversation_snapshot", []))
                    assert args["agent_id"] in args["prompt"]
                try:
                    output = await original(**args)
                    calls.append((args["schema"].__name__, args["turn_id"], args["agent_id"]))
                    if args["schema"].__name__ == "BossReview":
                        round_no = int(args["turn_id"].split(":")[1])
                        if round_no == 1 or resolve:
                            issue = {"issue_id": "fix-validation", "category": "must_fix", "description": "固定验证口径",
                                "resolved": resolve and round_no > 1, "resolution": "候选已修订为预先固定验证口径" if round_no > 1 and resolve else "", "experiment_mapping": ""}
                            output["issues"] = [issue]
                        # An omitted issue in later rounds must remain blocking.
                        if resolve and round_no == 2:
                            candidate = dict(args["context"]["proposals"][args["agent_id"]]["candidate"])
                            candidate["rationale"] = "修订后的验证口径"
                            output["revised_candidate"] = candidate
                    return output
                finally:
                    if public:
                        in_speech -= 1
            engine.demo._produce_turn = fixture
            members = engine.members(p, "boss")
            args = (p["id"], "test-discussion", "boss", members, {"initial": True, "lines": store.lines(p["id"]), "topic": "fixture"})
            if resolve:
                result = await engine.discussions.run(*args)
                assert result["selected_payload"]["rationale"] == "修订后的验证口径"
                before_calls = len(calls)
                selection_before = store.list("discussion_selection", p["id"])
                assert await engine.discussions.run(*args) == result
                assert len(calls) == before_calls and store.list("discussion_selection", p["id"]) == selection_before
                assert any(c[0] == "Vote" for c in calls)
            else:
                with pytest.raises(RuntimeBlocked, match="fix-validation") as error:
                    await engine.discussions.run(*args)
                assert error.value.code == "arbitration_needs_external_input"
                request = store.list("arbitration_request", p["id"])[0]
                assert request["reason_code"] == "design_deadlock"
                assert request["status"] == "needs_external_input"
                assert not any(c[0] == "Vote" for c in calls)
            assert maximum == 1
            messages = store.messages("test-discussion:serial-v2", limit=200)["items"]
            assert len(messages) == len([c for c in calls if c[0] not in {"Willingness", "GodDecision"}])
            assert all('"score"' not in m["content_markdown"] for m in messages)
            assert any(observed_context) and all(m["speaker"] for ctx in observed_context for m in ctx)
            selections = store.list("discussion_selection", p["id"])
            assert len(selections) == len([c for c in calls if c[0] in {"BossProposal", "BossReview", "Vote", "Synthesis"}])
            assert not any(c[0] == "Willingness" for c in calls)
            order = store.get("discussion_order", "test-discussion:serial-v2")["members"]
            for phase_round in {(s['phase'], s['round']) for s in selections if s['phase'] != 'synthesis'}:
                assert [s['speaker'] for s in selections if (s['phase'],s['round']) == phase_round] == order
            await engine.close()
    finally:
        control.close()


@pytest.mark.asyncio
async def test_selection_survives_interruption_without_redrawing_or_duplicate_messages(tmp_path):
    store = Store(tmp_path)
    p = store.start_project(store.create_project({"client_request_id": "resume", "topic": "fixture", "mode": "demo",
        "team": {"boss_members": 2}, "max_runtime_seconds": 120})["id"])
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(p["id"], p["deadline_at"])
    try:
        async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "checkpoints.sqlite")) as saver:
            engine = ResearchEngine(store, control, None, CredentialService(tmp_path, store), saver)
            original = engine.demo._produce_turn
            interrupted = False
            async def fixture(**args):
                nonlocal interrupted
                if args["schema"].__name__ == "BossProposal" and not interrupted:
                    interrupted = True
                    raise RuntimeBlocked("fixture_interrupted", "Injected interruption after committed selection")
                return await original(**args)
            engine.demo._produce_turn = fixture
            args = (p["id"], "recover-discussion", "boss", engine.members(p, "boss"), {"initial": True, "lines": store.lines(p["id"])})
            with pytest.raises(RuntimeBlocked, match="Injected interruption"):
                await engine.discussions.run(*args)
            before = store.list("discussion_selection", p["id"])
            assert len(before) == 1
            await engine.discussions.run(*args)
            assert store.get("discussion_selection", before[0]["id"]) == before[0]
            messages = store.messages("recover-discussion:serial-v2", limit=100)["items"]
            assert messages[0]["sender"]["id"] == before[0]["speaker"]
            assert len(messages) == 7 and len({m["source_event_id"] for m in messages}) == 7
            await engine.close()
    finally:
        control.close()
