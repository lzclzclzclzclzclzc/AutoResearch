"""Exercise the current protocol with labelled model fixtures and a real CPU run."""
import pytest
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from backend.graphs.engine import ResearchEngine
from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor
from backend.runtime.runner import RunnerSupervisor
from backend.services.credentials import CredentialService


@pytest.mark.asyncio
async def test_current_demo_votes_execute_verify_report_and_save_knowledge(tmp_path):
    store = Store(tmp_path)
    project = store.create_project({"client_request_id": "current-protocol-demo", "topic": "当前讨论工作流演示",
        "mode": "demo", "team": {"n": 1, "m1": 2, "m2": 2, "boss_members": 2, "k": 2},
        "max_runtime_seconds": 120})
    rid = project["id"]
    control = ControlSupervisor(tmp_path / "control")
    runner = RunnerSupervisor(tmp_path, control=control)
    try:
        async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "checkpoints.sqlite")) as saver:
            engine = ResearchEngine(store, control, runner, CredentialService(tmp_path, store), saver)
            try:
                initial = next(task for task in store.pending_tasks() if task["kind"] == "supervisor")
                await engine.supervisor(initial)
                store.finish_task(initial["id"])
                line = next(task for task in store.pending_tasks() if task["kind"] == "line")
                await engine.line(line)
                store.finish_task(line["id"])
                reviews = [task for task in store.pending_tasks() if task["kind"] == "boss"]
                assert len(reviews) == 1
                await engine.boss(reviews[0])
                store.finish_task(reviews[0]["id"])
                papers = [task for task in store.pending_tasks() if task["kind"] == "paper"]
                assert len(papers) == 1
                await engine.paper(papers[0])
                store.finish_task(papers[0]["id"])

                submissions = store.list("submission", rid)
                assert len(submissions) == 1 and submissions[0]["spec"]["discussion_protocol"] == 2
                assert submissions[0]["run"]["exit_code"] == 0
                assert submissions[0]["metrics"]["final_mse"] < submissions[0]["metrics"]["baseline_mse"]
                assert store.list("verification", rid)[0]["passed"]
                assert len(store.list("report", rid)) == 1
                assert len(store.list("memory_node", rid)) == 1
                assert store.list("memory_node", rid)[0]["source_ids"] == [submissions[0]["id"]]
                assert store.lines(rid)[0]["phase"] == "completed"
                discussions = store.list("discussion_state", rid)
                assert len(discussions) == 4  # Initial Boss, Exploration, Critic, review Boss.
                for discussion in discussions:
                    assert discussion["mode"] == "ended" and discussion["failed_end_votes"] == 0
                    assert [vote["type"] for vote in discussion["votes"]] == ["ordinary", "end"]
                    assert all(len(vote["ballots"]) == len(discussion["members"]) for vote in discussion["votes"])
                    messages = store.messages(discussion["id"], limit=200)["items"]
                    assert {message.get("vote", {}).get("type") for message in messages} >= {"ordinary", "end"}
                assert not store.list("arbitration_request", rid)
                assert not any(execution.get("turn_id") in {"freeze_spec", "synthesize", "respond_to_critic"}
                    for execution in store.list("execution", rid))

                before = {kind: len(store.list(kind, rid)) for kind in ("run", "cycle", "report", "memory_node", "execution")}
                await engine.supervisor(initial)
                await engine.line(line)
                await engine.boss(reviews[0])
                assert {kind: len(store.list(kind, rid)) for kind in before} == before
            finally:
                await engine.close()
    finally:
        runner.close()
        control.close()
