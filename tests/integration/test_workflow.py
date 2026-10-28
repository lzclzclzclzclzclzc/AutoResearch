"""A real local CPU process; only model conversations are explicit fixtures."""
import asyncio
from pathlib import Path

import pytest
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from backend.graphs.engine import ResearchEngine
from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor
from backend.runtime.runner import RunnerSupervisor
from backend.services.credentials import CredentialService


@pytest.mark.asyncio
async def test_complete_demo_workflow_is_labelled_and_runs_real_cpu(tmp_path):
    store = Store(tmp_path)
    project = store.create_project({"client_request_id": "cpu-demo", "topic": "演示 CPU 工作流", "mode": "demo",
        "team": {"n": 2, "m1": 2, "m2": 2, "boss_members": 3, "k": 3}, "max_runtime_seconds": 120})
    control = ControlSupervisor(tmp_path / "control")
    runner = RunnerSupervisor(tmp_path, control=control)
    try:
        async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "checkpoints.sqlite")) as saver:
            engine = ResearchEngine(store, control, runner, CredentialService(tmp_path, store), saver)
            supervisor = next(t for t in store.pending_tasks() if t["kind"] == "supervisor")
            await engine.supervisor(supervisor)
            store.finish_task(supervisor["id"])
            deadline = store.project(project["id"])["deadline_at"]
            lines = [t for t in store.pending_tasks() if t["kind"] == "line"]
            assert len(lines) == 2
            await asyncio.gather(*(engine.line(task) for task in lines))
            for task in lines:
                store.finish_task(task["id"])
            for task in [t for t in store.pending_tasks() if t["kind"] == "boss"]:
                await engine.boss(task)
                store.finish_task(task["id"])
            for task in [t for t in store.pending_tasks() if t["kind"] == "paper"]:
                await engine.paper(task)
                store.finish_task(task["id"])
            assert store.project(project["id"])["deadline_at"] == deadline
            assert all(line["phase"] == "completed" for line in store.lines(project["id"]))
            submissions = store.list("submission", project["id"])
            assert len(submissions) == 2
            assert all(s["run"]["exit_code"] == 0 for s in submissions)
            assert all(s["metrics"]["final_mse"] < s["metrics"]["baseline_mse"] for s in submissions)
            assert len(store.list("verification", project["id"])) == 2
            assert len(store.list("report", project["id"])) == 2
            assert store.list("memory_node", project["id"])
            cycles_before = store.list("cycle", project["id"])
            tasks_before = store.list("task", project["id"])
            # Replay after downstream work has already advanced: no extra run,
            # cycle reset, epoch increment, artifact mutation or fresh model turn.
            await engine.supervisor(supervisor)
            for task in lines:
                await engine.line(task)
            assert store.list("cycle", project["id"]) == cycles_before
            assert store.list("task", project["id"]) == tasks_before
            assert len(store.list("run", project["id"])) == 2
            for discussion in store.list("discussion", project["id"]):
                records = discussion["records"]
                for member in discussion["members"]:
                    assert f"proposal:1:{member}" in records
                    assert f"review:1:{member}" in records
                    assert f"vote:1:{member}" in records
            await engine.close()
    finally:
        runner.close()
        control.close()


@pytest.mark.asyncio
async def test_stop_before_first_dispatch_syncs_without_starting_clock_or_agents(tmp_path):
    from backend.runtime.worker import sync_control_stops
    from backend.adapters.dsh import ResearchStopped
    store = Store(tmp_path)
    project = store.create_project({"client_request_id": "stop-created", "topic": "Created project stop", "mode": "demo"})
    control = ControlSupervisor(tmp_path / "control")
    control.terminate(project["id"], "human_terminate")
    try:
        async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "checkpoints.sqlite")) as saver:
            engine = ResearchEngine(store, control, None, CredentialService(tmp_path, store), saver)
            supervisor = next(t for t in store.pending_tasks() if t["kind"] == "supervisor")
            with pytest.raises(ResearchStopped):
                await engine.supervisor(supervisor)
            await sync_control_stops(store, control)
            final = store.project(project["id"])
            assert final["started_at"] is None and final["deadline_at"] is None
            assert final["status"] == "terminated"
            assert not store.list("execution", project["id"])
            assert not store.list("cycle", project["id"])
            await engine.close()
    finally:
        control.close()


@pytest.mark.asyncio
async def test_control_restart_resyncs_already_terminated_project_without_reopening(tmp_path):
    from backend.runtime.worker import sync_control_stops
    store = Store(tmp_path)
    project = store.create_project({"client_request_id": "stopped-restart", "topic": "Stopped restart", "mode": "demo"})
    rid = project["id"]
    control = ControlSupervisor(tmp_path / "control")
    try:
        control.terminate(rid, "user_requested")
        await sync_control_stops(store, control)
        initial = store.project(rid)
        assert initial["status"] == "terminated"
        assert control.status(rid)["db_synced"]
        control.close()
        control = ControlSupervisor(tmp_path / "control")
        assert not control.status(rid)["db_synced"]
        await sync_control_stops(store, control)
        final = store.project(rid)
        assert control.status(rid)["state"] == "terminated"
        assert final["terminated_at"] == initial["terminated_at"]
        assert final["control_generation"] == initial["control_generation"]
        assert len([e for e in store.events(rid) if e["type"] == "research.terminated"]) == 1
        assert not store.list("execution", rid) and not store.list("cycle", rid)
        assert not store.pending_tasks()
    finally:
        control.close()


@pytest.mark.asyncio
async def test_failed_run_repairs_preserve_attempts_and_single_verify_round(tmp_path):
    store = Store(tmp_path)
    project = store.create_project({"client_request_id": "repair-demo", "topic": "CPU repair fixture", "mode": "demo",
        "team": {"n": 1, "m1": 1, "m2": 1, "boss_members": 1, "k": 3}, "max_runtime_seconds": 120})
    control = ControlSupervisor(tmp_path / "control")
    runner = RunnerSupervisor(tmp_path, control=control)
    try:
        async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "checkpoints.sqlite")) as saver:
            engine = ResearchEngine(store, control, runner, CredentialService(tmp_path, store), saver)
            original = engine.demo._produce_turn
            async def fixture(**arguments):
                output = await original(**arguments)
                if arguments["schema"].__name__ == "Implementation" and arguments["task_id"].endswith("implementation-1"):
                    output["files"] = {"experiment.py": "raise RuntimeError('explicit local fixture failure')"}
                if arguments["schema"].__name__ == "RunAssessment":
                    output["action"] = "repair_implementation"
                return output
            engine.demo._produce_turn = fixture
            supervisor = next(t for t in store.pending_tasks() if t["kind"] == "supervisor")
            await engine.supervisor(supervisor)
            line = next(t for t in store.pending_tasks() if t["kind"] == "line")
            await engine.line(line)
            runs = store.list("run", project["id"])
            assert len(runs) == 2
            assert {run["state"] for run in runs} == {"execution_failed", "completed"}
            submission = store.list("submission", project["id"])[0]
            assert len(submission["run_ids"]) == 2 and len(submission["code_versions"]) == 2
            assert len(submission["run_logs"]) == 2
            assert len(store.list("verification", project["id"])) == 1
            assert store.list("cycle", project["id"])[0]["completed_verify_rounds"] == 1
            await engine.close()
    finally:
        runner.close()
        control.close()
