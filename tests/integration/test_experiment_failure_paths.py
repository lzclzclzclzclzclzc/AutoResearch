import asyncio
import sys
import time

import pytest
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from backend.adapters.dsh import ResearchStopped
from backend.graphs.engine import ResearchEngine
from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor
from backend.runtime.runner import ResourceConflict, RunnerSupervisor
from backend.services.credentials import CredentialService
from packages.contracts.research import ExperimentSpec, digest


@pytest.fixture
def runtime(tmp_path):
    store = Store(tmp_path)
    p = store.start_project(store.create_project({"client_request_id": "failure-path", "topic": "failure evidence fixture", "mode": "demo",
        "team": {"n": 1, "m1": 1, "m2": 1, "boss_members": 1, "k": 1}, "max_runtime_seconds": 120})["id"])
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(p["id"], p["deadline_at"])
    runner = RunnerSupervisor(tmp_path, control)
    yield store, p, control, runner
    runner.close()
    control.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("lost_response", [False, True])
async def test_admitted_process_creation_failure_is_packaged_and_verified_once(runtime, tmp_path, monkeypatch, lost_response):
    store, p, control, runner = runtime
    calls = []
    def fail_to_create(*args, **kwargs):
        calls.append(1)
        raise OSError("Injected process creation failure")
    monkeypatch.setattr(runner, "_process_factory", fail_to_create)
    if lost_response:
        original = runner.start
        def lose_response(payload):
            original(payload)
            raise ConnectionError("Lost terminal start response")
        monkeypatch.setattr(runner, "start", lose_response)
    async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "checkpoints.sqlite")) as saver:
        engine = ResearchEngine(store, control, runner, CredentialService(tmp_path, store), saver)
        supervisor = next(t for t in store.pending_tasks() if t["kind"] == "supervisor")
        await engine.supervisor(supervisor)
        task = next(t for t in store.pending_tasks() if t["kind"] == "line")
        await engine.line(task)
        await engine.line(task)
        submissions = store.list("submission", p["id"])
        reports = store.list("verification", p["id"])
        assert len(calls) == len(submissions) == len(reports) == 1
        run = submissions[0]["run"]
        assert run["state"] == "execution_failed" and "creation failure" in run["error"]
        assert run["terminal_proof"]["start_operation_terminal"] and run["terminal_proof"]["process_tree_exited"]
        assert submissions[0]["logs"]["log_status"] == "not_created"
        assert not reports[0]["passed"] and reports[0]["scientific_observation"] == "execution_failed"
        assert store.list("cycle", p["id"])[0]["completed_verify_rounds"] == 1
        assert runner.resources()["allocations"][0]["state"] == "released"
        assert not control.status(p["id"])["unfinished"]
        await engine.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["admission_denied", "project_stopped", "exit_unknown"])
async def test_denial_stop_and_unproven_cleanup_do_not_reach_verify(runtime, tmp_path, monkeypatch, failure):
    store, p, control, runner = runtime
    async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "checkpoints.sqlite")) as saver:
        engine = ResearchEngine(store, control, runner, CredentialService(tmp_path, store), saver)
        await engine.supervisor(next(t for t in store.pending_tasks() if t["kind"] == "supervisor"))
        task = next(t for t in store.pending_tasks() if t["kind"] == "line")
        state = {"research_id": p["id"], "task_id": task["id"], "revision": 1, **task["payload"]}
        for method in (engine.design, engine.implement, engine.organize):
            state.update(await method(state))
        if failure == "admission_denied":
            def denied(payload):
                raise ResourceConflict("Injected admission denial")
            monkeypatch.setattr(runner, "start", denied)
        elif failure == "project_stopped":
            def stopped(*args, **kwargs):
                control.terminate(p["id"])
                raise OSError("Stopped before creation")
            monkeypatch.setattr(runner, "_process_factory", stopped)
        else:
            class UncertainProcess:
                def create_suspended(self):
                    raise OSError("Injected unknown process state")
                def terminate(self):
                    return False
                def close(self):
                    pass
            monkeypatch.setattr(runner, "_process_factory", lambda *a, **k: UncertainProcess())
        with pytest.raises((ResourceConflict, ResearchStopped)):
            await engine.run_experiment(state)
        assert not store.list("submission", p["id"]) and not store.list("verification", p["id"])
        if failure == "exit_unknown":
            assert runner.resources()["allocations"][0]["state"] != "released"
        await engine.close()


async def eventually(predicate):
    end = time.monotonic() + 8
    while time.monotonic() < end:
        if predicate():
            return
        await asyncio.sleep(0.05)
    assert predicate()


@pytest.mark.asyncio
async def test_incremental_monitor_reads_past_64k_and_reuses_window_after_failed_cursor_commit(runtime, tmp_path, monkeypatch):
    store, p, control, runner = runtime
    rid = p["id"]
    spec = ExperimentSpec(spec_id="monitor-spec", version=1, question="monitor logs", hypothesis="fixture", fixed={},
        baseline="none", data={"source": "fixture"}, metrics=["status"], design={}, monitoring={}, completion_criteria="fixture", deadline_at=p["deadline_at"])
    store.put("spec", spec.spec_id, spec.model_dump(), rid)
    request = {"research_id": rid, "line_id": "line-01", "implementation_agent_id": "line-01/implementation",
        "spec_id": spec.spec_id, "spec_version": 1, "spec_hash": digest(spec.model_dump()), "cpu_cores": 1,
        "memory_bytes": 134217728, "control_generation": 1, "resource_request_id": "monitor-resources", "gpu_count": 0}
    allocation = runner.request_allocation(request)
    program = "import pathlib,time; print('HEAD_ONLY_'+('x'*90000),flush=True)\nwhile not pathlib.Path('trigger').exists(): time.sleep(.05)\nprint('LATEST_ONLY_SIGNAL: divergence',flush=True)\ntime.sleep(60)"
    run = runner.start({**allocation, "experiment_run_id": "monitor-run", "start_operation_id": "monitor-start",
        "cwd": str(tmp_path), "command": [sys.executable, "-u", "-c", program], "code_version": "monitor-code"})
    await eventually(lambda: runner.collect("monitor-run")["log_size"] > 90000)
    async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "checkpoints.sqlite")) as saver:
        engine = ResearchEngine(store, control, runner, CredentialService(tmp_path, store), saver)
        state = {"research_id": rid, "line_id": "line-01", "spec_id": spec.spec_id}
        original_model, observations = engine.demo._produce_turn, []
        async def model(**args):
            result = await original_model(**args)
            observations.append(args["context"]["logs"])
            if "LATEST_ONLY_SIGNAL" in args["context"]["logs"]["text"]:
                result.update(request_early_stop=True, rule_id="execution_unusable")
            return result
        monkeypatch.setattr(engine.demo, "_produce_turn", model)
        original_put, failed = store._put, False
        def fail_cursor(c, kind, *args, **kwargs):
            nonlocal failed
            if kind == "run_monitor_cursor" and not failed:
                failed = True
                raise OSError("Injected cursor commit failure")
            return original_put(c, kind, *args, **kwargs)
        monkeypatch.setattr(store, "_put", fail_cursor)
        with pytest.raises(OSError, match="cursor commit"):
            await engine.monitor_run(state, run)
        assert not store.get("run_monitor_cursor", "monitor-run")
        (tmp_path / "trigger").touch()
        await eventually(lambda: "LATEST_ONLY_SIGNAL" in runner.collect("monitor-run", 90000)["text"])
        await engine.monitor_run(state, run)
        assert len(observations) == 1 and observations[0]["text_truncated"]
        first = store.get("run_monitor_cursor", "monitor-run")["next_offset"]
        await engine.monitor_run(state, run)
        assert observations[1]["offset"] == first
        assert "LATEST_ONLY_SIGNAL" in observations[1]["text"] and "HEAD_ONLY" not in observations[1]["text"]
        assert store.get("run_monitor_cursor", "monitor-run")["next_offset"] > first
        await eventually(lambda: runner.status("monitor-run")["state"] == "early_stopped")
        final = await engine.collect_run_logs("monitor-run")
        assert "LATEST_ONLY_SIGNAL" in final["text"] and final["text_truncated"]
        assert len(store.list("run_monitor", rid)) == 2
        await engine.close()
