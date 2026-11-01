"""Memory expansion keeps real Runner receipts and never changes scientific code."""
import asyncio
import os
import sys
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from backend.adapters.dsh import ResearchStopped, RuntimeBlocked
from backend.graphs.engine import ResearchEngine
from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor
from backend.runtime.runner import ResourceConflict, RunnerSupervisor, host_memory_failure
from backend.services.credentials import CredentialService
from packages.contracts.research import ExperimentSpec, digest

MIB = 1024 * 1024


def setup_engine(tmp_path, code="pass", memory_mb=64):
    store = Store(tmp_path)
    project = store.start_project(store.create_project({"client_request_id": "memory-expansion", "topic": "memory fixture",
        "mode": "demo", "max_runtime_seconds": 180, "team": {"n": 1, "m1": 1, "m2": 1, "boss_members": 1, "k": 2}})["id"])
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(project["id"], project["deadline_at"])
    plan = ExperimentSpec(spec_id="spec", version=1, question="memory fixture", fixed={}, metrics=["exit"],
        deadline_at=project["deadline_at"], discussion_protocol=2)
    store.put("spec", "spec", plan.model_dump(), project["id"])
    cwd = tmp_path / "source"
    cwd.mkdir()
    (cwd / "main.py").write_text(code, encoding="utf-8")
    files = {"main.py": code}
    store.put("implementation", "impl", {"files": files, "entrypoint": "main.py", "arguments": [], "cwd": str(cwd),
        "code_version": digest(files), "cpu_cores": 1, "memory_mb": memory_mb, "gpu_count": 0,
        "estimated_seconds": 10, "actual_config": {}, "spec_id": "spec"}, project["id"])
    store.put("cycle", "cycle", {"line_id": "line-01", "direction_epoch": 1, "k": 2,
        "completed_verify_rounds": 0, "status": "active"}, project["id"])
    store.update_line(project["id"], "line-01", cycle_id="cycle")
    state = {"research_id": project["id"], "line_id": "line-01", "spec_id": "spec", "implementation_id": "impl",
        "direction_epoch": 1, "cycle_id": "cycle", "revision": 1}
    return store, control, state


@pytest.mark.asyncio
@pytest.mark.skipif(os.name != "nt", reason="actual Job memory limits require the Windows Runner")
async def test_real_windows_host_limit_fails_then_same_code_succeeds_with_doubled_memory(tmp_path, monkeypatch):
    # Incremental allocations touch every page and exceed 64 MiB including the
    # interpreter, but stay below 128 MiB. The partial file detects cwd reuse.
    code = """import json,pathlib
assert not pathlib.Path('partial.tmp').exists(), 'previous attempt output leaked'
pathlib.Path('partial.tmp').write_text('partial output')
blocks=[]
for i in range(80):
    block=bytearray(1024*1024)
    block[::4096]=b'x'*256
    blocks.append(block)
print('touched 80 MiB',flush=True)
pathlib.Path('metrics.json').write_text(json.dumps({'allocated_mib':80}))
"""
    store, control, state = setup_engine(tmp_path, code)
    runner = RunnerSupervisor(tmp_path, control, capacity={"cpu_cores": 1, "memory_total_bytes": 512 * MIB, "gpus": []})
    checkpoint_path = str(tmp_path / "checkpoints.sqlite")
    saver_context = AsyncSqliteSaver.from_conn_string(checkpoint_path)
    saver = await saver_context.__aenter__()
    engine = ResearchEngine(store, control, runner, CredentialService(tmp_path, store), saver)
    try:
        state.update(await engine.organize(state))
        state.update(await engine.run_experiment(state))
        first_id = state["run_id"]
        first = store.get("run", first_id)
        assert first["state"] == "execution_failed" and first["memory_bytes"] == 64 * MIB
        assert first["oom_evidence"]["kind"] == "host_memory_limit"
        assert first["memory_observation"]["peak_job_memory_bytes"] > 0
        assert first["terminal_proof"]["process_tree_exited"]
        transition = await engine.assess_run(state)
        assert transition == {"run_route": "retry_memory", "experiment_attempt": 2, "memory_budget_bytes": 128 * MIB}
        assert await engine.assess_run(state) == transition  # retry decision survives replay
        assert len(store.list("memory_retry")) == 1
        state.update(transition)
        # A Worker restart discards the engine, not the persisted decision or IDs.
        await engine.close()
        await saver_context.__aexit__(None, None, None)
        saver_context = AsyncSqliteSaver.from_conn_string(checkpoint_path)
        saver = await saver_context.__aenter__()
        engine = ResearchEngine(store, control, runner, CredentialService(tmp_path, store), saver)
        state.update(await engine.organize(state))
        state.update(await engine.run_experiment(state))
        second = store.get("run", state["run_id"])
        assert second["state"] == "completed" and second["memory_bytes"] == 128 * MIB
        assert second["code_version"] == first["code_version"]
        assert second["spec_hash"] == first["spec_hash"] and second["command"] == first["command"]
        assert second["cwd"] != first["cwd"] and second["allocation_id"] != first["allocation_id"]
        assert second["start_operation_id"] != first["start_operation_id"]
        assert Path(first["cwd"], "partial.tmp").is_file() and not Path(first["cwd"], "metrics.json").exists()
        assert "MemoryError" in runner.collect(first_id)["text"]
        assert "touched 80 MiB" in runner.collect(state["run_id"])["text"]
        await engine.run_experiment(state)  # original operation only, no duplicate launch
        assert len(runner.status()["runs"]) == 2
        state.update(await engine.assess_run(state))
        assert state["run_route"] == "package"
        state.update(await engine.package(state))
        submission = store.get("submission", state["submission_id"])
        assert len(submission["run_ids"]) == len(submission["run_logs"]) == 2
        assert submission["metrics"] == {"allocated_mib": 80}
        original = engine.demo._produce_turn
        async def fixture(**args):
            if args["schema"].__name__ == "Verification":
                return {"content_markdown": "两次真实运行同代码；首次内存限额失败、第二次成功，局限是单次内存夹具。",
                    "passed": True, "scientific_observation": "no_improvement"}
            return await original(**args)
        monkeypatch.setattr(engine.demo, "_produce_turn", fixture)
        await engine.verify(state)
        await engine.verify(state)
        assert store.get("cycle", "cycle")["completed_verify_rounds"] == 1
        assert len(store.list("verification")) == 1
    finally:
        await engine.close()
        await saver_context.__aexit__(None, None, None)
        runner.close()
        control.close()


def terminal_oom(state, attempt, memory):
    run_id = f"impl:run-{attempt}"
    return {"experiment_run_id": run_id, "state": "execution_failed", "experiment_attempt": attempt,
        "memory_bytes": memory, "allocation_id": f"a-{attempt}", "exit_code": 1,
        "terminal_proof": {"process_tree_exited": True, "start_operation_terminal": True},
        "oom_evidence": {"kind": "host_memory_limit", "source": "windows_job_memory_limit",
            "experiment_run_id": run_id, "approved_memory_bytes": memory}}


@pytest.mark.asyncio
@pytest.mark.parametrize("cap,expected_budgets,reason", [
    (1024 * MIB, [128 * MIB, 256 * MIB], "retry_limit_reached"),
    (128 * MIB, [128 * MIB], "capacity_limit_reached"),
    (64 * MIB, [], "capacity_limit_reached"),
])
async def test_persistent_oom_retries_at_most_twice_and_honors_capacity(tmp_path, cap, expected_budgets, reason):
    store, control, state = setup_engine(tmp_path)
    active = []
    runner = SimpleNamespace(resources=lambda: {"capacity": {"memory_total_bytes": cap}, "allocations": active})
    engine = ResearchEngine(store, control, runner, CredentialService(tmp_path, store), None)
    try:
        budget = 64 * MIB
        for attempt in range(1, len(expected_budgets) + 2):
            run = terminal_oom(state, attempt, budget)
            state.update(run_id=run["experiment_run_id"], experiment_attempt=attempt)
            active[:] = [{"allocation_id": run["allocation_id"], "state": "released"}]
            result = await engine.memory_retry_decision(state, run)
            if attempt <= len(expected_budgets):
                assert result["run_route"] == "retry_memory" and result["memory_budget_bytes"] == expected_budgets[attempt-1]
                budget = result["memory_budget_bytes"]
            else:
                assert result == {"run_route": "package"}
                assert store.get("memory_retry", state["run_id"])["stop_reason"] == reason
    finally:
        await engine.close()
        control.close()


@pytest.mark.parametrize("failure", ["gpu", "script", "oom_word", "fabricated_memory_error"])
def test_non_host_failures_do_not_acquire_oom_evidence(tmp_path, failure):
    messages = {"gpu": "Traceback (most recent call last):\nRuntimeError: CUDA out of memory",
        "script": "Traceback (most recent call last):\nValueError: bad input", "oom_word": "OOM reported by application",
        "fabricated_memory_error": "Traceback (most recent call last):\nMemoryError"}
    log = tmp_path / "run.log"
    log.write_text(messages[failure])
    run = {"state": "execution_failed", "exit_code": 1, "experiment_run_id": "r", "memory_bytes": 128 * MIB,
        "log_path": str(log), "memory_observation": {"approved_memory_bytes": 128 * MIB,
            "peak_job_memory_bytes": 16 * MIB, "memory_limit_events": []}}
    assert host_memory_failure(run) is None


@pytest.mark.asyncio
async def test_resource_wait_reuses_request_and_user_stop_cancels_queue(tmp_path):
    store, control, state = setup_engine(tmp_path)
    calls, queued, polled_again = [], asyncio.Event(), asyncio.Event()
    loop = asyncio.get_running_loop()
    def allocation(request):
        calls.append(request.copy())
        loop.call_soon_threadsafe(queued.set)
        if len(calls) >= 2:
            loop.call_soon_threadsafe(polled_again.set)
        return {**request, "allocation_id": "queue", "state": "queued"}
    runner = SimpleNamespace(request_allocation=allocation)
    engine = ResearchEngine(store, control, runner, CredentialService(tmp_path, store), None)
    try:
        state.update(experiment_attempt=2, memory_budget_bytes=128 * MIB, run_id="impl:run-1")
        request = {"resource_request_id": "impl:attempt-2:resources", "memory_bytes": 128 * MIB}
        task = asyncio.create_task(engine.wait_for_allocation(state, request))
        await asyncio.wait_for(queued.wait(), timeout=10)
        # A queued allocation does not claim another retry decision or new ID.
        assert not store.list("memory_retry")
        await asyncio.wait_for(polled_again.wait(), timeout=10)
        control.terminate(state["research_id"])
        with pytest.raises(ResearchStopped):
            await task
        assert len(calls) >= 2 and {c["resource_request_id"] for c in calls} == {request["resource_request_id"]}
    finally:
        await engine.close()
        control.close()


@pytest.mark.asyncio
async def test_actual_runner_releases_queued_expansion_when_project_stops(tmp_path):
    store, control, state = setup_engine(tmp_path)
    runner = RunnerSupervisor(tmp_path, control, capacity={"cpu_cores": 1, "memory_total_bytes": 128 * MIB, "gpus": []})
    engine = ResearchEngine(store, control, runner, CredentialService(tmp_path, store), None)
    try:
        spec = engine.spec(state)
        common = {"research_id": state["research_id"], "implementation_agent_id": "line-01/implementation",
            "spec_id": "spec", "spec_version": 1, "spec_hash": digest(spec.model_dump()), "control_generation": 1,
            "memory_bytes": 128 * MIB, "cpu_cores": 1, "gpu_count": 0}
        holder = await asyncio.to_thread(runner.request_allocation, {**common, "resource_request_id": "holder"})
        assert holder["state"] == "granted"
        request = {**common, "resource_request_id": "impl:attempt-2:resources"}
        queued = await asyncio.to_thread(runner.request_allocation, request)
        assert queued["state"] == "queued"
        assert (await asyncio.to_thread(runner.request_allocation, request))["allocation_id"] == queued["allocation_id"]
        control.terminate(state["research_id"])
        until = asyncio.get_running_loop().time() + 8
        while asyncio.get_running_loop().time() < until:
            allocations = (await asyncio.to_thread(runner.resources))["allocations"]
            if all(a["state"] == "expired" for a in allocations):
                break
            await asyncio.sleep(.1)
        assert all(a["state"] == "expired" for a in allocations)
        assert not runner.status()["runs"]
    finally:
        await engine.close()
        runner.close()
        control.close()


def test_unhandled_host_exception_requires_same_limit_and_measured_peak(tmp_path):
    log = tmp_path / "run.log"
    log.write_text("Traceback (most recent call last):\n  File 'main.py', line 9\nMemoryError\n")
    run = {"state": "execution_failed", "exit_code": 1, "experiment_run_id": "r", "memory_bytes": 128 * MIB,
        "log_path": str(log), "memory_observation": {"approved_memory_bytes": 128 * MIB,
            "peak_job_memory_bytes": 120 * MIB, "memory_limit_events": []}}
    assert host_memory_failure(run)["source"] == "unhandled_host_allocation_exception"
    assert host_memory_failure({**run, "state": "cancelled"}) is None
    assert host_memory_failure({**run, "memory_bytes": 256 * MIB}) is None


@pytest.mark.parametrize("change", [{"direction_epoch": 2}, {"cycle_id": "next-cycle"},
    {"finish_requested": True}, {"direction_stop_epoch": 1}])
def test_runner_rechecks_direction_and_cycle_in_start_admission(tmp_path, monkeypatch, change):
    store, control, state = setup_engine(tmp_path)
    runner = RunnerSupervisor(tmp_path, control)
    try:
        spec = store.get("spec", "spec")
        spec = ExperimentSpec.model_validate(spec)
        request = {"research_id": state["research_id"], "implementation_agent_id": "line-01/implementation",
            "line_id": "line-01", "spec_id": "spec", "spec_version": 1, "spec_hash": digest(spec.model_dump()),
            "resource_request_id": "direction-resources", "direction_epoch": 1, "cycle_id": "cycle",
            "memory_bytes": 64 * MIB, "cpu_cores": 1, "gpu_count": 0, "control_generation": 1}
        grant = runner.request_allocation(request)
        assert grant["state"] == "granted"
        store.update_line(state["research_id"], "line-01", **change)
        def forbidden(*args, **kwargs):
            pytest.fail("changed research direction must not create a process")
        monkeypatch.setattr(runner, "_process_factory", forbidden)
        with pytest.raises(ResourceConflict, match="direction or cycle changed"):
            runner.start({**grant, "experiment_run_id": "direction-run", "start_operation_id": "direction-start",
                "command": [sys.executable, "-c", "pass"], "cwd": str(tmp_path), "code_version": "code"})
        assert not runner.status()["runs"] and not runner.service.runs()
        # Requests already issued before the new fields were introduced remain
        # readable/validatable under their original plan-only contract.
        legacy = {k: v for k, v in request.items() if k not in {"direction_epoch", "cycle_id"}}
        with runner.service.connect() as c:
            runner.service._frozen_spec(c, legacy)
    finally:
        runner.close()
        control.close()


@pytest.mark.asyncio
async def test_engine_rechecks_line_after_grant_before_dispatching_start(tmp_path):
    store, control, state = setup_engine(tmp_path)
    allocation = {"allocation_id": "a", "state": "granted", "bound_run_id": None}
    store.put("allocation", "a", allocation, state["research_id"])
    state["allocation_id"] = "a"
    def grant(*args):
        store.update_line(state["research_id"], "line-01", direction_epoch=2)
        return allocation
    def forbidden(*args):
        pytest.fail("late direction change must be checked before dispatching Runner.start")
    runner = SimpleNamespace(request_allocation=grant, start=forbidden)
    engine = ResearchEngine(store, control, runner, CredentialService(tmp_path, store), None)
    try:
        with pytest.raises(RuntimeBlocked) as failure:
            await engine.run_experiment(state)
        assert failure.value.code == "task_superseded"
        assert not store.list("run")
    finally:
        await engine.close()
        control.close()


@pytest.mark.skipif(os.name != "nt", reason="Windows completion port API")
def test_memory_notifications_are_bounded_and_do_not_grow_the_retained_audit(monkeypatch):
    from backend.runtime import processes
    managed = processes.ManagedProcess.__new__(processes.ManagedProcess)
    managed._handle_lock = threading.RLock()
    managed.job, managed.completion_port, managed.closed = 1, 1, False
    managed._memory_bytes, managed._memory_limit_events = 64 * MIB, []
    calls = []
    def query(job, information_class, buffer, size, returned):
        buffer._obj.JobMemoryLimit = 64 * MIB
        buffer._obj.PeakJobMemoryUsed = 60 * MIB
        return True
    def endless_notifications(port, message, key, pid, timeout):
        calls.append(1)
        message._obj.value = 10
        pid._obj.value = 42
        return True
    monkeypatch.setattr(processes.kernel, "QueryInformationJobObject", query)
    monkeypatch.setattr(processes.kernel, "GetQueuedCompletionStatus", endless_notifications)
    try:
        first = managed.memory_snapshot()
        assert len(calls) == 1024 and len(first["memory_limit_events"]) == 32
        second = managed.memory_snapshot()
        assert len(calls) == 2048 and len(second["memory_limit_events"]) == 32
        assert second["approved_memory_bytes"] == 64 * MIB
    finally:
        # These are fake handles; destructor must not pass them to CloseHandle.
        managed.closed = True
