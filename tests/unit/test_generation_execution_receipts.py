"""Corrections wait for execution exit proof independently of reply acceptance."""
import asyncio
from types import SimpleNamespace

import psutil
import pytest

from backend.adapters.dsh import DshRuntimeAdapter, ResearchStopped, RuntimeBlocked, kill_tree
from backend.graphs.engine import ResearchEngine
from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor
from backend.runtime.generation_budget import GenerationBudget
from backend.runtime.recovery import execution_exit_proven, recover_infrastructure_tasks, retryable_failure
from backend.runtime.watchdog import WatchdogService
from packages.contracts.research import Willingness, digest


@pytest.fixture
def scene(tmp_path):
    store = Store(tmp_path)
    project = store.start_project(store.create_project({
        "topic": "Execution receipt fixture", "client_request_id": "receipts", "mode": "demo",
        "team": {"n": 1, "m1": 1, "m2": 1, "boss_members": 1, "k": 1},
    })["id"])
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(project["id"], project["deadline_at"])
    yield SimpleNamespace(store=store, project=project, rid=project["id"], control=control)
    control.close()


def budget_for(data):
    return GenerationBudget(data.store, data.rid, {"kind": "discussion_generation", "key": "one-logical-turn"})


@pytest.mark.parametrize("missing_at", ["root", "enumeration"])
def test_missing_process_before_tree_observation_cannot_confirm_cleanup(monkeypatch, missing_at):
    def vanished(*args, **kwargs):
        raise psutil.NoSuchProcess(12345)
    monkeypatch.setattr(psutil, "Process", vanished if missing_at == "root" else
        lambda pid: SimpleNamespace(pid=pid, children=vanished))
    receipt = {}
    assert kill_tree(12345, receipt=receipt) is False
    assert receipt["process_tree_complete"] is False


@pytest.mark.parametrize("execution_status", ["registered", "running", "unknown"])
def test_rejected_reply_cannot_authorize_new_generation_while_execution_is_unresolved(scene, execution_status):
    budget = budget_for(scene)
    attempt, _ = budget.acquire("member", "task", "turn")
    budget.record(attempt, "rejected", output={"invalid": True}, error="Invalid response")
    scene.store.put("execution", attempt["execution_key"], {"status": execution_status}, scene.rid)
    for turn in ("turn", "turn:repair-2"):
        with pytest.raises(RuntimeBlocked) as blocked:
            budget.acquire("member", "task", turn)
        assert blocked.value.code == "execution_unknown"
    saved = scene.store.get("generation_budget", budget.key)["attempts"]
    assert len(saved) == 1 and saved[0]["status"] == "rejected"
    scene.store.patch("execution", attempt["execution_key"], status="interrupted", exit_confirmed=True)
    replacement, cached = budget.acquire("member", "task", "turn")
    assert replacement["number"] == 2 and cached is None


def test_legacy_earlier_unknown_execution_is_checked_before_another_replacement(scene):
    budget = budget_for(scene)
    first, _ = budget.acquire("member", "task", "turn")
    budget.record(first, "rejected", error="First reply rejected")
    second, _ = budget.acquire("member", "task", "turn")
    budget.record(second, "rejected", error="Second reply rejected")
    scene.store.put("execution", first["execution_key"], {"status": "unknown"}, scene.rid)
    with pytest.raises(RuntimeBlocked) as blocked:
        budget.acquire("member", "task", "turn")
    assert blocked.value.code == "execution_unknown"
    assert len(scene.store.get("generation_budget", budget.key)["attempts"]) == 2


@pytest.mark.parametrize("observation, expected", [
    ("alive", False), ("inaccessible", False), ("missing_identity", False),
    ("gone", True), ("reused_pid", True),
])
def test_generation_admission_uses_actual_process_identity(scene, monkeypatch, observation, expected):
    budget = budget_for(scene)
    attempt, _ = budget.acquire("member", "task", "turn")
    budget.record(attempt, "rejected", error="Incomplete provider response")
    identity = {"pid": 12345, "pid_create_time": None if observation == "missing_identity" else 100}
    scene.store.put("execution", attempt["execution_key"], {"status": "unknown", **identity,
        "process_tree": [identity], "process_tree_complete": True}, scene.rid)
    def observe(pid):
        if observation == "inaccessible":
            raise psutil.AccessDenied(pid)
        if observation == "gone":
            raise psutil.NoSuchProcess(pid)
        return SimpleNamespace(create_time=lambda: 101 if observation == "reused_pid" else 100)
    monkeypatch.setattr(psutil, "Process", observe)
    if expected:
        assert budget.acquire("member", "task", "turn")[0]["number"] == 2
    else:
        with pytest.raises(RuntimeBlocked) as blocked:
            budget.acquire("member", "task", "turn")
        assert blocked.value.code == "execution_unknown"
        assert len(scene.store.get("generation_budget", budget.key)["attempts"]) == 1


@pytest.mark.parametrize("proof", [None, "exit_confirmed", "exit_reconciled"])
def test_missing_tree_receipt_waits_for_explicit_exit_proof(scene, monkeypatch, proof):
    budget = budget_for(scene)
    attempt, _ = budget.acquire("member", "task", "turn")
    budget.record(attempt, "rejected", error="Unconfirmed tree cleanup")
    scene.store.put("execution", attempt["execution_key"], {"status": "unknown", "pid": 12345,
        **({proof: True} if proof else {})}, scene.rid)
    def gone(pid):
        raise psutil.NoSuchProcess(pid)
    monkeypatch.setattr(psutil, "Process", gone)
    if proof:
        assert budget.acquire("member", "task", "turn")[0]["number"] == 2
    else:
        with pytest.raises(RuntimeBlocked) as blocked:
            budget.acquire("member", "task", "turn")
        assert blocked.value.code == "execution_unknown"


def test_accepted_reply_still_replays_without_reissuing_a_generation(scene):
    budget = budget_for(scene)
    attempt, _ = budget.acquire("member", "task", "turn")
    result = {"score": 0.75}
    budget.record(attempt, "accepted", output=result)
    replay, cached = budget.acquire("member", "task", "turn")
    assert replay["attempt_id"] == attempt["attempt_id"] and cached == result
    assert len(scene.store.get("generation_budget", budget.key)["attempts"]) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("concurrent_state", ["completed", "replacement"])
async def test_late_cancellation_preserves_completion_and_newer_execution_receipts(scene, monkeypatch, concurrent_state):
    adapter = DshRuntimeAdapter(scene.store, scene.control, None)
    key, execution_id = "logical-execution", "attempt-1"
    scene.control.register_process(scene.rid, execution_id)
    scene.store.put("execution", key, {"status": "running", "execution_attempt_id": execution_id,
        "agent_id": "member", "task_id": "task", "pid": 12345}, scene.rid)
    adapter.processes[execution_id] = SimpleNamespace(pid=12345, returncode=None)
    output = {"score": 0.75}
    def finish_during_cleanup(pid, *, receipt=None):
        if concurrent_state == "completed":
            scene.store.complete_model_turn(key, output, payload={})
        else:
            scene.store.put("execution", key, {"status": "running",
                "execution_attempt_id": "attempt-2", "agent_id": "member", "task_id": "task"}, scene.rid)
        return False
    monkeypatch.setattr("backend.adapters.dsh.kill_tree", finish_during_cleanup)
    result = await adapter.cancel_execution(execution_id)
    current = scene.store.get("execution", key)
    if concurrent_state == "completed":
        assert result["exit_confirmed"]
        assert current["status"] == "completed" and current["output"] == output
    else:
        assert not result["exit_confirmed"]
        assert current["status"] == "running" and current["execution_attempt_id"] == "attempt-2"
    adapter.processes.clear()


@pytest.mark.asyncio
@pytest.mark.parametrize("error_code", ["invalid_output", "model_response_invalid", "model_output_truncated"])
async def test_engine_correction_cannot_bypass_a_rejected_unknown_execution(scene, error_code):
    engine = ResearchEngine(scene.store, scene.control, None, None, None)
    calls = []
    async def boundary(**arguments):
        calls.append(arguments["turn_id"])
        if len(calls) == 1:
            key = digest([scene.rid, arguments["agent_id"], arguments["task_id"], arguments["turn_id"]])
            scene.store.put("execution", key, {"status": "unknown",
                "execution_attempt_id": "unconfirmed-owner"}, scene.rid)
            raise RuntimeBlocked(error_code, "Local response failure fixture")
        pytest.fail("The first SDK execution has no exit proof")
    engine.demo.execute_turn = boundary
    try:
        with pytest.raises(RuntimeBlocked) as blocked:
            await engine.turn(scene.rid, "line-01/exploration/member-01", "exploration", "task", "score",
                Willingness, {"generation_budget": {"kind": "discussion_generation", "key": "same-call"}},
                publish_message=False)
        assert blocked.value.code == "execution_unknown"
        assert calls == ["score:generation-1"]
        assert scene.store.list("execution", scene.rid)[0]["status"] == "unknown"
        assert len(scene.store.list("generation_budget", scene.rid)[0]["attempts"]) == 1
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_unknown_execution_recovers_automatically_only_after_control_exit_receipt(scene):
    task = scene.store.add_task(scene.rid, "paper", {}, "receipt-task")
    failure = retryable_failure(RuntimeBlocked("execution_unknown", "Pending exit proof"))
    assert failure[0] == "execution_unknown"
    scene.store.finish_task(task["id"], error={"code": failure[0], "message": failure[1]})
    scene.store.update_project(scene.rid, status="blocked", error={"code": "execution_unknown"})
    scene.store.put("execution", "owner", {"status": "unknown", "task_id": task["id"],
        "execution_attempt_id": "owner-attempt"}, scene.rid)
    scene.control.register_process(scene.rid, "owner-attempt")
    assert not await execution_exit_proven(scene.store, scene.control, task)
    assert not await recover_infrastructure_tasks(scene.store, scene.control)
    assert scene.store.get("task", task["id"])["status"] == "failed"
    scene.control.complete_execution("owner-attempt")
    assert await recover_infrastructure_tasks(scene.store, scene.control) == [task["id"]]
    assert scene.store.get("execution", "owner")["exit_confirmed"]
    assert scene.store.get("task", task["id"])["status"] in {"pending", "retry_wait"}
    assert scene.store.project(scene.rid)["status"] == "running"


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["stop", "deadline"])
async def test_exit_receipt_never_reopens_stopped_work(scene, boundary):
    task = scene.store.add_task(scene.rid, "paper", {}, "stopped-task")
    scene.store.finish_task(task["id"], error={"code": "execution_unknown"})
    scene.store.put("execution", "owner", {"status": "unknown", "task_id": task["id"],
        "execution_attempt_id": "owner-attempt"}, scene.rid)
    scene.control.register_process(scene.rid, "owner-attempt")
    scene.control.complete_execution("owner-attempt")
    if boundary == "stop":
        scene.control.terminate(scene.rid)
    else:
        with scene.control._lock:
            scene.control.projects[scene.rid]["deadline_at"] = 1
    assert not await execution_exit_proven(scene.store, scene.control, task)
    assert not await recover_infrastructure_tasks(scene.store, scene.control)
    assert scene.store.get("task", task["id"])["status"] == "failed"


@pytest.mark.asyncio
@pytest.mark.parametrize("stop_while_waiting", [False, True])
async def test_watchdog_waits_in_same_scope_then_continues_after_exit(scene, stop_while_waiting):
    first_call, calls = asyncio.Event(), []
    async def execute_turn(**arguments):
        calls.append(arguments["turn_id"])
        if len(calls) == 1:
            key = digest([scene.rid, "watchdog", arguments["task_id"], arguments["turn_id"]])
            scene.store.put("execution", key, {"status": "unknown",
                "task_id": arguments["task_id"], "execution_attempt_id": key}, scene.rid)
            scene.control.register_process(scene.rid, key)
            first_call.set()
            raise RuntimeBlocked("model_response_invalid", "Incomplete diagnostic reply")
        return {"content_markdown": "Recovered diagnostic", "actions": []}
    runner = SimpleNamespace(status=lambda: {"runs": []}, resources=lambda: {"allocations": []})
    service = WatchdogService(SimpleNamespace(store=scene.store, control=scene.control, runner=runner,
        demo=SimpleNamespace(execute_turn=execute_turn)))
    event = scene.store.put("watchdog_event", "diagnosis", {"line_id": "line-01", "direction_epoch": 1,
        "cycle_id": None, "status": "diagnosing", "lease_token": "original",
        "diagnosis_round": 0, "action_ids": []}, scene.rid)
    pending = asyncio.create_task(service.diagnose(event))
    try:
        await asyncio.wait_for(first_call.wait(), 2)
        await asyncio.sleep(0.3)
        assert not pending.done() and len(calls) == 1
        assert scene.store.get("watchdog_event", event["id"])["lease_token"] == "original"
        execution = scene.store.list("execution", scene.rid)[0]
        if stop_while_waiting:
            scene.control.terminate(scene.rid)
        scene.control.complete_execution(execution["execution_attempt_id"])
        if stop_while_waiting:
            with pytest.raises(ResearchStopped):
                await asyncio.wait_for(pending, 3)
            assert len(calls) == 1
        else:
            await asyncio.wait_for(pending, 3)
            assert calls == ["diagnose-1:generation-1", "diagnose-1:generation-2"]
            assert scene.store.get("watchdog_event", event["id"])["status"] == "awaiting_progress"
    finally:
        if not pending.done():
            pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
