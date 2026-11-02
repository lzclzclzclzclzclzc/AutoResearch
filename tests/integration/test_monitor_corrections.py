import asyncio
from types import SimpleNamespace

import pytest

from backend.graphs.engine import ResearchEngine
from backend.adapters.dsh import RuntimeBlocked
from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor
from backend.runtime.generation_budget import GenerationBudget
from backend.runtime.worker import sync_runner_states
from backend.services.credentials import CredentialService
from packages.contracts.research import ExperimentSpec


@pytest.fixture
def monitor_runtime(tmp_path):
    store = Store(tmp_path)
    project = store.start_project(store.create_project({"client_request_id": "monitor-corrections", "topic": "monitor",
        "mode": "demo", "team": {"n": 1, "m1": 1, "m2": 1, "boss_members": 1, "k": 1}})["id"])
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(project["id"], project["deadline_at"])
    plan = ExperimentSpec(spec_id="spec", version=1, question="monitor", hypothesis="fixture", fixed={},
        baseline="none", data={}, metrics=["exit"], design={}, monitoring={}, completion_criteria="exit",
        early_stop_rules=[], deadline_at=project["deadline_at"])
    store.put("spec", "spec", plan.model_dump(), project["id"])
    run = {"experiment_run_id": "run", "state": "running", "research_id": project["id"],
        "allocation_id": "a", "start_operation_id": "start", "spec_id": "spec", "code_version": "code"}
    runner = SimpleNamespace(status=lambda *_: dict(run), stop_receipt=lambda *_: {"state": "unknown"})
    stops = []
    runner.request_stop = lambda *args: stops.append(args)
    engine = ResearchEngine(store, control, runner, CredentialService(tmp_path, store), None)
    async def logs(*args):
        return {"run": dict(run), "text": "unusable runtime", "offset": 0, "next_offset": 16}
    engine.collect_run_logs = logs
    state = {"research_id": project["id"], "line_id": "line-01", "spec_id": "spec", "direction_epoch": 1}
    yield engine, state, run, stops
    control.close()


def answer(rule_id="execution_unusable", **fields):
    return {"content_markdown": "停止依据见当前日志", "request_early_stop": True, "rule_id": rule_id,
        "rationale": "当前窗口不可继续", **fields}


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["missing", None, "", "   ", "imaginary-rule"])
async def test_invalid_stop_returns_same_agent_and_fixed_window_before_acceptance(monitor_runtime, monkeypatch, invalid):
    engine, state, run, stops = monitor_runtime
    calls = []
    async def model(**args):
        calls.append(args)
        if len(calls) == 1:
            value = answer(invalid)
            if invalid == "missing":
                value.pop("rule_id")
            return value
        assert "rule_id=" in args["prompt"] and "execution_unusable" in args["prompt"]
        assert not engine.store.get("run_monitor_cursor", "run") and not stops
        return answer()
    monkeypatch.setattr(engine.demo, "_produce_turn", model)
    result = await engine.monitor_run(state, run)
    assert result["rule_id"] == "execution_unusable" and len(calls) == 2
    assert calls[0]["agent_id"] == calls[1]["agent_id"] == "line-01/implementation"
    assert calls[0]["context"]["logs"] == calls[1]["context"]["logs"]
    assert stops == [("run", "early_stopped", "run:monitor-stop:1")]
    assert len(engine.store.list("run_monitor", state["research_id"])) == 1
    assert not engine.store.list("arbitration_request")


@pytest.mark.asyncio
async def test_corrections_keep_fixed_window_past_three_without_advancing_until_accepted(monitor_runtime, monkeypatch):
    engine, state, run, stops = monitor_runtime
    calls = []
    async def model(**args):
        calls.append(args)
        assert not stops and not engine.store.get("run_monitor_cursor", "run")
        return answer(None) if len(calls) <= 5 else answer()
    monkeypatch.setattr(engine.demo, "_produce_turn", model)
    result = await engine.monitor_run(state, run)
    assert result["rule_id"] == "execution_unusable"
    assert len(calls) == 6
    assert all(call["context"]["logs"] == calls[0]["context"]["logs"] for call in calls)
    assert engine.store.get("run_monitor_window", "run:monitor-window:1")["status"] == "accepted"
    assert len(engine.store.list("run_monitor")) == 1 and len(stops) == 1


@pytest.mark.asyncio
async def test_running_attempt_still_waits_for_receipt_after_restart(monitor_runtime, monkeypatch):
    engine, state, run, stops = monitor_runtime
    binding = {"kind": "monitor_generation", "key": "run:monitor-window:1", "limit": 3}
    budget = GenerationBudget(engine.store, state["research_id"], binding)
    for number in range(3):
        attempt, _ = budget.acquire("line-01/implementation", "run", "monitor-1")
        if number < 2:
            budget.record(attempt, "rejected", output=answer(None), error="invalid rule_id")
    async def forbidden(**args):
        pytest.fail("an unknown running execution must not be duplicated")
    engine.store.put("execution", attempt["execution_key"], {"status": "unknown"}, state["research_id"])
    monkeypatch.setattr(engine.demo, "_produce_turn", forbidden)
    with pytest.raises(RuntimeBlocked) as error:
        await engine.monitor_run(state, run)
    assert error.value.code == "execution_unknown"
    assert len(engine.store.get("generation_budget", budget.key)["attempts"]) == 3 and not stops


@pytest.mark.asyncio
async def test_legacy_exhausted_window_can_continue_without_waiting_for_new_logs(monitor_runtime, monkeypatch):
    engine, state, run, stops = monitor_runtime
    window_id = "run:monitor-window:1"
    logs = await engine.collect_run_logs("run", 0)
    engine.store.put("run_monitor_window", window_id, {"logs": logs, "log_hash": "saved-log-hash",
        "round_no": 1, "status": "exhausted", "from_offset": 0, "next_offset": 16}, state["research_id"])
    budget = GenerationBudget(engine.store, state["research_id"],
        {"kind": "monitor_generation", "key": window_id, "limit": 3})
    for _ in range(3):
        attempt, _ = budget.acquire("line-01/implementation", "run", "monitor-1")
        budget.record(attempt, "rejected", output=answer(None), error="invalid rule_id")
    async def corrected(**args):
        assert args["context"]["logs"] == logs
        return answer()
    monkeypatch.setattr(engine.demo, "_produce_turn", corrected)
    assert (await engine.monitor_run(state, run))["rule_id"] == "execution_unusable"
    assert len(engine.store.get("generation_budget", budget.key)["attempts"]) == 4
    assert engine.store.get("run_monitor_window", window_id)["status"] == "accepted" and len(stops) == 1


@pytest.mark.asyncio
async def test_invalid_corrections_end_when_experiment_exits(monitor_runtime, monkeypatch):
    engine, state, run, stops = monitor_runtime
    calls = []
    async def invalid_until_exit(**args):
        calls.append(args)
        if len(calls) == 5:
            run.update(state="completed", exit_code=0, terminal_proof={"process_tree_exited": True})
        return answer(None)
    monkeypatch.setattr(engine.demo, "_produce_turn", invalid_until_exit)
    assert await engine.monitor_run(state, run) is None
    assert len(calls) == 5 and not stops and not engine.store.get("run_monitor_cursor", "run")
    assert engine.store.get("run_monitor_window", "run:monitor-window:1")["status"] == "late"


@pytest.mark.asyncio
async def test_unknown_run_retains_monitor_window_for_reconciliation(monitor_runtime, monkeypatch):
    engine, state, run, stops = monitor_runtime
    async def forbidden(**args):
        pytest.fail("unknown run must be reconciled before generating another monitor response")
    monkeypatch.setattr(engine.demo, "_produce_turn", forbidden)
    run["state"] = "unknown"
    with pytest.raises(RuntimeBlocked) as error:
        await engine.monitor_run(state, run)
    assert error.value.code == "runner_unknown"
    assert engine.store.get("run_monitor_window", "run:monitor-window:1")["status"] == "pending"
    assert not stops and not engine.store.get("run_monitor_cursor", "run")


@pytest.mark.asyncio
async def test_late_valid_stop_archived_and_terminal_synced_while_model_waits(monitor_runtime, monkeypatch):
    engine, state, run, stops = monitor_runtime
    started, release = asyncio.Event(), asyncio.Event()
    async def model(**args):
        started.set()
        await release.wait()
        return answer()
    monkeypatch.setattr(engine.demo, "_produce_turn", model)
    engine.store.put("run", "run", dict(run), state["research_id"])
    task = asyncio.create_task(engine.monitor_run(state, run))
    await started.wait()
    run.update(state="completed", exit_code=0, terminal_proof={"process_tree_exited": True})
    await sync_runner_states(engine.store, engine.runner)
    assert engine.store.get("run", "run")["state"] == "completed"
    release.set()
    assert await task is None
    assert not stops and not engine.store.get("run_monitor_cursor", "run")
    assert engine.store.get("run_monitor_window", "run:monitor-window:1")["status"] == "late"


@pytest.mark.asyncio
async def test_lost_stop_response_recovers_receipt_without_second_effect(monitor_runtime, monkeypatch):
    engine, state, run, stops = monitor_runtime
    async def model(**args):
        return answer()
    monkeypatch.setattr(engine.demo, "_produce_turn", model)
    def lose_response(*args):
        stops.append(args)
        raise ConnectionError("stop acknowledgement lost")
    engine.runner.request_stop = lose_response
    with pytest.raises(ConnectionError):
        await engine.monitor_run(state, run)
    intent = engine.store.get("run_stop_intent", "run:monitor-stop:1")
    assert intent["status"] == "pending"
    engine.runner.stop_receipt = lambda *_: {"state": "accepted"}
    await engine.apply_monitor_stop(state, "run")
    assert len(stops) == 1
    assert engine.store.get("run_stop_intent", intent["id"])["status"] == "applied"
