import asyncio
from types import SimpleNamespace

import pytest

from backend.graphs.engine import ResearchEngine
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
async def test_exhausted_window_does_not_restart_or_advance_and_new_log_has_new_budget(monitor_runtime, monkeypatch):
    engine, state, run, stops = monitor_runtime
    calls = []
    async def model(**args):
        calls.append(args)
        return answer(None)
    monkeypatch.setattr(engine.demo, "_produce_turn", model)
    assert await engine.monitor_run(state, run) is None
    assert len(calls) == 3 and not stops
    assert not engine.store.get("run_monitor_cursor", "run")
    assert not engine.store.list("run_monitor")
    assert engine.store.get("run_monitor_window", "run:monitor-window:1")["status"] == "exhausted"
    assert await engine.monitor_run(state, run) is None
    assert len(calls) == 3
    async def growing_logs(*args):
        assert args[1] == 0  # only an accepted observation can move the cursor
        return {"run": run, "text": "additional logs", "next_offset": 32}
    engine.collect_run_logs = growing_logs
    assert await engine.monitor_run(state, run) is None
    assert len(calls) == 6
    assert engine.store.get("run_monitor_window", "run:monitor-window:2")["status"] == "exhausted"


@pytest.mark.asyncio
async def test_claimed_third_attempt_without_reply_remains_consumed_after_restart(monitor_runtime, monkeypatch):
    engine, state, run, stops = monitor_runtime
    binding = {"kind": "monitor_generation", "key": "run:monitor-window:1", "limit": 3}
    budget = GenerationBudget(engine.store, state["research_id"], binding)
    for number in range(3):
        attempt, _ = budget.acquire("line-01/implementation", "run", "monitor-1")
        if number < 2:
            budget.record(attempt, "rejected", output=answer(None), error="invalid rule_id")
    async def forbidden(**args):
        pytest.fail("a fourth generation must never be sent")
    monkeypatch.setattr(engine.demo, "_produce_turn", forbidden)
    assert await engine.monitor_run(state, run) is None
    assert len(engine.store.get("generation_budget", budget.key)["attempts"]) == 3 and not stops


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
