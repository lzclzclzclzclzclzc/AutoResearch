import asyncio
import time
from types import SimpleNamespace

import pytest

from backend.adapters.dsh import ResearchStopped, RuntimeBlocked
from backend.graphs.engine import ResearchEngine
from backend.graphs.tasks import current_task
from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor
from packages.contracts.research import ExperimentSpec, Willingness


@pytest.fixture
def runtime(tmp_path):
    store = Store(tmp_path)
    project = store.start_project(store.create_project({
        "client_request_id": "turn-scope", "topic": "scope", "mode": "demo",
        "team": {"n": 1, "m1": 1, "m2": 1, "boss_members": 1, "k": 1},
    })["id"])
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(project["id"], project["deadline_at"])
    engine = ResearchEngine(store, control, None, None, None)
    try:
        yield engine, project
    finally:
        control.close()


def change_scope(engine, rid, change):
    fields = {
        "direction": {"direction_epoch": 2},
        "cycle": {"cycle_id": "next-cycle"},
        "direction_stop": {"direction_stop_epoch": 1},
        "finish": {"finish_requested": True},
    }[change]
    engine.store.update_line(rid, "line-01", **fields)


def call_arguments(project, durable=False, publish=False):
    return dict(research_id=project["id"], agent_id="line-01/exploration/member-01",
        role="exploration", task_id="old-task", turn_id="score", schema=Willingness,
        context={"no_arbitration": True, **({"generation_budget": {
            "kind": "discussion_generation", "key": "same-call"}} if durable else {})},
        line_id="line-01", publish_message=publish)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["direction", "cycle", "direction_stop", "finish"])
@pytest.mark.parametrize("durable", [False, True])
@pytest.mark.parametrize("failure", ["schema", "invalid_output"])
async def test_obsolete_scope_stops_before_a_replacement_generation(runtime, monkeypatch, change, durable, failure):
    engine, project = runtime
    calls = []

    async def invalid(**args):
        calls.append(args["turn_id"])
        assert len(calls) == 1, "obsolete work must not start another generation"
        change_scope(engine, project["id"], change)
        if failure == "invalid_output":
            raise RuntimeBlocked("invalid_output", "malformed model JSON")
        return {"score": -1}

    monkeypatch.setattr(engine.demo, "_produce_turn", invalid)
    with pytest.raises(RuntimeBlocked) as error:
        await engine.turn(**call_arguments(project, durable))
    assert error.value.code == ("line_finished" if change == "finish" else "task_superseded")
    assert len(calls) == 1
    assert not engine.store.list("arbitration_request")


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["direction", "cycle", "direction_stop", "finish"])
@pytest.mark.parametrize("durable", [False, True])
async def test_late_valid_reply_is_not_accepted_or_published(runtime, monkeypatch, change, durable):
    engine, project = runtime

    async def late(**_):
        change_scope(engine, project["id"], change)
        return {"score": 50}

    monkeypatch.setattr(engine.demo, "_produce_turn", late)
    with pytest.raises(RuntimeBlocked) as error:
        await engine.turn(**call_arguments(project, durable, publish=True))
    assert error.value.code == ("line_finished" if change == "finish" else "task_superseded")
    assert engine.store.messages(project["id"] + ":old-task")["items"] == []
    # The raw execution receipt remains available, but it is not an accepted reply.
    assert engine.store.list("execution")[0]["output"] == {"score": 50}
    assert all(attempt["status"] != "accepted"
        for budget in engine.store.list("generation_budget") for attempt in budget["attempts"])


@pytest.mark.asyncio
async def test_waiting_member_keeps_its_original_cycle(runtime, monkeypatch):
    engine, project = runtime
    args = call_arguments(project)
    lock = engine.agent_turn_locks.setdefault((project["id"], args["agent_id"]), asyncio.Lock())
    await lock.acquire()

    async def forbidden(**_):
        pytest.fail("a queued old-cycle call must not bind itself to the new cycle")

    monkeypatch.setattr(engine.demo, "_produce_turn", forbidden)
    task = asyncio.create_task(engine.turn(**args))
    try:
        await asyncio.sleep(0)
        change_scope(engine, project["id"], "cycle")
    finally:
        lock.release()
    with pytest.raises(RuntimeBlocked) as error:
        await task
    assert error.value.code == "task_superseded"


@pytest.mark.asyncio
@pytest.mark.parametrize("source", ["context", "task"])
async def test_existing_original_scope_is_not_replaced_by_the_live_cycle(runtime, monkeypatch, source):
    engine, project = runtime
    args = call_arguments(project)
    original = {"line_id": "line-01", "direction_epoch": 1, "cycle_id": "old-cycle"}
    change_scope(engine, project["id"], "cycle")
    if source == "context":
        args["context"].update(original)
    token = current_task.set({"payload": original} if source == "task" else None)

    async def forbidden(**_):
        pytest.fail("the persisted task scope must remain authoritative")

    monkeypatch.setattr(engine.demo, "_produce_turn", forbidden)
    try:
        with pytest.raises(RuntimeBlocked) as error:
            await engine.turn(**args)
        assert error.value.code == "task_superseded"
    finally:
        current_task.reset(token)


@pytest.mark.asyncio
@pytest.mark.parametrize("durable", [False, True])
async def test_current_scope_keeps_unlimited_corrections_and_reuses_completed_receipt(runtime, monkeypatch, durable):
    engine, project = runtime
    calls = []

    async def corrected(**args):
        calls.append(args["turn_id"])
        return {"score": -1 if len(calls) <= 5 else 50}

    monkeypatch.setattr(engine.demo, "_produce_turn", corrected)
    args = call_arguments(project, durable)
    original_context = dict(args["context"])
    assert await engine.turn(**args) == {"score": 50}
    assert await engine.turn(**args) == {"score": 50}
    assert len(calls) == len(set(calls)) == 6
    assert args["context"] == original_context


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["pause", "stop", "deadline"])
@pytest.mark.parametrize("durable", [False, True])
async def test_project_control_still_ends_corrections(runtime, monkeypatch, boundary, durable):
    engine, project = runtime
    calls = []

    async def invalid(**args):
        calls.append(args["turn_id"])
        assert len(calls) == 1
        if boundary == "pause":
            engine.store.update_project(project["id"], status="paused")
        elif boundary == "stop":
            engine.control.terminate(project["id"])
        else:
            with engine.control._lock:
                engine.control.projects[project["id"]]["deadline_at"] = time.time() - 1
        return {"score": -1}

    monkeypatch.setattr(engine.demo, "_produce_turn", invalid)
    expected = RuntimeBlocked if boundary == "pause" else ResearchStopped
    with pytest.raises(expected) as error:
        await engine.turn(**call_arguments(project, durable))
    if boundary == "pause":
        assert error.value.code == "research_paused"
    assert len(calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["direction", "cycle", "direction_stop", "finish"])
@pytest.mark.parametrize("valid", [False, True])
@pytest.mark.parametrize("already_obsolete", [False, True])
async def test_obsolete_monitor_window_becomes_late_without_stopping_the_run(runtime, monkeypatch, change, valid, already_obsolete):
    engine, project = runtime
    plan = ExperimentSpec(spec_id="spec", version=1, question="monitor", hypothesis="fixture", fixed={},
        baseline="none", data={}, metrics=["exit"], design={}, monitoring={}, completion_criteria="exit",
        early_stop_rules=[], deadline_at=project["deadline_at"])
    engine.store.put("spec", "spec", plan.model_dump(), project["id"])
    run = {"experiment_run_id": "run", "state": "running", "research_id": project["id"], "spec_id": "spec"}
    stops, calls = [], []
    engine.runner = SimpleNamespace(status=lambda *_: dict(run), request_stop=lambda *args: stops.append(args))

    async def logs(*_):
        return {"run": dict(run), "text": "fixed log window", "offset": 0, "next_offset": 16}

    async def monitor(**args):
        calls.append(args["turn_id"])
        assert len(calls) == 1
        change_scope(engine, project["id"], change)
        return {"content_markdown": "stop proposal", "request_early_stop": True,
            "rule_id": "execution_unusable" if valid else None, "rationale": "frozen window"}

    monkeypatch.setattr(engine, "collect_run_logs", logs)
    monkeypatch.setattr(engine.demo, "_produce_turn", monitor)
    state = {"research_id": project["id"], "line_id": "line-01", "spec_id": "spec", "direction_epoch": 1, "cycle_id": None}
    if already_obsolete:
        change_scope(engine, project["id"], change)
    assert await engine.monitor_run(state, run) is None
    assert len(calls) == (0 if already_obsolete else 1) and not stops
    assert engine.store.get("run_monitor_window", "run:monitor-window:1")["status"] == "late"
    assert not engine.store.get("run_monitor_cursor", "run")
    assert not engine.store.list("run_monitor") and not engine.store.list("run_stop_intent")
