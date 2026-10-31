"""Regressions for protocol failures, critic repair routing and configuration echoes."""
import copy

import pytest
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from backend.adapters.dsh import ResearchStopped, RuntimeBlocked
from backend.graphs.engine import ResearchEngine
from backend.graphs.tasks import current_task
from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor
from backend.runtime.recovery import legacy_failure, recover_infrastructure_tasks
from packages.contracts.research import ExperimentSpec, MessageReply


@pytest.fixture
async def scene(tmp_path):
    store = Store(tmp_path)
    project = store.start_project(store.create_project({"client_request_id": "tweets6", "topic": "recovery",
        "mode": "demo", "team": {"n": 1, "m1": 1, "m2": 1, "boss_members": 1},
        "max_runtime_seconds": 120})["id"])
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(project["id"], project["deadline_at"])
    try:
        async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "cp.sqlite")) as saver:
            engine = ResearchEngine(store, control, None, None, saver)
            try:
                yield store, project, engine
            finally:
                await engine.close()
    finally:
        control.close()


def response_error():
    return RuntimeBlocked("model_response_invalid", "stream finish was malformed", provider_error_code="MALFORMED_RESPONSE")


async def test_protocol_failure_retries_same_turn_then_accepts_complete_output(scene):
    store, p, engine = scene
    calls = []
    async def produce(**args):
        calls.append(args["turn_id"])
        if len(calls) < 3:
            raise response_error()
        return {"content_markdown": "complete"}
    engine.demo._produce_turn = produce
    result = await engine.turn(p["id"], "paper", "paper", "task", "reply", MessageReply, {})
    assert result == {"content_markdown": "complete"}
    assert calls == ["reply"] * 3
    retries = [e for e in store.events(p["id"]) if e["type"] == "agent.retry_wait"]
    assert [e["payload"]["attempt"] for e in retries] == [1, 2]
    assert not store.list("arbitration_request", p["id"])


async def test_response_retry_budget_survives_replay_and_explicit_retry_gets_new_generation(scene):
    store, p, engine = scene
    calls = []
    async def produce(**args):
        calls.append(args["turn_id"])
        if len(calls) <= 3:
            raise response_error()
        return {"content_markdown": "recovered"}
    engine.demo._produce_turn = produce
    for _ in range(2):
        with pytest.raises(RuntimeBlocked) as error:
            await engine.turn(p["id"], "paper", "paper", "task", "reply", MessageReply, {})
        assert error.value.code == "model_response_invalid"
    assert len(calls) == 3
    task = store.add_task(p["id"], "paper", {}, "root")
    store.finish_task(task["id"], error={"code": "model_response_invalid"})
    store.retry_failed_tasks(p["id"])
    token = current_task.set(store.get("task", task["id"]))
    try:
        assert (await engine.turn(p["id"], "paper", "paper", "task", "reply", MessageReply, {}))["content_markdown"] == "recovered"
    finally:
        current_task.reset(token)
    assert calls[-1] == "reply:recovery-1"


async def test_stop_after_malformed_response_prevents_replacement(scene):
    store, p, engine = scene
    calls = []
    async def produce(**args):
        calls.append(args["turn_id"])
        engine.control.terminate(p["id"])
        raise response_error()
    engine.demo._produce_turn = produce
    with pytest.raises(ResearchStopped):
        await engine.turn(p["id"], "paper", "paper", "task", "reply", MessageReply, {})
    assert len(calls) == 1 and not store.list("model_response_retry", p["id"])


@pytest.mark.parametrize("always_fail", [False, True])
async def test_god_response_protocol_errors_use_existing_durable_three_attempt_budget(scene, always_fail):
    store, p, engine = scene
    calls = []
    async def judge(request, context, attempt, **kwargs):
        calls.append(attempt)
        if always_fail or attempt == 1:
            raise response_error()
        return {"arbitration_id": request["id"], "snapshot_hash": request["snapshot_hash"],
            "action": "select_candidate", "selected_candidate_id": "member", "content_markdown": "choose", "rationale": "valid"}
    engine.execute_god_turn = judge
    arguments = dict(research_id=p["id"], source_task_id="root", scope_kind="discussion", scope_id="discussion",
        reason_code="decision_blocked", origin_node="discussion.collect", allowed_actions=["select_candidate"],
        context={"role": "boss", "candidates": {"member": {}}, "blockers": []})
    if always_fail:
        with pytest.raises(RuntimeBlocked) as error:
            await engine.arbitration.resolve(**arguments)
        assert error.value.code == "arbitration_exhausted" and calls == [1, 2, 3]
        assert store.list("arbitration_request", p["id"])[0]["status"] == "exhausted"
    else:
        assert (await engine.arbitration.resolve(**arguments))["action"] == "select_candidate"
        assert calls == [1, 2]


async def test_legacy_protocol_error_is_recovered_only_from_latest_boundary_and_never_after_stop(scene):
    store, p, engine = scene
    task = store.add_task(p["id"], "paper", {}, "legacy")
    store.finish_task(task["id"], error={"code": "model_execution_failed"})
    def boundary(code):
        store.emit(p["id"], "agent.runtime_event", {"notification": {"payload": {"event": {
            "type": "turn/end", "data": {"reason": {"kind": "error", "error": {"code": code}}}}}}}, task_id=task["id"])
    boundary("MALFORMED_RESPONSE")
    assert legacy_failure(store, store.get("task", task["id"]))[0] == "model_response_invalid"
    assert await recover_infrastructure_tasks(store, engine.control) == [task["id"]]
    store.finish_task(task["id"], error={"code": "model_response_invalid"})
    assert not await recover_infrastructure_tasks(store, engine.control)
    store.finish_task(task["id"], error={"code": "model_execution_failed"})
    boundary("AUTH")
    assert legacy_failure(store, store.get("task", task["id"])) is None
    boundary("MALFORMED_RESPONSE")
    engine.control.terminate(p["id"])
    assert not await recover_infrastructure_tasks(store, engine.control)

def line_state(store, p):
    store.put("cycle", "cycle", {"objective": "frozen baseline", "line_id": "line-01"}, p["id"])
    store.update_line(p["id"], "line-01", cycle_id="cycle")
    return dict(research_id=p["id"], line_id="line-01", cycle_id="cycle", task_id="root", direction_epoch=1, revision=1)

@pytest.mark.parametrize("corrected", [True, False])
async def test_implementation_gets_one_configuration_correction_before_design_revision(scene, corrected):
    store, p, engine = scene
    state = line_state(store, p)
    spec = ExperimentSpec(spec_id="spec", version=1, question="q", hypothesis="h", fixed={"seed": 42, "methods": "word"},
        baseline="b", data={}, metrics=["f1"], design={}, monitoring={}, completion_criteria="done", deadline_at=p["deadline_at"])
    store.put("spec", "spec", spec.model_dump(), p["id"])
    state["spec_id"] = "spec"
    original, calls = engine.demo._produce_turn, []
    async def produce(**args):
        calls.append(args["turn_id"])
        assert args["context"]["spec"]["fixed"] == spec.fixed
        output = await original(**args)
        if len(calls) == 1 or not corrected:
            output["actual_config"] = {**copy.deepcopy(spec.fixed), "methods": "char", "data": {}}
        return output
    engine.demo._produce_turn = produce
    result = await engine.implement(state)
    assert calls == ["code", "code:config-repair"]
    repair = store.list("implementation_config_repair", p["id"])[0]
    assert repair["original_output"]["actual_config"]["methods"] == "char"
    assert "unexpected keys=['data']" in repair["reason"] and "changed keys=['methods']" in repair["reason"]
    if corrected:
        assert result["implementation_route"] == "organize" and repair["status"] == "corrected"
        assert store.list("implementation", p["id"])[0]["actual_config"] == spec.fixed
        assert not store.list("spec_revision_request", p["id"])
    else:
        assert result["implementation_route"] == "design" and repair["status"] == "requires_design_revision"
        assert len(store.list("spec_revision_request", p["id"])) == 1
        assert not store.list("implementation", p["id"])
