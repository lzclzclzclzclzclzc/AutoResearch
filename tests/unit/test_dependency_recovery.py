from types import SimpleNamespace

import pytest

from backend.graphs.dependencies import DependencyService
from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor


@pytest.fixture
def waiting(tmp_path):
    store = Store(tmp_path)
    p = store.start_project(store.create_project({"topic": "dependency", "mode": "demo", "client_request_id": "deps"})["id"])
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(p["id"], p["deadline_at"])
    service = DependencyService(SimpleNamespace(store=store, control=control))
    line = store.lines(p["id"])[1]
    task = store.add_task(p["id"], "line", {"line_id": line["line_id"],
        "cycle_id": line.get("cycle_id"), "direction_epoch": line["direction_epoch"]}, "dependent")
    store.patch("task", task["id"], status="waiting_recovery", resume_token="token")
    job = store.put("dependency_wait", "deps", {"status": "waiting_children", "line_id": line["line_id"],
        "source_cycle_id": line.get("cycle_id"), "direction_epoch": line["direction_epoch"], "control_generation": p["control_generation"],
        "root_task_id": task["id"], "resume_token": "token", "arbitration_id": "arb", "requirements": ["baseline"],
        "upstreams": [{"line_id": "line-01", "cycle_id": None, "direction_epoch": 1}], "coordination_ids": []}, p["id"])
    try:
        yield store, control, service, p, task, job
    finally:
        control.close()


@pytest.mark.asyncio
async def test_repeated_scans_do_not_duplicate_boss_or_accept_raw_cross_line_results(waiting, tmp_path):
    store, _, service, p, root, job = waiting
    for _ in range(4):
        await service.dispatch(p["id"])
    bosses = [t for t in store.pending_tasks() if t["payload"].get("dependency_wait_id")]
    assert len(bosses) == 1
    source = tmp_path / "baseline.json"
    source.write_text('{"f1":0.7}', encoding="utf-8")
    artifact = store.register_artifact(p["id"], source, line_id="line-01")
    store.finish_task(bosses[0]["id"], result={})
    store.put("boss_effects", bosses[0]["id"], {"status": "applied"}, p["id"])
    store.put("memory_node", "uncommitted", {"boss_decision_id": bosses[0]["id"],
        "source_ids": [artifact["id"]], "content_markdown": "not authorized"}, p["id"])
    store.put("memory_node", "missing-source", {"boss_decision_id": bosses[0]["id"], "receipt": {"persisted": True},
        "source_ids": [], "source_refs": ["missing"], "content_markdown": "not traceable"}, p["id"])
    await service.dispatch(p["id"])
    assert store.get("dependency_wait", job["id"])["status"] == "waiting_children"
    assert store.get("task", root["id"])["status"] == "waiting_recovery"
    assert not store.list("recovery_outbox", p["id"])


@pytest.mark.asyncio
@pytest.mark.parametrize("closed", ["pause", "cycle", "stop"])
async def test_dependency_wait_respects_pause_cycle_and_stop_without_creating_more_work(waiting, closed):
    store, control, service, p, root, job = waiting
    await service.dispatch(p["id"])
    boss_id = store.get("dependency_wait", job["id"])["coordination_ids"][0]
    if closed == "pause":
        store.update_project(p["id"], status="pausing")
    elif closed == "cycle":
        store.update_line(p["id"], job["line_id"], cycle_id="new-cycle")
    else:
        store.sync_stop(p["id"], control.terminate(p["id"]))
    await service.dispatch(p["id"])
    state = store.get("dependency_wait", job["id"])
    assert len(state["coordination_ids"]) == 1
    if closed == "pause":
        assert state["status"] == "waiting_children"
        assert store.get("task", root["id"])["status"] == "waiting_recovery"
    else:
        assert state["status"] == "cancelled"
        assert store.get("task", root["id"])["status"] == "cancelled"
        assert store.get("task", boss_id)["status"] == "cancelled"
        assert any(e["type"] == "dependency.cancelled" for e in store.events(p["id"]))


def test_only_registered_cross_line_dependencies_can_be_routed(waiting):
    _, _, service, p, _, _ = waiting
    request = {"research_id": p["id"], "scope_kind": "discussion", "line_id": "line-02"}
    assert service.line_ids(request, {"action": "needs_external_input", "missing_items": ["line-01 baseline"]}) == ["line-01"]
    assert not service.can_route(request, {"action": "needs_external_input", "missing_items": ["Kaggle API credential"]})
    for invalid in [["line-99"], ["line-02"], ["line-01", "line-01"]]:
        with pytest.raises(ValueError):
            service.line_ids(request, {"dependency_line_ids": invalid})


@pytest.mark.asyncio
async def test_verify_and_ordinary_boss_publication_trigger_new_coordination_and_resume(waiting, tmp_path):
    store, _, service, p, root, job = waiting
    store.put("submission", "upstream", {"line_id": "line-01", "status": "awaiting_verification"}, p["id"])
    await service.dispatch(p["id"])
    first = store.get("dependency_wait", job["id"])["coordination_ids"][0]
    store.finish_task(first, result={})
    store.put("boss_effects", first, {"status": "applied"}, p["id"])
    store.put("verification", "verified", {"submission_version_id": "upstream", "passed": True}, p["id"])
    await service.dispatch(p["id"])
    ids = store.get("dependency_wait", job["id"])["coordination_ids"]
    assert len(ids) == 2
    second = ids[-1]
    store.finish_task(second, result={})
    store.put("boss_effects", second, {"status": "applied"}, p["id"])
    source = tmp_path / "baseline.json"
    source.write_text('{"score":0.7}', encoding="utf-8")
    artifact = store.register_artifact(p["id"], source, line_id="line-01")
    shared = {"boss_decision_id": "ordinary-upstream-boss", "receipt": {"persisted": True},
        "source_ids": [artifact["id"]], "content_markdown": "verified baseline"}
    store.put("memory_node", "baseline", shared, p["id"])
    await service.dispatch(p["id"])
    ids = store.get("dependency_wait", job["id"])["coordination_ids"]
    assert len(ids) == 3
    for _ in range(3):
        await service.dispatch(p["id"])
    assert store.get("dependency_wait", job["id"])["coordination_ids"] == ids
    # Coordination itself may have been revised after a memory conflict.
    store.put("boss_reconsideration", ids[-1], {"replacement_task": {"id": "reconsidered"}}, p["id"])
    store.put("boss_effects", "reconsidered", {"status": "applied"}, p["id"])
    store.put("memory_node", "accepted", {**shared, "boss_decision_id": "reconsidered"}, p["id"])
    store.finish_task(ids[-1], result={})
    await service.dispatch(p["id"])
    assert store.get("dependency_wait", job["id"])["status"] == "fulfilled"
    assert store.get("task", root["id"])["status"] == "pending"
