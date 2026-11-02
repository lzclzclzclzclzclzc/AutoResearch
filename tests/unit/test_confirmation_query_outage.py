import pytest

from backend.persistence import Store
from backend.runtime.control import ControlSupervisor, ControlUnavailable


@pytest.fixture
def durable_business(tmp_path):
    store = Store(tmp_path)
    control = ControlSupervisor(tmp_path / "control")
    try:
        yield store, control
    finally:
        control.close()


def project(store, control, name):
    saved = store.create_project({"client_request_id": name, "topic": "Confirmation recovery",
        "mode": "demo", "max_runtime_seconds": 600,
        "team": {"n": 1, "m1": 1, "m2": 1, "boss_members": 1}})
    saved = store.start_project(saved["id"])
    control.register_project(saved["id"], saved["deadline_at"])
    store.finish_task(saved["id"] + ":supervisor", result={})
    return saved["id"]


def query_outage(*args, **kwargs):
    raise ControlUnavailable("Injected transient Control status query failure")


def pending_status(store, operation_id):
    with store.connect() as connection:
        return connection.execute("SELECT status FROM business_operations WHERE id=?",
            (operation_id,)).fetchone()[0]


def lose_confirmation_response(monkeypatch, control):
    original = control.confirm_operation

    def lose_response(*args, **kwargs):
        original(*args, **kwargs)
        raise TimeoutError("Injected response loss after durable confirmation")

    monkeypatch.setattr(control, "confirm_operation", lose_response)


def boss_decision(store, control, research_id):
    decision_id = research_id + ":decision"
    store.commit_boss_decision(research_id, decision_id, {
        "action": "assign", "request_report": True,
        "assignments": [{"line_id": "line-01", "objective": "Continue research"}],
    }, control)
    return decision_id


def pending_verification(store, control, research_id, monkeypatch, passed=False):
    cycle_id, submission_id = research_id + ":cycle", research_id + ":submission"
    store.put("cycle", cycle_id, {"line_id": "line-01", "status": "verifying",
        "k": 3, "completed_verify_rounds": 0}, research_id)
    store.update_line(research_id, "line-01", cycle_id=cycle_id)
    binding = {"spec_id": research_id + ":spec", "code_version": "code", "run_ids": ["run"]}
    store.put("submission", submission_id, {"cycle_id": cycle_id, **binding}, research_id)
    report = {"passed": passed, "checks": ["Compared plan and saved metrics"], **binding}
    with monkeypatch.context() as failure:
        lose_confirmation_response(failure, control)
        pending = store.commit_verification(cycle_id, submission_id, report, 1, control)
    assert pending["route"] == "awaiting_confirmation_sync"
    assert control.get_confirmation(pending["operation_id"])["persisted"]
    return pending["operation_id"], cycle_id, submission_id, report


@pytest.mark.parametrize("entrypoint", ["direct_apply", "receipt_recovery"])
def test_confirmed_boss_tasks_wait_for_control_query_and_recover_once(durable_business, monkeypatch, entrypoint):
    store, control = durable_business
    research_id = project(store, control, "boss-query")
    decision_id = boss_decision(store, control, research_id)
    original_lines = store.lines(research_id)
    if entrypoint == "receipt_recovery":
        with monkeypatch.context() as failure:
            lose_confirmation_response(failure, control)
            pending = store.apply_boss_effects(research_id, decision_id, control=control)
        assert pending["status"] == "awaiting_confirmation_sync"

    with monkeypatch.context() as outage:
        outage.setattr(control, "allows_progress", query_outage)
        if entrypoint == "direct_apply":
            pending = store.apply_boss_effects(research_id, decision_id, control=control)
            assert pending["status"] == "awaiting_confirmation_sync"
        operation_id = pending["operation_id"]
        assert control.get_confirmation(operation_id)["persisted"]
        assert store.recover_confirmed_operations(control) == {
            "applied": [], "pending": [operation_id], "conflicts": []}
        assert pending_status(store, operation_id) == "pending_control_confirmation"
        assert store.get("boss_effects", decision_id) is None
        assert not store.list("cycle", research_id)
        assert not store.list("report_snapshot", research_id)
        assert not store.pending_tasks()
        assert store.lines(research_id) == original_lines
        assert store.project(research_id)["status"] == "running"

    assert store.recover_confirmed_operations(control)["applied"] == [operation_id]
    effects = store.get("boss_effects", decision_id)
    assert effects["dispatch_allowed"] is True
    assert len(effects["task_ids"]) == 2
    assert all(store.get("task", key)["status"] == "pending" for key in effects["task_ids"])
    assert store.get("cycle", effects["cycle_ids"][0])["status"] == "active"
    assert store.apply_boss_effects(research_id, decision_id, control=control) == effects
    assert store.recover_confirmed_operations(control)["applied"] == []
    assert len(store.list("cycle", research_id)) == len(store.list("report_snapshot", research_id)) == 1


@pytest.mark.parametrize("passed", [False, True])
def test_confirmed_verify_waits_without_counting_or_cancelling_and_delivers_after_recovery(
        durable_business, monkeypatch, passed):
    store, control = durable_business
    research_id = project(store, control, "verify-query")
    operation_id, cycle_id, submission_id, report = pending_verification(
        store, control, research_id, monkeypatch, passed)
    with monkeypatch.context() as outage:
        outage.setattr(control, "allows_progress", query_outage)
        for _ in range(2):
            assert store.recover_confirmed_operations(control) == {
                "applied": [], "pending": [operation_id], "conflicts": []}
        assert pending_status(store, operation_id) == "pending_control_confirmation"
        assert store.get("cycle", cycle_id)["completed_verify_rounds"] == 0
        assert store.get("cycle", cycle_id)["status"] == "verifying"
        assert not store.list("verification", research_id)
        assert not store.list("outbox", research_id)
        assert store.project(research_id)["status"] == "running"

    assert store.recover_confirmed_operations(control)["applied"] == [operation_id]
    assert store.get("cycle", cycle_id)["completed_verify_rounds"] == 1
    outbox = store.get("outbox", operation_id)
    assert outbox["status"] == "pending"
    assert outbox["payload"]["route"] == ("boss" if passed else "exploration_and_critic")
    assert store.dispatch_outbox(control)["delivered"] == [operation_id]
    assert store.commit_verification(cycle_id, submission_id, report, 1, control) == outbox["payload"]
    assert store.get("cycle", cycle_id)["completed_verify_rounds"] == 1
    assert len(store.list("verification", research_id)) == 1
    assert store.recover_confirmed_operations(control)["applied"] == []


@pytest.mark.parametrize("kind", ["boss", "verification"])
def test_stop_during_query_outage_still_prevents_later_dispatch(durable_business, monkeypatch, kind):
    store, control = durable_business
    research_id = project(store, control, "stop-during-outage")
    if kind == "boss":
        decision_id = boss_decision(store, control, research_id)
        with monkeypatch.context() as outage:
            outage.setattr(control, "allows_progress", query_outage)
            pending = store.apply_boss_effects(research_id, decision_id, control=control)
        operation_id = pending["operation_id"]
    else:
        operation_id, cycle_id, _, _ = pending_verification(store, control, research_id, monkeypatch)
        with monkeypatch.context() as outage:
            outage.setattr(control, "allows_progress", query_outage)
            assert store.recover_confirmed_operations(control)["pending"] == [operation_id]

    control.terminate(research_id)
    assert not control.allows_progress(research_id)
    assert store.recover_confirmed_operations(control)["applied"] == [operation_id]
    if kind == "boss":
        effects = store.get("boss_effects", decision_id)
        assert effects["dispatch_allowed"] is False
        assert all(store.get("task", key)["status"] == "cancelled" for key in effects["task_ids"])
        assert store.get("cycle", effects["cycle_ids"][0])["status"] == "archived"
    else:
        assert store.get("cycle", cycle_id)["status"] == "stopped"
        assert store.get("outbox", operation_id)["status"] == "cancelled"


def test_query_outage_for_one_project_does_not_block_other_receipt_recovery(durable_business, monkeypatch):
    store, control = durable_business
    first, second = (project(store, control, name) for name in ("unavailable", "available"))
    first_op, first_cycle, _, _ = pending_verification(store, control, first, monkeypatch)
    second_op, second_cycle, _, _ = pending_verification(store, control, second, monkeypatch)
    original = control.allows_progress

    def query(research_id, *args, **kwargs):
        if research_id == first:
            return query_outage()
        return original(research_id, *args, **kwargs)

    monkeypatch.setattr(control, "allows_progress", query)
    assert store.recover_confirmed_operations(control) == {
        "applied": [second_op], "pending": [first_op], "conflicts": []}
    assert store.get("cycle", first_cycle)["completed_verify_rounds"] == 0
    assert store.get("cycle", second_cycle)["completed_verify_rounds"] == 1
