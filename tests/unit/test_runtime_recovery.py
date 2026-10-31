import sqlite3
from concurrent.futures import ThreadPoolExecutor

import pytest

from backend.persistence.store import Store, StoreError
from backend.persistence.resilience import is_storage_busy
from backend.runtime.recovery import recover_infrastructure_tasks, execution_exit_proven, legacy_failure
from backend.runtime.control import ControlSupervisor


def test_discussion_format_failure_retries_current_task_without_business_arbitration():
    from backend.adapters.dsh import RuntimeBlocked
    from backend.runtime.recovery import retryable_failure
    failure = retryable_failure(RuntimeBlocked("discussion_technical_failure", "当前票的选项不存在"))
    assert failure and failure[0] == "local_service_unavailable"
    assert "保留已收票" in failure[1]


def project(store):
    return store.start_project(store.create_project({"topic": "recovery", "client_request_id": "recovery", "mode": "demo"})["id"])


def test_real_database_lock_is_retryable_and_transaction_is_not_partially_applied(tmp_path):
    store = Store(tmp_path)
    p = project(store)
    with sqlite3.connect(store.path) as writer:
        writer.execute("BEGIN IMMEDIATE")
        with pytest.raises(StoreError) as failure:
            store.put("fixture", "one", {"value": 1}, p["id"])
        assert failure.value.code == "storage_busy" and is_storage_busy(failure.value)
        assert store.get("fixture", "one") is None
    assert store.put("fixture", "one", {"value": 1}, p["id"])["value"] == 1
    assert not is_storage_busy(sqlite3.OperationalError("no such table: missing"))


def test_concurrent_store_instances_share_writer_queue_and_preserve_every_event(tmp_path):
    first, second = Store(tmp_path), Store(tmp_path)
    p = project(first)
    assert first._write_lock is second._write_lock
    def write(i):
        (first if i % 2 else second).emit(p["id"], "fixture", {"index": i})
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(write, range(64)))
    events = [e for e in first.events(p["id"]) if e["type"] == "fixture"]
    assert len(events) == len({e["seq"] for e in events}) == 64


def test_retry_wait_is_durable_not_claimed_early_and_not_reported_as_idle_block(tmp_path):
    store = Store(tmp_path)
    p = project(store)
    for task in store.pending_tasks():
        store.finish_task(task["id"], result={})
    for line in store.lines(p["id"]):
        store.update_line(p["id"], line["line_id"], phase="blocked", error={"code": "fixture"})
    task = store.add_task(p["id"], "line", {"line_id": "line-01"}, "retry")
    store.claim_task(task["id"], "owner")
    store.retry_task(task["id"], "storage_busy", "retry", delay_seconds=20)
    assert not store.pending_tasks() and store.claim_task(task["id"], "other") is None
    assert store.reconcile_project_liveness(p["id"])["status"] == "running"
    store.patch("task", task["id"], retry_not_before="2000-01-01T00:00:00Z")
    assert store.pending_tasks()[0]["id"] == task["id"]
    assert store.claim_task(task["id"], "other")["attempt"] == 2


def test_idle_failed_lines_block_project_but_new_recovery_unblocks_only_aggregate_error(tmp_path):
    store = Store(tmp_path)
    p = project(store)
    for task in store.pending_tasks():
        store.finish_task(task["id"], result={})
    for line in store.lines(p["id"]):
        store.update_line(p["id"], line["line_id"], phase="blocked", error={"code": "fixture"})
    assert store.reconcile_project_liveness(p["id"])["error"]["code"] == "all_lines_blocked"
    store.add_task(p["id"], "boss", {}, "new-recovery")
    assert store.reconcile_project_liveness(p["id"])["status"] == "running"
    store.update_project(p["id"], status="blocked", blocked_reason="credential_missing", error={"code": "credential_missing"})
    assert store.reconcile_project_liveness(p["id"])["status"] == "blocked"


def test_stop_cancels_scheduled_retry_instead_of_waking_a_stopped_task(tmp_path):
    store = Store(tmp_path)
    p = project(store)
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(p["id"], p["deadline_at"])
    task = store.add_task(p["id"], "paper", {}, "retry-stop")
    store.retry_task(task["id"], "storage_busy", "retry", delay_seconds=10)
    try:
        status = control.terminate(p["id"])
        store.sync_stop(p["id"], status)
        assert store.get("task", task["id"])["status"] == "cancelled"
        assert store.claim_task(task["id"], "other") is None
    finally:
        control.close()


def test_completion_cache_session_and_event_rollback_together_and_replay_once(tmp_path, monkeypatch):
    store = Store(tmp_path)
    p = project(store)
    store.put("dsh_session", "session", {"has_history": False}, p["id"])
    store.put("execution", "execution", {"status": "running", "execution_attempt_id": "attempt",
        "agent_id": "paper", "task_id": "task"}, p["id"])
    original_emit = store._emit
    def emit(c, rid, event_type, *args, **kwargs):
        if event_type == "agent.completed":
            raise sqlite3.OperationalError("database is locked")
        return original_emit(c, rid, event_type, *args, **kwargs)
    monkeypatch.setattr(store, "_emit", emit)
    with pytest.raises(StoreError) as error:
        store.complete_model_turn("execution", {"result": "done"}, payload={}, session_id="session")
    assert error.value.code == "storage_busy"
    assert store.get("execution", "execution")["status"] == "running"
    assert not store.get("dsh_session", "session")["has_history"]
    monkeypatch.setattr(store, "_emit", original_emit)
    for _ in range(2):
        store.complete_model_turn("execution", {"result": "done"}, payload={}, session_id="session")
    assert store.get("execution", "execution")["output"] == {"result": "done"}
    assert store.get("dsh_session", "session")["has_history"]
    assert len([e for e in store.events(p["id"]) if e["type"] == "agent.completed"]) == 1


def test_empty_checkpoint_database_does_not_turn_unknown_error_into_lock_failure(tmp_path):
    store = Store(tmp_path)
    p = project(store)
    with sqlite3.connect(tmp_path / "checkpoints.sqlite"):
        pass
    assert legacy_failure(store, {"research_id": p["id"], "id": "task", "kind": "paper",
        "error": {"code": "OperationalError"}}) is None


@pytest.mark.asyncio
async def test_exit_proof_does_not_wait_for_unrelated_task_or_god_and_denies_unknown_live_identity(tmp_path, monkeypatch):
    import psutil
    store = Store(tmp_path)
    p = project(store)
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(p["id"], p["deadline_at"])
    task = store.add_task(p["id"], "paper", {}, "root")
    store.put("execution", "unrelated", {"status": "running", "task_id": "root-other",
        "agent_id": "god/other", "execution_attempt_id": "unrelated", "pid": 1234}, p["id"])
    try:
        assert await execution_exit_proven(store, control, task)
        assert store.get("execution", "unrelated")["status"] == "running"
        store.put("execution", "related", {"status": "running", "task_id": task["id"] + ":discussion",
            "agent_id": "paper", "execution_attempt_id": "related", "pid": 5678}, p["id"])
        def inaccessible(pid):
            raise psutil.AccessDenied(pid)
        monkeypatch.setattr(psutil, "Process", inaccessible)
        assert not await execution_exit_proven(store, control, task)
        assert store.get("execution", "related")["status"] == "running"
    finally:
        control.close()


@pytest.mark.asyncio
async def test_legacy_storage_failure_only_recovers_matching_checkpoint_and_never_stopped(tmp_path):
    store = Store(tmp_path)
    p = project(store)
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(p["id"], p["deadline_at"])
    task = store.add_task(p["id"], "line", {"line_id": "line-01", "direction_epoch": 1}, "legacy")
    store.finish_task(task["id"], error={"code": "OperationalError", "message": "generic"})
    with sqlite3.connect(tmp_path / "checkpoints.sqlite") as c:
        c.execute("CREATE TABLE writes (thread_id TEXT,channel TEXT,value BLOB)")
        c.execute("INSERT INTO writes VALUES (?,?,?)", (f"research/{p['id']}/line/line-02/1/cycle", "__error__", b"OperationalError('database is locked')"))
    try:
        assert not await recover_infrastructure_tasks(store, control)
        with sqlite3.connect(tmp_path / "checkpoints.sqlite") as c:
            c.execute("INSERT INTO writes VALUES (?,?,?)", (f"research/{p['id']}/line/line-01/1/cycle", "__error__", b"OperationalError('database is locked')"))
        assert await recover_infrastructure_tasks(store, control) == [task["id"]]
        store.finish_task(task["id"], error={"code": "storage_busy"})
        control.terminate(p["id"])
        assert not await recover_infrastructure_tasks(store, control)
    finally:
        control.close()
