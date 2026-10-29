import asyncio
from types import SimpleNamespace

import pytest

from backend.persistence.store import Store, StoreError
from backend.runtime.control import ControlClient, ControlUnavailable
from backend.runtime.recovery import retryable_failure
from backend.runtime.worker import retry_startup, recover_running_tasks
from scripts.launch import WorkerWatchdog


def test_unknown_control_admission_is_retryable_but_explicit_denial_stays_denied(monkeypatch):
    control = ControlClient()
    def unavailable(*args, **kwargs):
        raise ControlUnavailable("temporary connection loss")
    monkeypatch.setattr(control, "_post", unavailable)
    with pytest.raises(ControlUnavailable) as error:
        control.allows_progress("research", 1)
    assert retryable_failure(error.value)[0] == "local_service_unavailable"
    assert retryable_failure(ControlUnavailable("corrupt journal", "control_journal_incomplete")) is None
    monkeypatch.setattr(control, "_post", lambda *a, **kw: {"allowed": False})
    assert control.allows_progress("research", 1) is False


@pytest.mark.parametrize("status", [409, 503])
def test_rejected_service_operations_are_not_transport_retries(monkeypatch, status):
    import io
    import urllib.error
    import backend.runtime.control as module
    def error(*args, **kwargs):
        raise urllib.error.HTTPError("http://127.0.0.1", status, "fixture", {}, io.BytesIO(b'{}'))
    monkeypatch.setattr(module.urllib.request, "urlopen", error)
    with pytest.raises(ControlUnavailable) as caught:
        ControlClient().allows_progress("research", 1)
    assert bool(retryable_failure(caught.value)) == (status == 503)


@pytest.mark.asyncio
async def test_startup_recovers_running_task_after_real_database_lock_releases(tmp_path):
    store = Store(tmp_path)
    project = store.create_project({"topic": "startup", "client_request_id": "startup", "mode": "demo"})
    task = store.pending_tasks()[0]
    store.claim_task(task["id"], "previous-worker")
    control = SimpleNamespace(allows_progress=lambda *args: True)
    # Hold the real SQLite writer independently of Store's in-process lock.
    import sqlite3
    writer = sqlite3.connect(store.path)
    writer.execute("BEGIN IMMEDIATE")
    recovery = asyncio.create_task(retry_startup(lambda: recover_running_tasks(store, control)))
    try:
        # The first recovery attempt times out on the real lock, then retries.
        await asyncio.sleep(.1)
        assert not recovery.done()
        writer.rollback()
        await asyncio.wait_for(recovery, 5)
        assert store.get("task", task["id"])["status"] == "pending"
        assert store.project(project["id"])["deadline_at"] is None
    finally:
        writer.close()
        recovery.cancel()
        await asyncio.gather(recovery, return_exceptions=True)


@pytest.mark.asyncio
async def test_startup_does_not_hide_permanent_errors_or_ignore_shutdown():
    def permanent():
        raise ValueError("bad schema")
    with pytest.raises(ValueError, match="bad schema"):
        await retry_startup(permanent)
    stop = asyncio.Event()
    def busy():
        stop.set()
        raise StoreError("storage_busy", "busy")
    with pytest.raises(asyncio.CancelledError):
        await retry_startup(busy, stop)


def test_launcher_restarts_only_exited_worker_with_backoff():
    watcher = WorkerWatchdog()
    dead = SimpleNamespace(poll=lambda: 1)
    alive = SimpleNamespace(poll=lambda: None)
    starts = []
    restart = lambda: starts.append(True)
    assert not watcher.check(dead, restart, 0)
    assert not watcher.check(dead, restart, .9)
    assert watcher.check(dead, restart, 1)
    assert not watcher.check(alive, restart, 2)
    assert not watcher.check(dead, restart, 3)
    assert not watcher.check(dead, restart, 4)
    assert watcher.check(dead, restart, 5)
    assert len(starts) == 2
