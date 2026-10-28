"""Queue eligibility, initial Control registration and unbound grant recovery."""
import asyncio
import concurrent.futures
import time
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient
from langgraph.checkpoint.memory import MemorySaver
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from apps.api.main import create_app
from backend.adapters.dsh import ResearchStopped, RuntimeBlocked
from backend.graphs.engine import ResearchEngine
from backend.graphs.tasks import ResearchSupervisorGraph
from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor
from backend.runtime.processes import detect_resources
from backend.runtime.runner import ResourceConflict, ResourceService, RunnerSupervisor
from backend.runtime.worker import recover_running_tasks
from backend.services.credentials import CredentialService
from packages.contracts.research import ExperimentSpec, digest


class OpenGate:
    def allows_progress(self, *args):
        return True


def make_request(store, name, control=None, **resources):
    project = store.start_project(store.create_project({"client_request_id": name, "topic": name, "mode": "demo",
        "team": {"n": 1, "m1": 1, "m2": 1, "boss_members": 1}, "max_runtime_seconds": 600})["id"])
    if control:
        control.register_project(project["id"], project["deadline_at"])
    spec = ExperimentSpec(spec_id="spec-" + name, version=1, question=name, hypothesis="fixture", fixed={},
        baseline="none", data={}, metrics=["value"], design={}, monitoring={}, completion_criteria="exit",
        deadline_at=project["deadline_at"])
    store.put("spec", spec.spec_id, spec.model_dump(), project["id"])
    return project, {"research_id": project["id"], "implementation_agent_id": "line-01/implementation", "line_id": "line-01",
        "spec_id": spec.spec_id, "spec_version": 1, "spec_hash": digest(spec.model_dump()),
        "resource_request_id": name, "control_generation": 1, "cpu_cores": 1, "memory_bytes": 2**20,
        "gpu_count": 0, **resources}


@pytest.fixture
def ledger(tmp_path, monkeypatch):
    capacity = {"cpu_cores": 1, "memory_total_bytes": 2**30, "memory_available_bytes": 2**30, "gpus": []}
    monkeypatch.setattr("backend.runtime.runner.detect_resources", lambda *args: capacity)
    store = Store(tmp_path)
    return store, ResourceService(store.path, OpenGate(), capacity)


@pytest.mark.parametrize("status", ["pausing", "paused", "blocked", "completed", "terminated", "quarantined", "obsolete", "expired"])
def test_ineligible_waiter_does_not_block_another_project(ledger, status):
    store, service = ledger
    holder, request = make_request(store, "holder")
    service.request_allocation(request)
    parked, request = make_request(store, "parked", priority=10)
    queued = service.request_allocation(request)
    assert queued["state"] == "queued"
    if status == "obsolete":
        store.update_project(parked["id"], control_generation=2)
    elif status == "expired":
        store.update_project(parked["id"], deadline_at="2000-01-01T00:00:00Z")
    else:
        store.update_project(parked["id"], status=status)
    service.cancel_unbound(holder["id"], "fixture_release")
    active, request = make_request(store, "active")
    assert service.request_allocation(request)["state"] == "granted"
    assert store.get("allocation", queued["allocation_id"])["state"] == "queued"
    if status in {"pausing", "paused", "blocked"}:
        service.cancel_unbound(active["id"], "fixture_release")
        store.update_project(parked["id"], status="running")
        resumed = service.request_allocation(queued)
        assert resumed["state"] == "granted"
        assert resumed["requested_at"] == queued["requested_at"]


def test_runnable_older_waiter_still_has_priority(ledger):
    store, service = ledger
    holder, request = make_request(store, "holder")
    service.request_allocation(request)
    _, request = make_request(store, "older", priority=10)
    older = service.request_allocation(request)
    service.cancel_unbound(holder["id"], "fixture_release")
    _, request = make_request(store, "newer")
    assert service.request_allocation(request)["state"] == "queued"
    assert service.request_allocation(older)["state"] == "granted"


def expire_grant(service, grant):
    with service.connect(True) as c:
        service._put_allocation(c, {**grant, "grant_expires_at": time.time() - 1})


def test_expired_grant_renews_once_and_rejects_old_start_fence(ledger):
    store, service = ledger
    _, request = make_request(store, "expiry")
    old = service.request_allocation(request)
    expire_grant(service, old)
    with concurrent.futures.ThreadPoolExecutor(2) as pool:
        renewed = list(pool.map(service.request_allocation, [request, request]))
    fresh = renewed[0]
    assert renewed[1] == fresh
    assert fresh["allocation_id"] == old["allocation_id"]
    assert fresh["state"] == "granted" and fresh["grant_revision"] == old["grant_revision"] + 1
    assert fresh["fencing_token"] != old["fencing_token"]
    start = {"experiment_run_id": "run", "start_operation_id": "start", "code_version": "code"}
    with pytest.raises(ResourceConflict, match="binding mismatch"):
        service.begin_start({**old, **start}, "runner", "generation")
    run = service.begin_start({**fresh, **start}, "runner", "generation")
    assert run["state"] == "starting"
    replay = service.request_allocation(request)
    assert replay["bound_run_id"] == "run" and replay["grant_revision"] == fresh["grant_revision"]
    service.release({**run, "process_tree_exited": True, "start_operation_terminal": True}, "completed")
    released = service.request_allocation(request)
    assert released["state"] == "released" and released["grant_revision"] == fresh["grant_revision"]


@pytest.mark.parametrize("pinned", [False, True])
def test_grant_renewal_preserves_requested_devices_but_can_replace_an_unpinned_gpu(ledger, pinned):
    store, service = ledger
    gpus = [{"uuid": identity, "memory_free_bytes": 2**30, "occupancy_known": True, "process_ids": []}
            for identity in ("gpu-a", "gpu-b")]
    service.capacity["gpus"] = gpus
    _, request = make_request(store, "gpu", gpu_count=1, gpu_uuids=["gpu-a"] if pinned else [])
    grant = service.request_allocation(request)
    assert grant["gpu_uuids"] == ["gpu-a"]
    expire_grant(service, grant)
    gpus[0]["process_ids"] = [123]
    renewed = service.request_allocation(grant)
    assert renewed["requested_gpu_uuids"] == request["gpu_uuids"]
    assert renewed["state"] == ("queued" if pinned else "granted")
    assert renewed["gpu_uuids"] == ([] if pinned else ["gpu-b"])


def test_renewal_cannot_change_reserved_quantities(ledger):
    store, service = ledger
    _, request = make_request(store, "immutable-resources")
    grant = service.request_allocation(request)
    expire_grant(service, grant)
    with pytest.raises(ResourceConflict, match="resource quantities"):
        service.request_allocation({**request, "memory_bytes": 2**19})
    assert service.request_allocation(request)["memory_bytes"] == request["memory_bytes"]


def test_cancelled_unbound_allocation_never_renews(ledger):
    store, service = ledger
    project, request = make_request(store, "cancelled")
    grant = service.request_allocation(request)
    service.cancel_unbound(project["id"], "user_requested")
    replay = service.request_allocation(request)
    assert replay["state"] == "expired" and replay["terminal_proof"]["never_admitted"]
    assert replay["grant_revision"] == grant["grant_revision"]


def api_client(store, control):
    app = create_app(store.data_dir, control_client=control, testing=True)
    app.state.store = store
    client = TestClient(app)
    client.headers["X-CSRF-Token"] = client.get("/api/session").json()["csrf_token"]
    return client


def test_api_cannot_register_new_control_identity_after_initial_supervision_completed(tmp_path):
    store = Store(tmp_path)
    project = store.start_project(store.create_project({"client_request_id": "completed-supervisor", "topic": "fixture", "mode": "demo"})["id"])
    store.finish_task(store.pending_tasks()[0]["id"])
    control = ControlSupervisor(tmp_path / "control")
    try:
        with api_client(store, control) as client:
            response = client.post(f"/api/research/{project['id']}/commands", json={"kind": "recover", "client_request_id": "recover"})
        assert response.status_code == 409
        assert control.status(project["id"])["state"] == "unknown"
    finally:
        control.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("accepted", [False, True])
async def test_registration_retry_through_api_keeps_original_clock(tmp_path, monkeypatch, accepted):
    store = Store(tmp_path)
    project = store.create_project({"client_request_id": "registration", "topic": "registration", "mode": "demo"})
    rid = project["id"]
    control = ControlSupervisor(tmp_path / "control")
    saver = MemorySaver()
    engine = ResearchEngine(store, control, None, CredentialService(tmp_path, store), saver)
    engine.boss = AsyncMock(return_value={"initial_assignment": True})
    register = control.register_project
    attempts = []
    def lose_first_request(*args, **kwargs):
        attempts.append(args)
        if len(attempts) == 1:
            if accepted:
                register(*args, **kwargs)
            raise ConnectionError("Registration transport lost")
        return register(*args, **kwargs)
    monkeypatch.setattr(control, "register_project", lose_first_request)
    task = store.pending_tasks()[0]
    graph = ResearchSupervisorGraph(engine.supervisor, saver)
    try:
        with pytest.raises(ConnectionError):
            await graph.run(task)
        frozen = store.project(rid)
        store.finish_task(task["id"], error={"code": "connection_failed"})
        store.update_project(rid, status="blocked")
        with api_client(store, control) as client:
            response = client.post(f"/api/research/{rid}/commands", json={"kind": "retry", "client_request_id": "retry"})
        assert response.status_code == 202, response.text
        assert store.get("task", task["id"])["status"] == "pending"
        assert await graph.run(task) == {"initial_assignment": True}
        assert control.allows_progress(rid)
        assert len(attempts) >= 2
        assert store.project(rid)["started_at"] == frozen["started_at"]
        assert store.project(rid)["deadline_at"] == frozen["deadline_at"]
        assert engine.boss.await_count == 1
    finally:
        await engine.close()
        control.close()


@pytest.mark.asyncio
async def test_worker_recovers_supervisor_before_registration_but_keeps_other_work_blocked(tmp_path):
    store = Store(tmp_path)
    project = store.start_project(store.create_project({"client_request_id": "crashed", "topic": "crashed", "mode": "demo"})["id"])
    rid = project["id"]
    supervisor = store.claim_task(store.pending_tasks()[0]["id"], "old-worker")
    line = store.add_task(rid, "line", {}, "line-task")
    store.claim_task(line["id"], "old-worker")
    control = ControlSupervisor(tmp_path / "control")
    engine = ResearchEngine(store, control, None, CredentialService(tmp_path, store), MemorySaver())
    engine.boss = AsyncMock(return_value={"recovered": True})
    try:
        await recover_running_tasks(store, control)
        assert store.get("task", supervisor["id"])["status"] == "pending"
        assert store.get("task", line["id"])["status"] == "blocked"
        assert await engine.supervisor(supervisor) == {"recovered": True}
        assert control.allows_progress(rid)
        assert store.project(rid)["deadline_at"] == project["deadline_at"]
    finally:
        await engine.close()
        control.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["user_stop", "control_restart"])
async def test_registration_recovery_never_reopens_stopped_control(tmp_path, reason):
    store = Store(tmp_path)
    project = store.start_project(store.create_project({"client_request_id": "stopped", "topic": "stopped", "mode": "demo"})["id"])
    rid = project["id"]
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(rid, project["deadline_at"])
    if reason == "control_restart":
        control.close()
        control = ControlSupervisor(tmp_path / "control")
    else:
        control.terminate(rid)
    engine = ResearchEngine(store, control, None, CredentialService(tmp_path, store), MemorySaver())
    engine.boss = AsyncMock()
    try:
        with api_client(store, control) as client:
            response = client.post(f"/api/research/{rid}/commands", json={"kind": "recover", "client_request_id": "recover"})
        assert response.status_code == 409
        with pytest.raises(ResearchStopped):
            await engine.supervisor(store.pending_tasks()[0])
        assert not control.allows_progress(rid)
        engine.boss.assert_not_awaited()
        assert store.project(rid)["deadline_at"] == project["deadline_at"]
    finally:
        await engine.close()
        control.close()


@pytest.mark.asyncio
async def test_paused_checkpoint_renews_grant_then_waits_for_real_capacity(tmp_path, monkeypatch):
    # Shorten Windows paths for the real process and immutable artifacts.
    root = tmp_path.parent / "grant-recovery"
    root.mkdir()
    store = Store(root)
    project = store.create_project({"client_request_id": "pause", "topic": "pause", "mode": "demo",
        "team": {"n": 1, "m1": 1, "m2": 1, "boss_members": 1}, "max_runtime_seconds": 120})
    rid = project["id"]
    control = ControlSupervisor(root / "control")
    capacity = {**detect_resources(root), "cpu_cores": 1, "memory_total_bytes": 2**30, "memory_available_bytes": 2**30, "gpus": []}
    monkeypatch.setattr("backend.runtime.runner.detect_resources", lambda *args: capacity)
    runner = RunnerSupervisor(root, control, capacity)
    engine = None
    try:
        async with AsyncSqliteSaver.from_conn_string(str(root / "checkpoints.sqlite")) as saver:
            engine = ResearchEngine(store, control, runner, CredentialService(root, store), saver)
            await engine.supervisor(store.pending_tasks()[0])
            task = next(t for t in store.pending_tasks() if t["kind"] == "line")
            request_allocation, grants = runner.request_allocation, []
            def pause_after_grant(payload):
                result = request_allocation(payload)
                if result["state"] == "granted" and not grants:
                    grants.append(result)
                    store.update_project(rid, status="paused")
                return result
            monkeypatch.setattr(runner, "request_allocation", pause_after_grant)
            with pytest.raises(RuntimeBlocked) as error:
                await engine.line(task)
            assert error.value.code == "research_paused"
            old = grants[0]
            expire_grant(runner.service, old)
            holder, request = make_request(store, "holder", control)
            assert request_allocation(request)["state"] == "granted"
            store.update_project(rid, status="running")
            resumed = asyncio.create_task(engine.line(task))
            try:
                until = asyncio.get_running_loop().time() + 8
                while store.get("allocation", old["allocation_id"])["state"] != "queued":
                    assert not resumed.done(), resumed.exception() if resumed.done() else None
                    assert asyncio.get_running_loop().time() < until
                    await asyncio.sleep(0.05)
                fresh = store.get("allocation", old["allocation_id"])
                assert fresh["grant_revision"] == old["grant_revision"] + 1
                assert fresh["fencing_token"] != old["fencing_token"]
                assert not store.list("experiment_run", rid)
                runner.service.cancel_unbound(holder["id"], "fixture_release")
                await asyncio.wait_for(resumed, timeout=20)
            finally:
                if not resumed.done():
                    resumed.cancel()
                    await asyncio.gather(resumed, return_exceptions=True)
            await engine.line(task)
            assert len(store.list("experiment_run", rid)) == 1
            assert len(store.list("submission", rid)) == len(store.list("verification", rid)) == 1
            assert store.list("experiment_run", rid)[0]["state"] == "completed"
            assert store.list("experiment_run", rid)[0]["grant_revision"] == fresh["grant_revision"]
            assert store.project(rid)["deadline_at"] == store.get("spec", store.lines(rid)[0]["active_spec_id"])["deadline_at"]
            await engine.close()
            engine = None
    finally:
        if engine:
            await engine.close()
        runner.close()
        control.close()
