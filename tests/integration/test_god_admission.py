import asyncio

import pytest

from backend.adapters.dsh import DshRuntimeAdapter, ResearchStopped, RuntimeBlocked
from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor


@pytest.mark.asyncio
@pytest.mark.parametrize("stop_scope", [True, False])
async def test_waiting_god_capacity_exits_when_scope_or_project_stops(tmp_path, stop_scope):
    store = Store(tmp_path)
    p = store.start_project(store.create_project({"topic": "admission", "client_request_id": "admission"})["id"])
    store.put("arbitration_request", "arb", {"status": "running"}, p["id"])
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(p["id"], p["deadline_at"])
    adapter = DshRuntimeAdapter(store, control, None)
    member_gate, capacity = asyncio.Lock(), asyncio.Semaphore(0)
    entered = asyncio.Event()
    async def wait():
        async with adapter._admission(p["id"], 1, {"arbitration_id": "arb"}, member_gate, capacity):
            entered.set()
    waiting = asyncio.create_task(wait())
    try:
        for _ in range(30):
            if store.events(p["id"])[-1]["type"] == "arbitration.capacity_wait":
                break
            await asyncio.sleep(0.01)
        if stop_scope:
            store.patch("arbitration_request", "arb", status="cancelled")
        else:
            control.terminate(p["id"])
        with pytest.raises(ResearchStopped):
            await asyncio.wait_for(waiting, 2)
        assert not entered.is_set() and not member_gate.locked()
        assert capacity._value == 0
    finally:
        waiting.cancel()
        await asyncio.gather(waiting, return_exceptions=True)
        control.close()


@pytest.mark.asyncio
async def test_capacity_admission_rechecks_pause_before_start_and_releases_slots(tmp_path):
    store = Store(tmp_path)
    p = store.start_project(store.create_project({"topic": "paused", "client_request_id": "paused"})["id"])
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(p["id"], p["deadline_at"])
    store.update_project(p["id"], status="paused")
    adapter = DshRuntimeAdapter(store, control, None)
    slots = asyncio.Semaphore(1)
    try:
        with pytest.raises(RuntimeBlocked) as error:
            async with adapter._admission(p["id"], 1, {}, slots):
                raise AssertionError("Paused research must not start a model")
        assert error.value.code == "research_paused" and slots._value == 1
    finally:
        control.close()


@pytest.mark.asyncio
async def test_uncontended_god_does_not_emit_fake_capacity_waits(tmp_path):
    store = Store(tmp_path)
    p = store.start_project(store.create_project({"topic": "capacity", "client_request_id": "capacity"})["id"])
    store.put("arbitration_request", "arb", {"status": "running"}, p["id"])
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(p["id"], p["deadline_at"])
    adapter = DshRuntimeAdapter(store, control, None)
    gates = [asyncio.Lock(), asyncio.Semaphore(32), asyncio.Semaphore(32)]
    try:
        async with adapter._admission(p["id"], 1, {"arbitration_id": "arb"}, *gates):
            pass
        assert not [e for e in store.events(p["id"]) if e["type"].startswith("arbitration.capacity_")]
        assert not gates[0].locked() and gates[1]._value == gates[2]._value == 32
    finally:
        control.close()


@pytest.mark.asyncio
async def test_god_wait_announced_once_across_multiple_busy_gates(tmp_path):
    store = Store(tmp_path)
    p = store.start_project(store.create_project({"topic": "queued", "client_request_id": "queued"})["id"])
    store.put("arbitration_request", "arb", {"status": "running"}, p["id"])
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(p["id"], p["deadline_at"])
    adapter = DshRuntimeAdapter(store, control, None)
    gates = [asyncio.Semaphore(0), asyncio.Semaphore(0)]
    async def release():
        for gate in gates:
            await asyncio.sleep(0.4)
            gate.release()
    releasing = asyncio.create_task(release())
    try:
        async with adapter._admission(p["id"], 1, {"arbitration_id": "arb"}, *gates):
            pass
        assert [e["type"] for e in store.events(p["id"]) if e["type"].startswith("arbitration.capacity_")] == [
            "arbitration.capacity_wait", "arbitration.capacity_acquired"]
        assert all(gate._value == 1 for gate in gates)
    finally:
        await releasing
        control.close()
