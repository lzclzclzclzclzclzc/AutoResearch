"""Real local child lifetimes fence SDK sessions without making model requests."""
import asyncio
import sys
from types import SimpleNamespace

import psutil
import pytest
import pytest_asyncio
from pydantic import BaseModel

from backend.adapters.dsh import DshRuntimeAdapter, RuntimeBlocked, kill_tree
from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor
from backend.runtime.generation_budget import GenerationBudget


class Reply(BaseModel):
    resumed: bool


@pytest_asyncio.fixture
async def child_scene(tmp_path, monkeypatch):
    store = Store(tmp_path)
    control = ControlSupervisor(tmp_path / "control")
    def project(request_id):
        item = store.start_project(store.create_project({
            "topic": "Local process exit fixture", "mode": "live", "client_request_id": request_id,
        })["id"])
        control.register_project(item["id"], item["deadline_at"])
        return item
    first, other = project("first"), project("other")
    adapter = DshRuntimeAdapter(store, control,
        SimpleNamespace(get_secret=lambda *args, **kwargs: "synthetic-local-fixture-key"), runner=object())
    adapter.gateway = SimpleNamespace(url="http://127.0.0.1:1", register=lambda **kwargs: "fixture-capability",
        revoke=lambda *args, **kwargs: None, last_message=lambda *_: None, close=lambda: None)
    monkeypatch.setattr(adapter, "availability", lambda: {"available": True, "mode": "local-child-fixture"})
    original_spawn, children, owned_identities = asyncio.create_subprocess_exec, [], []
    async def spawn(*args, **kwargs):
        script = ("import json,sys,time,subprocess; payload=json.loads(sys.stdin.readline()); " +
            ("subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)'],stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,creationflags=0x08000000 if sys.platform=='win32' else 0); "
             "print(json.dumps({'type':'error','code':'model_response_invalid','message':'LOCAL INCOMPLETE RESPONSE'}),flush=True); time.sleep(60)"
             if not children else
             "print(json.dumps({'type':'result','finish_reason':'completed','final_response':json.dumps({'resumed':payload['resume']})}),flush=True)"))
        process = await original_spawn(sys.executable, "-u", "-c", script, **kwargs)
        children.append(process)
        return process
    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    # The failed cleanup must retain a real, still-live SDK owner.
    def incomplete_cleanup(pid, *, receipt=None):
        parent = psutil.Process(pid)
        identities = [{"pid": process.pid, "pid_create_time": process.create_time()}
            for process in [*parent.children(recursive=True), parent]]
        owned_identities.extend(identities)
        if receipt is not None:
            receipt.update(process_tree=identities, process_tree_complete=True)
        return False
    monkeypatch.setattr("backend.adapters.dsh.kill_tree", incomplete_cleanup)
    try:
        yield SimpleNamespace(store=store, control=control, adapter=adapter,
            first=first, other=other, children=children)
    finally:
        for identity in owned_identities:
            try:
                process = psutil.Process(identity["pid"])
                if process.create_time() == identity["pid_create_time"]:
                    await asyncio.to_thread(kill_tree, process.pid)
            except psutil.NoSuchProcess:
                pass
        for child in children:
            if child.returncode is None:
                await asyncio.to_thread(kill_tree, child.pid)
            await asyncio.wait_for(child.wait(), 5)
        await adapter.close()
        control.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("next_scope", ["same_turn", "correction", "different_task", "organizer_other_project"])
async def test_unconfirmed_exit_blocks_every_new_call_in_the_same_sdk_session(child_scene, next_scope):
    data = child_scene
    arguments = {"research_id": data.first["id"],
        "agent_id": "organizer" if next_scope == "organizer_other_project" else "boss/member-01",
        "role": "organizer" if next_scope == "organizer_other_project" else "boss",
        "task_id": "original-task", "turn_id": "answer", "prompt": "LOCAL CHILD ONLY", "schema": Reply}
    with pytest.raises(RuntimeBlocked) as initial:
        await asyncio.wait_for(data.adapter.execute_turn(**arguments), 5)
    assert initial.value.code == "execution_unknown"
    assert initial.value.__cause__.code == "model_response_invalid"
    old = data.store.list("execution", data.first["id"])[0]
    assert old["status"] == "unknown" and old["exit_confirmed"] is False
    assert old["error_code"] == "model_response_invalid"
    assert old["execution_attempt_id"] in data.adapter.processes
    assert data.control.status(data.first["id"])["unfinished"]
    assert data.children[0].returncode is None

    following = {**arguments}
    if next_scope == "correction":
        following["turn_id"] = "answer:repair-2"
    elif next_scope == "different_task":
        following["task_id"] = "another-task"
    elif next_scope == "organizer_other_project":
        following.update(research_id=data.other["id"], task_id="another-project-task")
    with pytest.raises(RuntimeBlocked) as blocked:
        await asyncio.wait_for(data.adapter.execute_turn(**following), 5)
    assert blocked.value.code == "execution_unknown"
    assert len(data.children) == 1
    assert data.store.get("execution", old["id"])["status"] == "unknown"

    assert await asyncio.to_thread(kill_tree, data.children[0].pid)
    await asyncio.wait_for(data.children[0].wait(), 5)
    assert await asyncio.wait_for(data.adapter.execute_turn(**following), 5) == {"resumed": True}
    assert len(data.children) == 2
    if next_scope != "same_turn":
        reconciled = data.store.get("execution", old["id"])
        assert reconciled["status"] == "interrupted" and reconciled["exit_confirmed"]
    assert old["execution_attempt_id"] not in data.adapter.processes
    assert not data.control.status(data.first["id"])["unfinished"]
    assert not data.control.status(data.other["id"])["unfinished"]
    # Completed output replay stays independent of new model admission.
    assert await data.adapter.execute_turn(**following) == {"resumed": True}
    completed = next(item for item in data.store.list("execution", following["research_id"])
        if item["status"] == "completed")
    assert (await data.adapter.cancel_execution(completed["execution_attempt_id"]))["exit_confirmed"]
    assert data.store.get("execution", completed["id"])["status"] == "completed"
    assert await data.adapter.execute_turn(**following) == {"resumed": True}
    assert len(data.children) == 2


@pytest.mark.asyncio
async def test_root_exit_with_surviving_descendant_fences_generation_session_and_cancel(child_scene, monkeypatch):
    data = child_scene
    budget = GenerationBudget(data.store, data.first["id"], {"kind": "discussion_generation", "key": "partial-cleanup"})
    attempt, _ = budget.acquire("boss/member-01", "partial-task", "turn")
    arguments = {"research_id": data.first["id"], "agent_id": "boss/member-01", "role": "boss",
        "task_id": "partial-task", "turn_id": attempt["turn_id"], "prompt": "LOCAL CHILD ONLY", "schema": Reply}
    with pytest.raises(RuntimeBlocked) as initial:
        await data.adapter.execute_turn(**arguments)
    assert initial.value.code == "execution_unknown"
    budget.record(attempt, "rejected", error="Legacy caller rejected the reply")
    execution = data.store.get("execution", attempt["execution_key"])
    assert execution["process_tree_complete"]
    descendants = [item for item in execution["process_tree"] if item["pid"] != execution["pid"]]
    assert descendants
    root = psutil.Process(execution["pid"])
    root.kill()
    await asyncio.to_thread(root.wait, timeout=3)
    assert any(psutil.pid_exists(item["pid"]) for item in descendants)

    with pytest.raises(RuntimeBlocked) as blocked_budget:
        budget.acquire("boss/member-01", "partial-task", "turn")
    assert blocked_budget.value.code == "execution_unknown"
    with pytest.raises(RuntimeBlocked) as blocked_session:
        await data.adapter.execute_turn(**{**arguments, "turn_id": "new-correction"})
    assert blocked_session.value.code == "execution_unknown"
    assert len(data.children) == 1
    assert len(data.store.get("generation_budget", budget.key)["attempts"]) == 1

    with monkeypatch.context() as denied:
        denied.setattr("backend.runtime.processes.terminate_identity", lambda *args: False)
        assert not (await data.adapter.cancel_execution(execution["execution_attempt_id"]))["exit_confirmed"]
    assert any(psutil.pid_exists(item["pid"]) for item in descendants)
    assert data.control.status(data.first["id"])["unfinished"]
    assert (await data.adapter.cancel_execution(execution["execution_attempt_id"]))["exit_confirmed"]
    await asyncio.wait_for(data.children[0].wait(), 5)
    replacement, _ = budget.acquire("boss/member-01", "partial-task", "turn")
    assert replacement["number"] == 2
    assert await data.adapter.execute_turn(**{**arguments, "turn_id": replacement["turn_id"]}) == {"resumed": True}
