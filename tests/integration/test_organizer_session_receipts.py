"""Shared Organizer receipts preserve both session and project ownership."""
import asyncio
import sqlite3
import sys
from types import SimpleNamespace

import pytest

from backend.adapters.dsh import DshRuntimeAdapter, kill_tree
from backend.graphs.engine import ResearchEngine
from backend.persistence.store import Store, StoreError
from backend.runtime.control import ControlSupervisor


@pytest.fixture
def scene(tmp_path):
    store = Store(tmp_path)
    control = ControlSupervisor(tmp_path / "control")
    projects = []
    for request_id in ("first", "second"):
        project = store.start_project(store.create_project({
            "topic": "Organizer session ownership", "mode": "live", "client_request_id": request_id,
        })["id"])
        control.register_project(project["id"], project["deadline_at"])
        projects.append(project)
    adapter = DshRuntimeAdapter(store, control, None)
    try:
        yield SimpleNamespace(store=store, control=control, adapter=adapter, first=projects[0], second=projects[1])
    finally:
        control.close()


@pytest.mark.asyncio
async def test_actual_organizer_advice_completes_for_two_projects_on_one_sdk_session(scene, monkeypatch):
    data = scene
    credentials = SimpleNamespace(get_secret=lambda *args, **kwargs: "local-fixture-key")
    engine = ResearchEngine(data.store, data.control, object(), credentials, None)
    adapter = engine.live
    adapter.gateway = SimpleNamespace(url="http://127.0.0.1:1", register=lambda **kwargs: "local-capability",
        revoke=lambda *args, **kwargs: None, last_message=lambda *_: None, close=lambda: None)
    monkeypatch.setattr(adapter, "availability", lambda: {"available": True, "mode": "local-child-fixture"})
    original_spawn, children = asyncio.create_subprocess_exec, []

    async def spawn(*args, **kwargs):
        script = ("import json,sys; payload=json.loads(sys.stdin.readline()); "
            "output={'content_markdown':'resumed' if payload['resume'] else 'new', 'priority':0, 'reason':'local advice'}; "
            "print(json.dumps({'type':'result','finish_reason':'completed','final_response':json.dumps(output)}),flush=True)")
        child = await original_spawn(sys.executable, "-u", "-c", script, **kwargs)
        children.append(child)
        return child

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    tasks = [{"id": project["id"] + ":organizer", "research_id": project["id"],
        "payload": {"line_id": "line-01", "implementation_agent_id": "line-01/implementation"}}
        for project in (data.first, data.second)]
    try:
        first = await asyncio.wait_for(engine.organizer_advice(tasks[0]), 5)
        second = await asyncio.wait_for(engine.organizer_advice(tasks[1]), 5)
        assert first["content_markdown"] == "new"
        assert second["content_markdown"] == "resumed"
        sessions = data.store.list("dsh_session")
        assert len(sessions) == 1
        session = sessions[0]
        assert session["research_id"] == data.first["id"] and session["has_history"]
        for project, task, output in zip((data.first, data.second), tasks, (first, second)):
            executions = data.store.list("execution", project["id"])
            assert len(executions) == 1
            assert executions[0]["status"] == "completed" and executions[0]["output"] == output
            assert executions[0]["session_id"] == session["session_id"]
            completed = [event for event in data.store.events(project["id"]) if event["type"] == "agent.completed"]
            assert len(completed) == 1 and completed[0]["task_id"] == task["id"]
            assert await engine.organizer_advice(task) == output
            assert not data.control.status(project["id"])["unfinished"]
        assert len(children) == 2, "replaying either completed advice must not dispatch a model again"
        assert data.store.get("dsh_session", session["id"])["research_id"] == data.first["id"]
    finally:
        for child in children:
            if child.returncode is None:
                await asyncio.to_thread(kill_tree, child.pid)
            await asyncio.wait_for(child.wait(), 5)
        await adapter.close()


@pytest.mark.parametrize("invalid_binding", ["ordinary_session", "ordinary_execution", "wrong_agent", "non_machine_session", "wrong_sdk_session"])
def test_shared_exception_does_not_allow_other_cross_project_session_writes(scene, invalid_binding):
    data = scene
    session_agent = "paper" if invalid_binding in {"ordinary_session", "wrong_agent"} else "organizer"
    session = data.adapter.ensure_session(data.first["id"], session_agent)
    if invalid_binding == "non_machine_session":
        session = data.store.put("dsh_session", "project-local-organizer", {
            "agent_id": "organizer", "session_id": "ar-project-local-organizer", "has_history": False}, data.first["id"])
    execution_agent = "paper" if invalid_binding in {"ordinary_session", "ordinary_execution"} else "organizer"
    execution = data.store.put("execution", "attempt", {
        "agent_id": execution_agent, "task_id": "second-task", "execution_attempt_id": "attempt-1",
        "session_id": "unrelated-sdk-session" if invalid_binding == "wrong_sdk_session" else session["session_id"],
        "status": "running"}, data.second["id"])
    with pytest.raises(StoreError) as error:
        data.store.complete_model_turn("attempt", {"answer": "done"}, payload={}, session_id=session["id"])
    assert error.value.code == "cross_project_reference"
    assert data.store.get("execution", "attempt") == execution
    assert data.store.get("dsh_session", session["id"]) == session
    assert not [event for event in data.store.events(data.second["id"]) if event["type"] == "agent.completed"]


def test_shared_session_completion_still_rolls_back_and_replays_atomically(scene, monkeypatch):
    data = scene
    session = data.adapter.ensure_session(data.first["id"], "organizer")
    execution = data.store.put("execution", "attempt", {"agent_id": "organizer",
        "task_id": "second-task", "execution_attempt_id": "attempt-1", "session_id": session["session_id"],
        "status": "running"}, data.second["id"])
    emit = data.store._emit

    def unavailable(connection, rid, kind, *args, **kwargs):
        if kind == "agent.completed":
            raise sqlite3.OperationalError("database is locked")
        return emit(connection, rid, kind, *args, **kwargs)

    monkeypatch.setattr(data.store, "_emit", unavailable)
    with pytest.raises(StoreError) as error:
        data.store.complete_model_turn("attempt", {"answer": "done"}, payload={}, session_id=session["id"])
    assert error.value.code == "storage_busy"
    assert data.store.get("execution", "attempt") == execution
    assert data.store.get("dsh_session", session["id"]) == session
    monkeypatch.setattr(data.store, "_emit", emit)
    for _ in range(2):
        result = data.store.complete_model_turn("attempt", {"answer": "done"}, payload={}, session_id=session["id"])
        assert result["status"] == "completed" and result["research_id"] == data.second["id"]
    saved_session = data.store.get("dsh_session", session["id"])
    assert saved_session["has_history"] and saved_session["research_id"] == data.first["id"]
    assert len([event for event in data.store.events(data.second["id"]) if event["type"] == "agent.completed"]) == 1
    assert not [event for event in data.store.events(data.first["id"]) if event["type"] == "agent.completed"]
