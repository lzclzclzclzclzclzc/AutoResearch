"""Hard Runner failure, restart reconciliation and historical Verify handoff."""
import asyncio
import json
import os
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import psutil
import pytest
import uvicorn
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from starlette.responses import JSONResponse

from backend.graphs.engine import ResearchEngine
from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor, _request, create_app as control_app
from backend.runtime.processes import identity_alive
from backend.runtime.runner import RunnerClient
from backend.runtime.worker import sync_control_stops
from backend.services.credentials import CredentialService


def eventually(predicate, timeout=10):
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        try:
            if predicate():
                return
        except (OSError, RuntimeError, ValueError):
            pass
        time.sleep(0.03)
    assert predicate()


@pytest.mark.skipif(os.name != "nt", reason="Windows production Job crash recovery")
@pytest.mark.asyncio
async def test_actual_runner_crash_reconciles_then_packages_and_verifies_once(tmp_path, monkeypatch):
    # Keep the existing Windows artifact layout below MAX_PATH even when the
    # caller's pytest base directory has a descriptive name.
    tmp_path = tmp_path.parent / "recovery"
    tmp_path.mkdir()
    token = "isolated-runner-recovery-fixture"
    monkeypatch.setenv("AUTORESEARCH_SESSION_TOKEN", token)
    store = Store(tmp_path)
    project = store.create_project({"client_request_id": "runner-recovery", "topic": "Runner crash fixture", "mode": "demo",
        "team": {"n": 1, "m1": 1, "m2": 1, "boss_members": 1, "k": 3}, "max_runtime_seconds": 120})
    rid = project["id"]
    control = ControlSupervisor(tmp_path / "control")
    app = control_app(supervisor=control)
    completion_failures = [0]

    @app.middleware("http")
    async def lose_one_completion(request, call_next):
        if request.url.path == "/complete-execution" and completion_failures[0]:
            completion_failures[0] -= 1
            return JSONResponse(status_code=503, content={"code": "fixture_ack_lost"})
        return await call_next(request)

    control_socket = socket.socket()
    control_socket.bind(("127.0.0.1", 0))
    control_socket.listen()
    control_url = f"http://127.0.0.1:{control_socket.getsockname()[1]}"
    server = uvicorn.Server(uvicorn.Config(app, log_level="error"))
    server_thread = threading.Thread(target=server.run, kwargs={"sockets": [control_socket]}, daemon=True)
    server_thread.start()
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        runner_port = reservation.getsockname()[1]
    runner_url = f"http://127.0.0.1:{runner_port}"
    runner = RunnerClient(runner_url, token)
    root = Path(__file__).resolve().parents[2]
    processes, logs = [], []
    engine = None

    def launch_runner(number):
        owner_path = tmp_path / f"runner-owner-{number}.json"
        helper = """import json,os,sys,psutil,uvicorn
from pathlib import Path
from backend.runtime.control import ControlClient
from backend.runtime.runner import RunnerSupervisor,create_app
runner=RunnerSupervisor(sys.argv[1],ControlClient(sys.argv[2]),self_url=sys.argv[3])
Path(sys.argv[4]).write_text(json.dumps([os.getpid(),psutil.Process().create_time()]))
uvicorn.run(create_app(sys.argv[1],supervisor=runner),host='127.0.0.1',port=int(sys.argv[5]),log_level='error')
"""
        log = (tmp_path / f"runner-{number}.log").open("wb")
        logs.append(log)
        process = subprocess.Popen([sys.executable, "-c", helper, str(tmp_path), control_url, runner_url,
            str(owner_path), str(runner_port)], cwd=root, stdin=subprocess.DEVNULL, stdout=log, stderr=log,
            creationflags=0x08000000)
        record = {"process": process, "identity": None}
        processes.append(record)
        eventually(lambda: owner_path.exists() or process.poll() is not None)
        assert process.poll() is None, (tmp_path / f"runner-{number}.log").read_text(errors="replace")
        record["identity"] = tuple(json.loads(owner_path.read_text()))
        eventually(lambda: _request(runner_url + "/health", token=token)["status"] == "ok")
        return record

    def crash_runner(record):
        pid, born = record["identity"]
        assert identity_alive(pid, born)
        # Kill the exact Python owner, not its experiment descendants. Only
        # creation-time Job ownership can account for the tree disappearing.
        psutil.Process(pid).kill()
        record["process"].wait(timeout=5)
        eventually(lambda: not identity_alive(pid, born))

    try:
        eventually(lambda: server.started)
        first = launch_runner(1)
        async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "checkpoints.sqlite")) as saver:
            engine = ResearchEngine(store, control, runner, CredentialService(tmp_path, store), saver)
            produce = engine.demo._produce_turn

            async def long_experiment(**arguments):
                output = await produce(**arguments)
                if arguments["schema"].__name__ == "Implementation":
                    output["files"] = {"experiment.py": "import subprocess,sys,time,json,os\n"
                        "p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)'])\n"
                        "print(json.dumps({'parent':os.getpid(),'child':p.pid}),flush=True)\n"
                        "time.sleep(60)\n"}
                return output

            engine.demo._produce_turn = long_experiment
            task = next(t for t in store.pending_tasks() if t["kind"] == "supervisor")
            await engine.supervisor(task)
            line = next(t for t in store.pending_tasks() if t["kind"] == "line")
            state = {"research_id": rid, "task_id": line["id"], "revision": 1, **line["payload"]}
            state.update(await engine.design(state))
            state.update(await engine.implement(state))
            state.update(await engine.organize(state))
            allocation = store.get("allocation", state["allocation_id"])
            implementation = store.get("implementation", state["implementation_id"])
            run_id = state["implementation_id"] + ":run-1"
            payload = {**allocation, "experiment_run_id": run_id, "start_operation_id": run_id + ":start",
                "command": [sys.executable, "-u", implementation["entrypoint"], *implementation["arguments"]],
                "cwd": implementation["cwd"], "code_version": implementation["code_version"]}
            original = await asyncio.to_thread(runner.start, payload)
            eventually(lambda: runner.collect(run_id)["text"].strip())
            evidence = json.loads(runner.collect(run_id)["text"].strip())
            identities = {(original["pid"], original["process_create_time"])}
            identities.update((pid, psutil.Process(pid).create_time()) for pid in evidence.values())
            identities.update((p.pid, p.create_time()) for p in psutil.Process(original["pid"]).children(recursive=True))
            assert [e["execution_id"] for e in control.status(rid)["unfinished"]] == [run_id]
            crash_runner(first)
            eventually(lambda: all(not identity_alive(pid, born) for pid, born in identities))

            # Lose completion responses after release commits. The first
            # replacement retries; another hard failure must still leave enough
            # durable information for the next owner to clear Control once.
            completion_failures[0] = 1000
            second = launch_runner(2)
            eventually(lambda: completion_failures[0] < 998)
            assert control.status(rid)["unfinished"]
            assert runner.resources()["allocations"][0]["state"] == "released"
            crash_runner(second)
            completion_failures[0] = 0
            launch_runner(3)
            eventually(lambda: not control.status(rid)["unfinished"])
            recovered = runner.status(run_id)
            assert recovered["state"] == "interrupted"
            assert recovered["runner_generation"] == original["runner_generation"]
            assert runner.status()["runner_generation"] != original["runner_generation"]
            assert recovered["terminal_proof"]["proof_reason"] == "old_runner_exited_and_job_closed"
            assert runner.resources()["allocations"][0]["state"] == "released"
            assert runner.start(payload)["experiment_run_id"] == run_id
            with pytest.raises(Exception):
                runner.start({**payload, "start_operation_id": "different"})

            # Re-enter the node whose response/checkpoint was lost. The same
            # launch returns its terminal history and reaches real packaging.
            state.update(await asyncio.wait_for(engine.run_experiment(state), timeout=3))
            assert (await engine.assess_run(state))["run_route"] == "package"
            state.update(await engine.package(state))
            await engine.verify(state)
            await engine.verify(state)
            submission = store.get("submission", state["submission_id"])
            report = store.list("verification", rid)
            assert submission["run"]["state"] == "interrupted"
            assert submission["run_ids"] == [run_id]
            assert json.loads(submission["logs"]["text"].strip()) == evidence
            assert len(report) == 1 and not report[0]["passed"]
            assert report[0]["scientific_observation"] == "execution_failed"
            assert store.get("cycle", state["cycle_id"])["completed_verify_rounds"] == 1
            events = store.events(rid)
            failures = [e for e in events if e["type"] == "experiment.failed"]
            assert len(failures) == 1 and failures[0]["payload"]["state"] == "interrupted"
            assert len(store.list("experiment_run", rid)) == 1
            control.terminate(rid)
            await sync_control_stops(store, control)
            assert store.project(rid)["status"] == "terminated"
            assert control.status(rid)["state"] == "terminated"
            await engine.close()
            engine = None
    finally:
        if engine is not None:
            await engine.close()
        for record in reversed(processes):
            identity = record["identity"]
            if identity and identity_alive(*identity):
                psutil.Process(identity[0]).kill()
            process = record["process"]
            if process.poll() is None:
                process.wait(timeout=5)
        server.should_exit = True
        server_thread.join(timeout=5)
        control_socket.close()
        for log in logs:
            log.close()
        assert not server_thread.is_alive()
