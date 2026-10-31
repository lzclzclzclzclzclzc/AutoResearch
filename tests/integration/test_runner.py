import concurrent.futures
import hashlib
import json
import os
import sqlite3
import sys
import subprocess
import threading
import time
from datetime import datetime, timezone

import pytest

from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor
from backend.runtime.processes import ManagedProcess, identity_alive, sanitized_environment
from backend.runtime.runner import ResourceConflict, RunnerSupervisor
from packages.contracts.research import ExperimentSpec, digest


def eventually(predicate, timeout=6):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return
        time.sleep(0.03)
    assert predicate()


@pytest.fixture
def setup_runtime(tmp_path):
    store = Store(tmp_path)
    project = store.create_project({"client_request_id": "cpu-integration", "topic": "CPU containment", "mode": "demo"})
    project = store.start_project(project["research_id"])
    rid = project["research_id"]
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(rid, project["deadline_at"])
    spec = ExperimentSpec(spec_id="spec-cpu", version=1, question="CPU example", hypothesis="Deterministic",
        fixed={"steps": 1}, baseline="none", data={"source": "synthetic"}, metrics=["elapsed"],
        design={"method": "sleep"}, monitoring={"signal": "stdout"}, completion_criteria="exits",
        deadline_at=project["deadline_at"])
    store.put("spec", spec.spec_id, {**spec.model_dump(), "spec_hash": digest(spec.model_dump()), "line_id": "line-01"}, rid)
    runner = RunnerSupervisor(tmp_path, control)
    request = {"research_id": rid, "implementation_agent_id": "line-01/implementation", "line_id": "line-01",
        "spec_id": spec.spec_id, "spec_version": 1, "spec_hash": digest(spec.model_dump()),
        "cpu_cores": 1, "memory_bytes": 268435456, "control_generation": 1,
        "resource_request_id": "request-cpu", "gpu_count": 0}
    yield store, control, runner, request
    runner.close()
    control.close()


def start_payload(allocation, tmp_path, command=None):
    return {**allocation, "experiment_run_id": "run-cpu", "start_operation_id": "start-cpu", "code_version": "code-cpu-v1",
        "cwd": str(tmp_path), "command": command or [sys.executable, "-c", "import json;print('actual CPU process', flush=True);open('metrics.json','w').write(json.dumps({'score':1.0}))"]}


def test_actual_cpu_process_and_idempotent_start(setup_runtime, tmp_path):
    store, _, runner, request = setup_runtime
    allocation = runner.request_allocation(request)
    assert allocation["state"] == "granted"
    payload = start_payload(allocation, tmp_path)
    run = runner.start(payload)
    assert run["pid"] > 0
    assert runner.start(payload)["pid"] == run["pid"]
    eventually(lambda: runner.status("run-cpu")["state"] == "completed")
    assert "actual CPU process" in runner.collect("run-cpu")["text"]
    assert runner.resources()["allocations"][0]["state"] == "released"
    events=store.events(request["research_id"])
    started=[e for e in events if e["type"]=="experiment.started"]
    stopped=[e for e in events if e["type"]=="experiment.stopped"]
    metrics=[e for e in events if e["type"]=="experiment.metric" and e["payload"]["kind"]=="scientific_metrics"]
    assert len(started)==len(stopped)==len(metrics)==1
    assert started[0]["seq"]<metrics[0]["seq"]<stopped[0]["seq"]
    assert metrics[0]["payload"]["metrics"]=={"score":1.0}
    assert started[0]["agent_id"]==request["implementation_agent_id"]
    assert stopped[0]["payload"]["state"]=="completed"
    with pytest.raises(ResourceConflict):
        runner.start({**payload, "start_operation_id": "changed"})


def test_failed_experiment_lifecycle_is_persisted_with_exit_code(setup_runtime,tmp_path):
    store,_,runner,request=setup_runtime
    allocation=runner.request_allocation(request)
    runner.start(start_payload(allocation,tmp_path,[sys.executable,"-c","raise SystemExit(7)"]))
    eventually(lambda:runner.status("run-cpu")["state"]=="execution_failed")
    failures=[e for e in store.events(request["research_id"]) if e["type"]=="experiment.failed"]
    assert len(failures)==1
    assert failures[0]["payload"]["exit_code"]==7
    assert failures[0]["payload"]["run_id"]=="run-cpu"
    assert failures[0]["payload"]["process_tree_exited"]


def test_nonfinite_metrics_keep_raw_evidence_and_strict_json_projections(setup_runtime,tmp_path):
    store,_,runner,request=setup_runtime
    allocation=runner.request_allocation(request)
    raw='{"loss":NaN,"up":Infinity,"down":-Infinity,"overflow":1e400,"count":4}'
    command=[sys.executable,"-c",f"open('metrics.json','w').write({raw!r})"]
    runner.start(start_payload(allocation,tmp_path,command))
    eventually(lambda:runner.status("run-cpu")["state"]=="completed")
    run=runner.status("run-cpu")
    assert run["metrics_numeric_status"]=="non_finite"
    assert run["metrics_snapshot"]["loss"]=={"value":"NaN","numeric_status":"non_finite"}
    collected=runner.collect("run-cpu")
    assert collected["metrics"]["up"]["value"]=="Infinity"
    metrics=[e for e in store.events(request["research_id"]) if e["type"]=="experiment.metric" and e["payload"]["kind"]=="scientific_metrics"]
    assert metrics[0]["payload"]["metrics_numeric_status"]=="non_finite"
    assert metrics[0]["payload"]["metrics"]["count"]==4
    from starlette.responses import JSONResponse
    snapshot=store.snapshot(request["research_id"])
    JSONResponse(snapshot)
    for value in (snapshot,collected,metrics):
        encoded=json.dumps(value,allow_nan=False)
        json.loads(encoded,parse_constant=lambda x:(_ for _ in ()).throw(ValueError(x)))
    assert (tmp_path/"metrics.json").read_text()==raw


def test_emergency_stop_kills_entire_tree_while_business_db_locked(setup_runtime, tmp_path):
    store, control, runner, request = setup_runtime
    allocation = runner.request_allocation(request)
    command = [sys.executable, "-c", "import subprocess,sys,time; p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)']);print(p.pid,flush=True);time.sleep(60)"]
    run = runner.start(start_payload(allocation, tmp_path, command))
    eventually(lambda: runner.collect("run-cpu")["text"].strip().isdigit())
    child_pid = int(runner.collect("run-cpu")["text"].strip())
    import psutil
    child_time = psutil.Process(child_pid).create_time()
    db = sqlite3.connect(store.path, isolation_level=None)
    db.execute("BEGIN IMMEDIATE")
    try:
        start = time.monotonic()
        control.terminate(request["research_id"])
        assert time.monotonic() - start < 0.3
        eventually(lambda: not identity_alive(run["pid"], run["process_create_time"]))
        eventually(lambda: not identity_alive(child_pid, child_time))
        assert not control.allows_progress(request["research_id"])
        # Resource ledger remains occupied until a terminal proof can commit after lock release.
        assert runner.service.resources()["allocations"][0]["state"] in ("running", "stopping", "released")
    finally:
        db.rollback()
        db.close()
    eventually(lambda: runner.resources()["allocations"][0]["state"] == "released")


def test_expiry_cannot_release_starting_and_proof_binding_is_required(setup_runtime, tmp_path):
    _, _, runner, request = setup_runtime
    allocation = runner.request_allocation(request)
    payload = start_payload(allocation, tmp_path)
    run = runner.service.begin_start(payload, runner.instance_id, runner.generation)
    with runner.service.connect(write=True) as connection:
        current = json.loads(connection.execute("SELECT data FROM runtime_allocations WHERE id=?", (allocation["allocation_id"],)).fetchone()[0])
        current["grant_expires_at"] = time.time() - 10
        runner.service._put_allocation(connection, current)
        runner.service._expire(connection, time.time())
    assert runner.resources()["allocations"][0]["state"] == "starting"
    with pytest.raises(ResourceConflict):
        runner.service.release({**runner._proof(run), "runner_generation": "obsolete"}, "cancelled")
    with pytest.raises(ResourceConflict):
        runner.service.release({**runner._proof(run), "process_tree_exited": False}, "cancelled")


def test_cancel_before_start_leaves_a_tombstone(setup_runtime, tmp_path):
    _, _, runner, request = setup_runtime
    allocation = runner.request_allocation(request)
    runner.request_stop("run-cpu")
    with pytest.raises(ResourceConflict):
        runner.start(start_payload(allocation, tmp_path))


def test_project_stop_expires_unused_grants(setup_runtime):
    _, control, runner, request = setup_runtime
    allocation = runner.request_allocation(request)
    assert allocation["state"] == "granted"
    control.terminate(request["research_id"])
    eventually(lambda: runner.resources()["allocations"][0]["state"] == "expired")


def test_cancel_during_start_cannot_release_or_create_late(setup_runtime, tmp_path):
    _, _, runner, request = setup_runtime
    allocation = runner.request_allocation(request)
    entered, release = threading.Event(), threading.Event()
    original = runner.journal._write
    def stalled(record):
        if record.get("kind") == "launch":
            entered.set()
            release.wait(3)
        return original(record)
    runner.journal._write = stalled
    with concurrent.futures.ThreadPoolExecutor() as executor:
        pending = executor.submit(runner.start, start_payload(allocation, tmp_path))
        assert entered.wait(1)
        before = time.monotonic()
        stopped = runner.request_stop("run-cpu")
        assert time.monotonic() - before < 0.2
        assert stopped["state"] == "stopping"
        # No PID yet is not sufficient proof: admission still owns its resource.
        assert runner.resources()["allocations"][0]["state"] == "starting"
        release.set()
        with pytest.raises(Exception):
            pending.result(timeout=4)
    assert "run-cpu" not in runner._processes
    assert runner.resources()["allocations"][0]["state"] == "released"


def test_control_connection_loss_stops_active_process(setup_runtime, tmp_path):
    _, control, runner, request = setup_runtime
    allocation = runner.request_allocation(request)
    run = runner.start(start_payload(allocation, tmp_path, [sys.executable, "-c", "import time;time.sleep(60)"]))
    original = control.status
    def unavailable(*args, **kwargs):
        raise ConnectionError("simulated control failure")
    control.status = unavailable
    try:
        eventually(lambda: not identity_alive(run["pid"], run["process_create_time"]))
        assert not runner._control_ok
    finally:
        control.status = original


@pytest.mark.skipif(os.name != "nt", reason="Windows exclusive Job ownership contract")
def test_supervisor_crash_closes_job_and_kills_children(tmp_path):
    from pathlib import Path
    root = Path(__file__).resolve().parents[2]
    evidence = tmp_path / "tree.json"
    code = "import subprocess,sys,time,json,os;p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)']);open(sys.argv[1],'w').write(json.dumps([os.getpid(),p.pid]));time.sleep(60)"
    helper = """
import json,sys,time
from pathlib import Path
from backend.runtime.processes import ManagedProcess,sanitized_environment
p=ManagedProcess([sys.executable,'-c',sys.argv[3],sys.argv[1]],sys.argv[2],sys.argv[1]+'.log',sanitized_environment())
p.resume()
for _ in range(200):
 if Path(sys.argv[1]).exists(): break
 time.sleep(.01)
print(Path(sys.argv[1]).read_text(),flush=True)
sys.stdin.readline()
"""
    supervisor = subprocess.Popen([sys.executable, "-c", helper, str(evidence), str(tmp_path), code], cwd=root,
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        creationflags=0x08000000)
    try:
        line = supervisor.stdout.readline()
        assert line, supervisor.stderr.read()
        pids = json.loads(line)
        import psutil
        identities = [(pid, psutil.Process(pid).create_time()) for pid in pids]
        supervisor.kill()  # abrupt failure, no Python cleanup/finalizers
        supervisor.wait(timeout=3)
        for pid, created in identities:
            eventually(lambda pid=pid, created=created: not identity_alive(pid, created))
    finally:
        if supervisor.poll() is None:
            supervisor.kill()
            supervisor.wait(timeout=3)


def test_concurrent_duplicate_start_creates_only_one_process(setup_runtime, tmp_path):
    _, _, runner, request = setup_runtime
    allocation = runner.request_allocation(request)
    payload = start_payload(allocation, tmp_path, [sys.executable, "-c", "import time;print('one',flush=True);time.sleep(.4)"])
    created = []
    original = runner._process_factory
    def factory(*args, **kwargs):
        process = original(*args, **kwargs)
        created.append(process.pid)
        return process
    runner._process_factory = factory
    with concurrent.futures.ThreadPoolExecutor(4) as executor:
        results = list(executor.map(lambda _: runner.start(payload), range(4)))
    assert len(created) == 1
    eventually(lambda: runner.status("run-cpu")["state"] == "completed")


def test_experiment_environment_drops_service_credentials(monkeypatch):
    monkeypatch.setenv("DEEPSEEK_API_KEY", "must-not-leak")
    monkeypatch.setenv("AUTORESEARCH_SESSION_TOKEN", "must-not-leak-either")
    env = sanitized_environment({"EXPERIMENT_MODE": "test"})
    assert "DEEPSEEK_API_KEY" not in env
    assert "AUTORESEARCH_SESSION_TOKEN" not in env
    assert env["CUDA_VISIBLE_DEVICES"] == ""


def test_light_cpu_aggregates_existing_evidence_and_replays_idempotently(setup_runtime,tmp_path):
    store,_,runner,request=setup_runtime
    source=tmp_path/"observed.json"
    source.write_text(json.dumps({"values":[2,4,6]}),encoding="utf-8")
    artifact=store.register_artifact(request["research_id"],source,"application/json")
    eid=artifact["artifact_id"]
    payload={"research_id":request["research_id"],"agent_id":"line-01/verify","role":"verify",
        "execution_id":"light-mean","parent_execution_id":"verify-attempt","control_generation":1,"evidence_ids":[eid],
        "code":f"import statistics\nresult = {{'mean': statistics.mean(evidence[{eid!r}]['data']['values'])}}\nprint(result)"}
    result=runner.light_cpu.execute(payload)
    assert result["state"]=="completed",result
    assert result["result"]=={"mean":4}
    assert result["artifact"]["sha256"]
    assert result["process_tree_exited"]
    assert runner.light_cpu.execute(payload)["execution_id"]==result["execution_id"]
    assert runner.service.light_jobs()[0]["state"]=="completed"


def test_light_cpu_requires_actual_role_and_verify_evidence(setup_runtime):
    _,_,runner,request=setup_runtime
    base={"research_id":request["research_id"],"agent_id":"line-01/verify","role":"verify","code":"result=1","control_generation":1}
    with pytest.raises(ValueError):runner.light_cpu.execute(base)
    with pytest.raises(ValueError):runner.light_cpu.execute({**base,"role":"boss"})
    with pytest.raises(ValueError):runner.light_cpu.execute({**base,"role":"exploration"})


def test_light_cpu_supervised_csv_analysis_supports_safe_data_methods(setup_runtime, tmp_path):
    store, _, runner, request = setup_runtime
    source = tmp_path / "sample.csv"
    source.write_text("id,text,target\n1,Hello,1\n2,hello,0\n3,World,1\n", encoding="utf-8")
    artifact = store.register_artifact(request["research_id"], source, "text/csv")
    result = runner.light_cpu.execute({"research_id": request["research_id"], "agent_id": "line-01/verify", "role": "verify",
        "execution_id": "light-csv", "parent_execution_id": "verify-attempt", "control_generation": 1,
        "evidence_ids": [artifact["id"]], "code": f"""rows = evidence[{artifact['id']!r}]['data']
counts = {{}}
for row in rows:
    text = row['text'].strip().lower()
    counts[text] = counts.get(text, 0) + 1
result = {{'counts': counts, 'positive': sum(int(row['target']) for row in rows)}}
"""})
    assert result["state"] == "completed", result
    assert result["result"] == {"counts": {"hello": 2, "world": 1}, "positive": 2}
    assert result["process_tree_exited"]


def test_light_cpu_nonfinite_result_is_explicit_and_json_serializable(setup_runtime):
    _,_,runner,request=setup_runtime
    result=runner.run_light_cpu({"research_id":request["research_id"],"agent_id":"line-01/exploration/member-01",
        "role":"exploration","code":"import math\nresult={'loss':math.nan,'limit':math.inf}","control_generation":1})
    assert result["state"]=="completed"
    assert result["result_numeric_status"]=="non_finite"
    assert result["result"]["loss"]["value"]=="NaN"
    json.dumps(result,allow_nan=False)


def test_parent_cancellation_stops_active_light_cpu_without_stopping_project(setup_runtime):
    _,control,runner,request=setup_runtime
    payload={"research_id":request["research_id"],"agent_id":"line-01/exploration/member-01","role":"exploration",
        "execution_id":"light-infinite","parent_execution_id":"agent-attempt","code":"i=0\nwhile True:\n i+=1","control_generation":1}
    with concurrent.futures.ThreadPoolExecutor() as pool:
        pending=pool.submit(runner.light_cpu.execute,payload)
        eventually(lambda:bool(runner.light_cpu.processes))
        runner.light_cpu.cancel_parent("agent-attempt")
        result=pending.result(timeout=4)
    assert result["state"]=="cancelled",result
    assert result["process_tree_exited"]
    assert control.allows_progress(request["research_id"])
    assert not runner.light_cpu.processes


def test_owner_death_stops_light_cpu_by_process_creation_identity(setup_runtime):
    _,control,runner,request=setup_runtime
    import psutil
    owner=subprocess.Popen([sys.executable,"-c","import time;time.sleep(60)"],
        creationflags=0x08000000 if os.name=="nt" else 0)
    created=psutil.Process(owner.pid).create_time()
    payload={"research_id":request["research_id"],"agent_id":"line-01/exploration/member-01","role":"exploration",
        "execution_id":"light-owner-lifetime","parent_execution_id":"owner-attempt","parent_pid":owner.pid,
        "parent_create_time":created,"code":"i=0\nwhile True:\n i+=1","control_generation":1}
    try:
        with concurrent.futures.ThreadPoolExecutor() as pool:
            pending=pool.submit(runner.light_cpu.execute,payload)
            eventually(lambda:bool(runner.light_cpu.processes))
            owner.kill();owner.wait(timeout=3)
            result=pending.result(timeout=5)
        assert result["state"]=="cancelled",result
        assert result["process_tree_exited"]
        assert control.allows_progress(request["research_id"])
        assert not runner.light_cpu.processes
        with pytest.raises(Exception):runner.light_cpu.execute({**payload,"execution_id":"late-after-owner-death"})
    finally:
        if owner.poll() is None:owner.kill();owner.wait(timeout=3)


def test_light_cpu_runs_normal_python_without_idle_core(setup_runtime):
    _, _, runner, request = setup_runtime
    runner.service.capacity['cpu_cores'] = 0
    result = runner.light_cpu.execute({
        'research_id': request['research_id'],
        'agent_id': 'line-01/exploration/member-01', 'role': 'exploration',
        'code': """import os
import sys
import subprocess
import numpy as np
import pandas as pd
class Analysis:
    def run(self):
        with open('scratch.txt', 'w') as stream:
            stream.write('local')
        child = subprocess.check_output([sys.executable, '-c', 'print(7)'], text=True)
        frame = pd.DataFrame({'value': np.arange(3)})
        return {'array': np.arange(3), 'scalar': np.int64(3),
                'frame': frame, 'mean': frame['value'].mean(),
                'child': int(child), 'cwd': os.getcwd()}
result = Analysis().run()
""", 'control_generation': 1})
    assert result['state'] == 'completed', result
    assert result['result']['array'] == [0, 1, 2]
    assert result['result']['scalar'] == 3
    assert result['result']['frame'] == [{'value': 0}, {'value': 1}, {'value': 2}]
    assert result['result']['mean'] == 1.0
    assert result['result']['child'] == 7
    assert result['process_tree_exited']


def test_light_cpu_reads_supplied_data_and_writes_workspace(setup_runtime, tmp_path):
    store, _, runner, request = setup_runtime
    inputs = tmp_path / 'user-data'
    inputs.mkdir()
    source = inputs / 'values.csv'
    original = 'value\n2\n4\n'
    source.write_text(original, encoding='utf-8')
    store.update_project(request['research_id'], workspace_path=str(inputs))
    result = runner.light_cpu.execute({
        'research_id': request['research_id'],
        'agent_id': 'line-01/exploration/member-01', 'role': 'exploration',
        'control_generation': 1,
        'code': """from pathlib import Path
from dataclasses import dataclass
import csv, importlib
assert __name__ == '__main__'
assert Path(__file__).parent == Path(workspace_dir)
rows = list(csv.DictReader(open(Path(data_dir) / 'values.csv')))
@dataclass
class Analysis:
    total: int
value = Analysis(sum(int(row['value']) for row in rows))
Path('helper.py').write_text('def transform(value): return value * 2')
helper = importlib.import_module('helper')
Path(workspace_dir, 'output.txt').write_text(str(helper.transform(value.total)))
result = {'sum': value.total, 'doubled': int(Path('output.txt').read_text()),
          'data_dir': data_dir, 'workspace_dir': workspace_dir}
"""})
    assert result['state'] == 'completed', result
    assert result['result']['sum'] == 6
    assert result['result']['doubled'] == 12
    assert result['result']['data_dir'] == str(inputs.resolve())
    assert result['result']['workspace_dir'] == result['workspace_dir']
    assert source.read_text(encoding='utf-8') == original
    assert not (inputs / 'output.txt').exists()
    assert result['process_tree_exited']
