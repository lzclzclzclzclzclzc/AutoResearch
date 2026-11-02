"""Lightweight exit proofs survive Runner failure and lost completion ACKs."""
import ctypes
import json
import os
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

import psutil
import pytest
import uvicorn

from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor, ControlUnavailable, create_app as control_app, timestamp
from backend.runtime.light_cpu import LightCpuService
from backend.runtime.processes import identity_alive, terminate_identity
from backend.runtime.runner import ResourceService, RunnerSupervisor


class CompletionTransport:
    def __init__(self, control):
        self.control = control
        self.unavailable = False
        self.apply_before_failure = False
        self.attempts = 0

    def __getattr__(self, name):
        return getattr(self.control, name)

    def register_supervisor(self, *args):
        # Direct-object tests do not expose a Runner HTTP stop endpoint.
        return {'registered': True, 'control_epoch': self.control.epoch}

    def complete_execution(self, execution_id):
        self.attempts += 1
        if self.unavailable:
            if self.apply_before_failure:
                self.control.complete_execution(execution_id)
            raise ControlUnavailable('isolated completion transport outage')
        return self.control.complete_execution(execution_id)


def eventually(predicate, timeout=8):
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        if predicate():
            return
        time.sleep(.03)
    assert predicate()


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    root = tmp_path.parent / ('lc-' + uuid.uuid4().hex[:10])
    store = Store(root)
    project = store.start_project(store.create_project({
        'topic': 'Light CPU recovery', 'mode': 'demo', 'client_request_id': 'light-recovery',
        'max_runtime_seconds': 120, 'team': {'n': 1, 'm1': 1, 'm2': 1, 'boss_members': 1},
    })['id'])
    control = ControlSupervisor(root / 'control')
    control.register_project(project['id'], project['deadline_at'])
    remote = CompletionTransport(control)
    capacity = {'cpu_cores': 4, 'memory_total_bytes': 4 * 1024**3,
        'memory_available_bytes': 4 * 1024**3, 'gpus': []}
    monkeypatch.setattr('backend.runtime.runner.detect_resources', lambda *args: capacity)
    store.put('agent', project['id'] + ':line-01/implementation', {'role': 'implementation'}, project['id'])
    runners = []
    def runner():
        instance = RunnerSupervisor(root, remote, capacity)
        runners.append(instance)
        return instance
    try:
        yield root, store, project, control, remote, capacity, runner
    finally:
        remote.unavailable = False
        for instance in runners:
            if not instance._lock_file.closed:
                instance.close()
        control.close()


def request(project, execution_id='light-history'):
    return {'research_id': project['id'], 'agent_id': 'line-01/implementation',
        'role': 'implementation', 'line_id': 'line-01', 'execution_id': execution_id,
        'control_generation': 1, 'code': "from pathlib import Path\nmarker=Path('execution-count.txt')\n"
            "count=int(marker.read_text())+1 if marker.exists() else 1\nmarker.write_text(str(count))\nresult=count"}


def registered_job(project, control, execution_id, pid=None, created=None):
    return {'execution_id': execution_id, 'research_id': project['id'],
        'agent_id': 'line-01/implementation', 'role': 'implementation', 'line_id': 'line-01',
        'content_hash': 'restored-fixture', 'state': 'starting', 'light_cpu': True,
        'cpu_cores': 1, 'memory_bytes': 512 * 1024**2, 'gpu_uuids': [],
        'control_generation': 1, 'control_epoch': control.epoch,
        'deadline_at': timestamp(project['deadline_at']), 'runner_instance_id': 'previous-runner',
        'runner_generation': 'previous-generation', 'pid': pid, 'process_create_time': created,
        'process_creation_started': pid is not None}


@pytest.mark.parametrize('ack_applied', [False, True])
def test_terminal_history_retries_lost_completion_across_runner_restarts(runtime, ack_applied):
    _, _, project, control, remote, _, make_runner = runtime
    remote.unavailable = True
    remote.apply_before_failure = ack_applied
    first = make_runner()
    payload = request(project)
    finished = first.run_light_cpu(payload)
    assert finished['state'] == 'completed' and finished['result'] == 1
    assert finished['process_tree_exited'] and finished['reconciliation_pending']
    assert not identity_alive(finished['pid'], finished['process_create_time'])
    artifact_id = finished['artifact']['id']
    first.close()
    # Older terminal rows did not persist reconciliation_pending. They still
    # require completion replay after a crash between release and Control ACK.
    legacy = {key: value for key, value in first.service.light_jobs()[0].items() if key != 'reconciliation_pending'}
    first.service.update_light(legacy, terminal_proof=True)
    second = make_runner()
    restored = second.light_cpu.jobs[payload['execution_id']]
    assert restored['state'] == 'completed' and restored['reconciliation_pending']
    assert restored['artifact']['id'] == artifact_id
    attempts = remote.attempts
    remote.unavailable = False
    eventually(lambda: not second.light_cpu.jobs[payload['execution_id']]['reconciliation_pending'])
    assert remote.attempts > attempts
    assert control.status(project['id'])['unfinished'] == []
    assert second.service.light_jobs()[0]['reconciliation_pending'] is False
    replay = second.run_light_cpu(payload)
    assert replay['execution_id'] == finished['execution_id'] and replay['result'] == 1
    assert replay['artifact']['id'] == artifact_id
    assert (Path(replay['workspace_dir']) / 'execution-count.txt').read_text() == '1'
    control.terminate(project['id'])
    status = control.status(project['id'])
    control.mark_db_synced(project['id'], status['stop_id'], resources_reconciled=True)
    eventually(lambda: control.status(project['id'])['state'] == 'terminated')


def test_restart_retries_database_failure_before_clearing_control(runtime, monkeypatch):
    _, _, project, control, remote, _, make_runner = runtime
    remote.unavailable = True
    first = make_runner()
    payload = request(project)
    assert first.run_light_cpu(payload)['state'] == 'completed'
    first.close()
    failed = threading.Event()
    def database_unavailable(self, job, terminal_proof=False):
        failed.set()
        raise sqlite3.OperationalError('isolated temporary database failure')
    remote.unavailable = False
    attempts = remote.attempts
    with monkeypatch.context() as patch:
        patch.setattr(ResourceService, 'update_light', database_unavailable)
        second = make_runner()
        eventually(failed.is_set)
        assert second.light_cpu.jobs[payload['execution_id']]['reconciliation_pending']
        assert remote.attempts == attempts
        assert [job['execution_id'] for job in control.status(project['id'])['unfinished']] == [payload['execution_id']]
    eventually(lambda: not second.light_cpu.jobs[payload['execution_id']]['reconciliation_pending'])
    assert control.status(project['id'])['unfinished'] == []
    assert second.service.light_jobs()[0]['reconciliation_pending'] is False


@pytest.mark.parametrize('unconfirmed', ['missing_identity', 'cleanup_refused'])
def test_restored_process_is_not_released_without_verified_exit(runtime, monkeypatch, unconfirmed):
    _, store, project, control, remote, capacity, make_runner = runtime
    process = subprocess.Popen([sys.executable, '-c', 'import time;time.sleep(45)'],
        creationflags=0x08000000 if os.name == 'nt' else 0)
    created = psutil.Process(process.pid).create_time()
    job = registered_job(project, control, 'unconfirmed-child', process.pid,
        None if unconfirmed == 'missing_identity' else created)
    resources = ResourceService(store.path, remote, capacity)
    try:
        resources.reserve_light(job)
        control.register_process(project['id'], job['execution_id'], pid=process.pid, create_time=created, kind='light_cpu')
        if unconfirmed == 'cleanup_refused':
            monkeypatch.setattr('backend.runtime.light_cpu.terminate_identity', lambda *args: False)
        runner = make_runner()
        restored = runner.light_cpu.jobs[job['execution_id']]
        assert restored['state'] == 'reconciling' and restored['reconciliation_pending']
        assert not restored.get('process_tree_exited')
        assert resources.light_jobs()[0]['state'] == 'reconciling'
        assert process.poll() is None and control.status(project['id'])['unfinished']
        if unconfirmed == 'cleanup_refused':
            monkeypatch.setattr('backend.runtime.light_cpu.terminate_identity', terminate_identity)
            eventually(lambda: not restored['reconciliation_pending'])
            assert not identity_alive(process.pid, created)
            assert restored['state'] == 'interrupted' and restored['process_tree_exited']
            assert control.status(project['id'])['unfinished'] == []
    finally:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=5)


def test_pidless_registration_waits_for_previous_owner_fence(runtime):
    root, store, project, control, remote, capacity, make_runner = runtime
    resources = ResourceService(store.path, remote, capacity)
    job = registered_job(project, control, 'before-create')
    resources.reserve_light(job)
    control.register_process(project['id'], job['execution_id'], kind='light_cpu')
    unfenced = LightCpuService(root, remote, resources, 'standalone', 'standalone-generation')
    try:
        assert unfenced.jobs[job['execution_id']]['reconciliation_pending']
        assert not unfenced.jobs[job['execution_id']].get('process_tree_exited')
        assert control.status(project['id'])['unfinished']
    finally:
        unfenced.close()
    runner = make_runner()
    eventually(lambda: not runner.light_cpu.jobs[job['execution_id']]['reconciliation_pending'])
    restored = resources.light_jobs()[0]
    assert restored['state'] == 'interrupted' and restored['process_tree_exited']
    assert restored['recovery_proof_reason'] == 'previous_runner_exited_before_creation'
    assert control.status(project['id'])['unfinished'] == []


def test_actual_runner_exit_after_registration_clears_pidless_control_execution(runtime, monkeypatch):
    root, store, project, control, _, capacity, make_runner = runtime
    token = 'isolated-light-recovery-fixture'
    monkeypatch.setenv('AUTORESEARCH_SESSION_TOKEN', token)
    endpoint = socket.socket()
    endpoint.bind(('127.0.0.1', 0))
    endpoint.listen()
    control_url = f'http://127.0.0.1:{endpoint.getsockname()[1]}'
    server = uvicorn.Server(uvicorn.Config(control_app(supervisor=control), log_level='error'))
    server_thread = threading.Thread(target=server.run, kwargs={'sockets': [endpoint]}, daemon=True)
    server_thread.start()
    helper = """import os,sys
from backend.runtime.control import ControlClient
from backend.runtime.runner import RunnerSupervisor
class CrashAfterRegistration(ControlClient):
    def register_process(self,*args,**kwargs):
        result=super().register_process(*args,**kwargs)
        if kwargs.get('pid') is None:
            os._exit(23)
        return result
runner=RunnerSupervisor(sys.argv[1],CrashAfterRegistration(sys.argv[2]))
runner.run_light_cpu({'research_id':sys.argv[3],'agent_id':'line-01/implementation',
    'role':'implementation','execution_id':'crash-before-create','control_generation':1,'code':'result=42'})
"""
    process = None
    try:
        eventually(lambda: server.started)
        process = subprocess.Popen([sys.executable, '-c', helper, str(root), control_url, project['id']],
            cwd=Path(__file__).resolve().parents[2], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            creationflags=0x08000000 if os.name == 'nt' else 0)
        _, error = process.communicate(timeout=12)
        assert process.returncode == 23, error.decode(errors='replace')
        registered = control.status(project['id'])['unfinished']
        assert len(registered) == 1 and registered[0]['pid'] is None
        before = ResourceService(store.path, control, capacity).light_jobs()[0]
        assert before['state'] == 'starting' and before['process_creation_started'] is False
        runner = make_runner()
        eventually(lambda: not control.status(project['id'])['unfinished'])
        recovered = runner.service.light_jobs()[0]
        assert recovered['state'] == 'interrupted' and recovered['process_tree_exited']
        assert recovered['reconciliation_pending'] is False
    finally:
        if process and process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        server.should_exit = True
        server_thread.join(timeout=5)
        endpoint.close()


@pytest.mark.skipif(os.name != 'nt', reason='Windows atomic JOB_LIST creation and owner lifetime')
def test_runner_crash_after_createprocess_before_pid_assignment_keeps_child_contained(runtime, monkeypatch):
    from ctypes import wintypes
    root, store, project, control, _, capacity, make_runner = runtime
    monkeypatch.setenv('AUTORESEARCH_SESSION_TOKEN', 'isolated-create-window-fixture')
    marker = root / 'created-child.json'
    endpoint = socket.socket()
    endpoint.bind(('127.0.0.1', 0))
    endpoint.listen()
    control_url = f'http://127.0.0.1:{endpoint.getsockname()[1]}'
    server = uvicorn.Server(uvicorn.Config(control_app(supervisor=control), log_level='error'))
    server_thread = threading.Thread(target=server.run, kwargs={'sockets': [endpoint]}, daemon=True)
    server_thread.start()
    helper = """import ctypes,json,os,sys,time,psutil
from pathlib import Path
from ctypes import wintypes
from backend.runtime import light_cpu,processes
from backend.runtime.control import ControlClient
from backend.runtime.runner import RunnerSupervisor
class PauseInsideCreateProcess(processes.ManagedProcess):
    def create_suspended(self):
        original=processes.kernel.CreateProcessW
        def observe_created(*args):
            result=original(*args)
            if not result:
                return result
            info=ctypes.cast(args[-1],ctypes.POINTER(processes.PROCESS_INFORMATION)).contents
            is_in_job=processes.kernel.IsProcessInJob
            is_in_job.argtypes=[wintypes.HANDLE,wintypes.HANDLE,ctypes.POINTER(wintypes.BOOL)]
            is_in_job.restype=wintypes.BOOL
            member=wintypes.BOOL()
            assert is_in_job(info.hProcess,self.job,ctypes.byref(member))
            snapshot={'owner_pid':os.getpid(),'owner_created':psutil.Process().create_time(),
                'child_pid':int(info.dwProcessId),'child_created':psutil.Process(info.dwProcessId).create_time(),
                'managed_pid':self.pid,'in_private_job':bool(member.value)}
            temporary=Path(sys.argv[4]+'.tmp')
            temporary.write_text(json.dumps(snapshot))
            temporary.replace(sys.argv[4])
            time.sleep(60)
            return result
        processes.kernel.CreateProcessW=observe_created
        try:
            super().create_suspended()
        finally:
            processes.kernel.CreateProcessW=original
light_cpu.ManagedProcess=PauseInsideCreateProcess
runner=RunnerSupervisor(sys.argv[1],ControlClient(sys.argv[2]))
runner.run_light_cpu({'research_id':sys.argv[3],'agent_id':'line-01/implementation',
    'role':'implementation','execution_id':'created-without-pid','control_generation':1,'code':'result=42'})
"""
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel.OpenProcess.restype = wintypes.HANDLE
    kernel.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    kernel.WaitForSingleObject.restype = wintypes.DWORD
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel.CloseHandle.restype = wintypes.BOOL
    owner = None
    child_handle = None
    observed = {}
    try:
        eventually(lambda: server.started)
        owner = subprocess.Popen([sys.executable, '-c', helper, str(root), control_url, project['id'], str(marker)],
            cwd=Path(__file__).resolve().parents[2], stdout=subprocess.PIPE, stderr=subprocess.PIPE, creationflags=0x08000000)
        eventually(lambda: marker.exists() or owner.poll() is not None, timeout=12)
        if not marker.exists():
            _, error = owner.communicate(timeout=5)
            pytest.fail(error.decode(errors='replace'))
        observed = json.loads(marker.read_text())
        assert observed['managed_pid'] == 0 and observed['in_private_job']
        assert identity_alive(observed['child_pid'], observed['child_created'])
        before = ResourceService(store.path, control, capacity).light_jobs()[0]
        assert before['pid'] is None and before['process_creation_started'] is True
        assert control.status(project['id'])['unfinished'][0]['pid'] is None
        child_handle = kernel.OpenProcess(0x00100000, False, observed['child_pid'])  # SYNCHRONIZE
        assert child_handle
        assert kernel.WaitForSingleObject(child_handle, 0) == 258  # WAIT_TIMEOUT: still alive
        assert identity_alive(observed['owner_pid'], observed['owner_created'])
        # Kill only the exact Runner owner. Keep the child handle so an OS
        # signal proves exit, independently of PID enumeration or reuse.
        psutil.Process(observed['owner_pid']).kill()
        owner.communicate(timeout=5)
        assert kernel.WaitForSingleObject(child_handle, 5000) == 0  # WAIT_OBJECT_0
        runner = make_runner()
        eventually(lambda: not control.status(project['id'])['unfinished'])
        recovered = runner.service.light_jobs()[0]
        assert recovered['state'] == 'interrupted' and recovered['process_tree_exited']
        assert recovered['recovery_proof_reason'] == 'previous_runner_exited_and_job_closed'
        assert recovered['reconciliation_pending'] is False
    finally:
        if observed and identity_alive(observed['owner_pid'], observed['owner_created']):
            psutil.Process(observed['owner_pid']).kill()
        if owner and owner.poll() is None:
            try:
                for descendant in psutil.Process(owner.pid).children(recursive=True):
                    try:descendant.kill()
                    except psutil.NoSuchProcess:pass
            except psutil.NoSuchProcess:pass
            owner.kill()
            owner.wait(timeout=5)
        if observed and identity_alive(observed['child_pid'], observed['child_created']):
            terminate_identity(observed['child_pid'], observed['child_created'])
        if child_handle:
            kernel.CloseHandle(child_handle)
        server.should_exit = True
        server_thread.join(timeout=5)
        endpoint.close()
