"""A transport outage fences old executions without terminating their project."""
import concurrent.futures
import sqlite3
import sys
import threading
import time

import pytest

from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor, ControlUnavailable
from backend.runtime.processes import identity_alive
from backend.runtime.runner import ResourceConflict, RunnerSupervisor
from packages.contracts.research import ExperimentSpec, digest


class IntermittentControl:
    def __init__(self, target):
        self.target = target
        self.offline = False

    def __getattr__(self, name):
        method = getattr(self.target, name)
        if not callable(method):
            return method
        def call(*args, **kwargs):
            if self.offline:
                raise ControlUnavailable('isolated transport outage')
            if name == 'register_supervisor':
                # These tests use direct objects, never a real HTTP stop endpoint.
                return {'registered': True, 'control_epoch': self.target.epoch}
            return method(*args, **kwargs)
        return call


def eventually(predicate, timeout=8):
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        if predicate():
            return
        time.sleep(.03)
    assert predicate()


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    store = Store(tmp_path)
    project = store.start_project(store.create_project({
        'topic': 'Control transport recovery', 'mode': 'demo', 'client_request_id': 'transport',
        'max_runtime_seconds': 90,
        'team': {'n': 1, 'm1': 1, 'm2': 1, 'boss_members': 1, 'k': 2, 'experiment_concurrency': 2},
    })['id'])
    control = ControlSupervisor(tmp_path / 'control')
    control.register_project(project['id'], project['deadline_at'])
    remote = IntermittentControl(control)
    capacity = {'cpu_cores': 4, 'memory_total_bytes': 4 * 1024**3,
        'memory_available_bytes': 4 * 1024**3, 'gpus': [], 'process_backend': 'test_managed_process'}
    monkeypatch.setattr('backend.runtime.runner.detect_resources', lambda *args: capacity)
    plan = ExperimentSpec(spec_id='spec', version=1, question='transport', fixed={},
        metrics=['exit'], deadline_at=project['deadline_at'], discussion_protocol=2)
    store.put('spec', 'spec', plan.model_dump(), project['id'])
    request = {'research_id': project['id'], 'implementation_agent_id': 'line-01/implementation',
        'line_id': 'line-01', 'spec_id': 'spec', 'spec_version': 1, 'spec_hash': digest(plan.model_dump()),
        'cpu_cores': 1, 'memory_bytes': 256 * 1024**2, 'gpu_count': 0,
        'control_generation': 1, 'resource_request_id': 'original-request'}
    runner = RunnerSupervisor(tmp_path, remote, capacity=capacity)
    try:
        yield store, project, control, remote, runner, request
    finally:
        remote.offline = False
        runner.close()
        control.close()


def payload(allocation, tmp_path, run_id='original-run', code='import time; time.sleep(45)'):
    return {**allocation, 'experiment_run_id': run_id, 'start_operation_id': run_id + ':start',
        'code_version': digest(code), 'cwd': str(tmp_path), 'command': [sys.executable, '-c', code]}


def interrupt_active(remote, runner, project, run):
    remote.offline = True
    eventually(lambda: project['id'] in runner._control_recoveries)
    eventually(lambda: not identity_alive(run['pid'], run['process_create_time']))
    assert not runner._control_ok
    assert project['id'] not in runner._stopped_projects


def test_same_epoch_recovery_reuses_queue_and_preserves_cancelled_run(runtime, tmp_path):
    _, project, control, remote, runner, request = runtime
    original = payload(runner.request_allocation(request), tmp_path)
    run = runner.start(original)
    waiting = {**request, 'resource_request_id': 'waiting-request', 'cpu_cores': 4}
    queued = runner.request_allocation(waiting)
    assert queued['state'] == 'queued'
    epoch = control.epoch
    interrupt_active(remote, runner, project, run)
    remote.offline = False
    eventually(lambda: project['id'] not in runner._control_recoveries)
    assert control.epoch == epoch and control.allows_progress(project['id'], 1)
    recovered = runner.start(original)
    assert recovered['state'] == 'cancelled'
    assert recovered['terminal_proof']['process_tree_exited']
    assert recovered['terminal_proof']['start_operation_terminal']
    assert recovered['pid'] == run['pid']
    with pytest.raises(ResourceConflict):
        runner.start({**original, 'start_operation_id': 'replacement-for-old-run'})
    grant = runner.request_allocation(waiting)
    assert grant['state'] == 'granted' and grant['allocation_id'] == queued['allocation_id']
    new_run = runner.start(payload(grant, tmp_path, 'new-run', "print('after recovery')"))
    eventually(lambda: runner.status('new-run')['state'] == 'completed')
    assert 'after recovery' in runner.collect('new-run')['text']
    assert new_run['experiment_run_id'] != recovered['experiment_run_id']
    assert len(runner.status()['runs']) == 2


def test_reconnected_control_waits_for_durable_exit_proof_before_new_admission(runtime, tmp_path, monkeypatch):
    _, project, _, remote, runner, request = runtime
    run = runner.start(payload(runner.request_allocation(request), tmp_path))
    original_release = runner.service.release
    unavailable = threading.Event()
    unavailable.set()
    def fail_release(*args, **kwargs):
        if unavailable.is_set():
            raise sqlite3.OperationalError('database is locked')
        return original_release(*args, **kwargs)
    monkeypatch.setattr(runner.service, 'release', fail_release)
    try:
        interrupt_active(remote, runner, project, run)
        remote.offline = False
        eventually(lambda: runner._control_ok)
        assert runner.status(run['experiment_run_id'])['reconciliation_pending']
        # Capacity and project concurrency both allow another run, but proof does not.
        with pytest.raises(ControlUnavailable, match='reconciliation'):
            runner.request_allocation({**request, 'resource_request_id': 'too-early'})
        assert len(runner.resources()['allocations']) == 1
    finally:
        unavailable.clear()
    eventually(lambda: project['id'] not in runner._control_recoveries)
    assert runner.request_allocation({**request, 'resource_request_id': 'after-proof'})['state'] == 'granted'


@pytest.mark.parametrize('change', ['user_stop', 'new_epoch', 'new_generation'])
def test_transport_recovery_cannot_override_explicit_stop_or_control_identity(runtime, tmp_path, change):
    _, project, control, remote, runner, request = runtime
    run = runner.start(payload(runner.request_allocation(request), tmp_path))
    interrupt_active(remote, runner, project, run)
    if change == 'user_stop':
        control.terminate(project['id'], 'user_requested')
    elif change == 'new_epoch':
        control.epoch = 'replacement-control-epoch'
    else:
        with control._lock:
            control.projects[project['id']]['control_generation'] += 1
    remote.offline = False
    eventually(lambda: project['id'] in runner._stopped_projects)
    with pytest.raises((ControlUnavailable, ResourceConflict)):
        runner.request_allocation({**request, 'resource_request_id': 'forbidden-replacement'})
    assert len(runner.status()['runs']) == 1


def test_control_failure_during_start_still_finishes_the_admitted_operation(runtime, tmp_path, monkeypatch):
    _, project, _, remote, runner, request = runtime
    grant = runner.request_allocation(request)
    original_register = remote.register_process
    def lose_registration(*args, **kwargs):
        remote.offline = True
        return original_register(*args, **kwargs)
    monkeypatch.setattr(remote, 'register_process', lose_registration)
    with pytest.raises(ControlUnavailable):
        runner.start(payload(grant, tmp_path))
    assert runner.status('original-run')['process_tree_exited']
    remote.offline = False
    monkeypatch.setattr(remote, 'register_process', original_register)
    eventually(lambda: not runner.status('original-run').get('reconciliation_pending'))
    eventually(lambda: project['id'] not in runner._control_recoveries)
    assert runner.status('original-run')['terminal_proof']['start_operation_terminal']
    assert runner.resources()['allocations'][0]['state'] == 'released'


def test_transport_recovery_waits_for_control_to_acknowledge_execution_exit(runtime, tmp_path, monkeypatch):
    _, project, _, remote, runner, request = runtime
    entered = threading.Event()
    acknowledge = threading.Event()
    original_complete = remote.complete_execution
    def delayed_complete(execution_id):
        if execution_id == 'original-run':
            entered.set()
            assert acknowledge.wait(8)
        return original_complete(execution_id)
    monkeypatch.setattr(remote, 'complete_execution', delayed_complete)
    run = runner.start(payload(runner.request_allocation(request), tmp_path))
    try:
        interrupt_active(remote, runner, project, run)
        eventually(entered.is_set)
        remote.offline = False
        assert runner.resources()['allocations'][0]['state'] == 'released'
        with pytest.raises(ControlUnavailable, match='reconciliation'):
            runner.request_allocation({**request, 'resource_request_id': 'before-control-ack'})
        assert runner._control_ok
    finally:
        acknowledge.set()
    eventually(lambda: project['id'] not in runner._control_recoveries)


def test_lightweight_work_uses_recoverable_gate_without_a_permanent_stop(runtime):
    store, project, _, remote, runner, _ = runtime
    agent = 'line-01/implementation'
    store.put('agent', project['id'] + ':' + agent, {'role': 'implementation'}, project['id'])
    request = {'research_id': project['id'], 'agent_id': agent, 'role': 'implementation',
        'line_id': 'line-01', 'control_generation': 1, 'execution_id': 'old-light',
        'code': 'import time; time.sleep(45); result=1'}
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        pending = executor.submit(runner.run_light_cpu, request)
        eventually(lambda: runner.light_cpu.jobs.get('old-light', {}).get('state') == 'running')
        remote.offline = True
        eventually(lambda: project['id'] in runner._control_recoveries)
        stopped = pending.result(timeout=8)
        assert stopped['state'] == 'cancelled' and stopped['process_tree_exited']
        assert project['id'] not in runner.light_cpu.stopped
        remote.offline = False
        eventually(lambda: project['id'] not in runner._control_recoveries)
    result = runner.run_light_cpu({**request, 'execution_id': 'new-light', 'code': 'result=42'})
    assert result['state'] == 'completed' and result['result'] == 42


@pytest.mark.parametrize('boundary', ['resource_release', 'control_completion'])
def test_lightweight_cleanup_must_finish_before_recovery_admits_new_work(runtime, monkeypatch, boundary):
    store, project, _, remote, runner, allocation_request = runtime
    agent = 'line-01/implementation'
    store.put('agent', project['id'] + ':' + agent, {'role': 'implementation'}, project['id'])
    request = {'research_id': project['id'], 'agent_id': agent, 'role': 'implementation',
        'control_generation': 1, 'execution_id': 'cleanup-light', 'code': 'import time; time.sleep(45)'}
    entered = threading.Event()
    acknowledge = threading.Event()
    original_update = runner.service.update_light
    original_complete = remote.complete_execution
    def delayed_update(job, terminal_proof=False):
        if boundary == 'resource_release' and terminal_proof:
            entered.set()
            assert acknowledge.wait(8)
        return original_update(job, terminal_proof=terminal_proof)
    def delayed_complete(execution_id):
        if boundary == 'control_completion' and execution_id == 'cleanup-light':
            entered.set()
            assert acknowledge.wait(8)
        return original_complete(execution_id)
    monkeypatch.setattr(runner.service, 'update_light', delayed_update)
    monkeypatch.setattr(remote, 'complete_execution', delayed_complete)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        pending = executor.submit(runner.run_light_cpu, request)
        try:
            eventually(lambda: runner.light_cpu.jobs.get('cleanup-light', {}).get('state') == 'running')
            remote.offline = True
            eventually(lambda: project['id'] in runner._control_recoveries)
            eventually(entered.is_set)
            remote.offline = False
            with pytest.raises(ControlUnavailable, match='reconciliation'):
                runner.request_allocation(allocation_request)
        finally:
            acknowledge.set()
        assert pending.result(timeout=8)['state'] == 'cancelled'
    eventually(lambda: project['id'] not in runner._control_recoveries)
    assert runner.request_allocation(allocation_request)['state'] == 'granted'


def test_lightweight_reservation_failure_cannot_leave_a_recovery_fence(runtime, monkeypatch):
    store, project, _, remote, runner, allocation_request = runtime
    agent = 'line-01/implementation'
    store.put('agent', project['id'] + ':' + agent, {'role': 'implementation'}, project['id'])
    request = {'research_id': project['id'], 'agent_id': agent, 'role': 'implementation',
        'control_generation': 1, 'execution_id': 'unadmitted-light', 'code': 'result=1'}
    entered = threading.Event()
    reject = threading.Event()
    def rejected_reservation(job):
        entered.set()
        assert reject.wait(8)
        raise ResourceConflict('isolated admission failure')
    monkeypatch.setattr(runner.service, 'reserve_light', rejected_reservation)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        pending = executor.submit(runner.run_light_cpu, request)
        try:
            eventually(entered.is_set)
            remote.offline = True
            with pytest.raises(ControlUnavailable):
                runner._connect_control()
            assert project['id'] in runner._control_recoveries
        finally:
            reject.set()
        with pytest.raises(ResourceConflict, match='admission failure'):
            pending.result(timeout=8)
    assert 'unadmitted-light' not in runner.light_cpu.jobs
    assert runner.service.light_jobs() == []
    remote.offline = False
    assert runner.request_allocation(allocation_request)['state'] == 'granted'
    assert project['id'] not in runner._control_recoveries
