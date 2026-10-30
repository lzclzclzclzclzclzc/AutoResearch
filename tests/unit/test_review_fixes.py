import asyncio
import subprocess
import sys
from types import SimpleNamespace

import httpx
import pytest

from backend.adapters.dsh import DshRuntimeAdapter, RuntimeBlocked
from backend.graphs.engine import ResearchEngine
from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor, ControlUnavailable
from backend.runtime.light_cpu import LightCpuService
from backend.runtime.processes import sanitized_environment
from backend.runtime.recovery import retryable_failure
from backend.runtime.runner import ResourceService, ResourceConflict, create_app
from packages.contracts.research import ExperimentSpec, Implementation, digest


def started(store, name):
    return store.start_project(store.create_project({'topic': name, 'client_request_id': name, 'mode': 'demo'})['id'])


def test_unicode_experiment_logs_do_not_crash(tmp_path):
    run = subprocess.run([sys.executable, '-c', 'print(chr(0x1f600))'], env=sanitized_environment(), capture_output=True)
    assert run.returncode == 0
    assert run.stdout.decode('utf-8').strip() == chr(0x1f600)

@pytest.mark.asyncio
async def test_control_transport_error_remains_retryable_over_runner_http():
    def unavailable(*args):
        raise ControlUnavailable('temporary')
    app = create_app(supervisor=SimpleNamespace(request_allocation=unavailable))
    import os
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1',
            headers={'X-AutoResearch-Token': os.environ.get('AUTORESEARCH_SESSION_TOKEN', '')}) as client:
        response = await client.post('/allocations', json={})
    assert response.status_code == 503
    assert response.json()['code'] == 'control_unavailable'
    assert retryable_failure(RuntimeBlocked('confirmation_pending', 'pending'))


@pytest.mark.asyncio
async def test_verify_confirmation_loss_retries_same_report(tmp_path):
    store = Store(tmp_path)
    project = started(store, 'verify')
    rid = project['id']
    store.put('cycle', 'cycle', {'line_id': 'line-01', 'k': 3, 'status': 'verifying', 'completed_verify_rounds': 0}, rid)
    store.put('submission', 'submission', {'cycle_id': 'cycle', 'spec_id': 's', 'spec_version': 1, 'code_version': 'c', 'run_ids': ['r']}, rid)
    control = ControlSupervisor(tmp_path/'control')
    control.register_project(rid, project['deadline_at'])
    engine = ResearchEngine.__new__(ResearchEngine)
    engine.store, engine.control = store, control
    async def guard(*args): return project
    async def turn(*args, **kwargs): return {'content_markdown': 'observed', 'passed': False, 'evidence': [], 'limitations': []}
    engine.guard, engine.turn = guard, turn
    original = control.confirm_operation
    control.confirm_operation = lambda *args: None
    state = {'research_id': rid, 'line_id': 'line-01', 'cycle_id': 'cycle', 'submission_id': 'submission'}
    try:
        with pytest.raises(RuntimeBlocked, match='控制确认'):
            await engine.verify(state)
        control.confirm_operation = original
        assert (await engine.verify(state))['route'] == 'exploration_and_critic'
        assert store.get('cycle', 'cycle')['completed_verify_rounds'] == 1
    finally:
        control.close()

def test_paused_backlog_does_not_hide_other_projects(tmp_path):
    store = Store(tmp_path)
    first, second = sorted([started(store, 'a'), started(store, 'b')], key=lambda p: p['id'])
    store.update_project(first['id'], status='paused')
    for i in range(120):
        store.add_task(first['id'], 'communication', {}, f'paused-{i}')
    assert any(t['research_id'] == second['id'] for t in store.pending_tasks())

def test_light_retry_artifacts_and_restart(tmp_path, monkeypatch):
    store = Store(tmp_path)
    project = started(store, 'light')
    rid = project['id']
    control = ControlSupervisor(tmp_path/'control')
    control.register_project(rid, project['deadline_at'])
    resources = ResourceService(store.path, control, {'cpu_cores': 8, 'memory_total_bytes': 8*1024**3, 'gpus': []})
    service = LightCpuService(tmp_path, control, resources, 'runner', 'generation')
    store.put('agent', rid+':line-01/implementation', {'role': 'implementation'}, rid)
    payload = {'research_id': rid, 'agent_id': 'line-01/implementation', 'role': 'implementation',
        'line_id': 'line-01', 'code': 'result=42', 'execution_id': 'light', 'control_generation': 1}
    try:
        with monkeypatch.context() as patch:
            patch.setattr(resources, 'reserve_light', lambda *args: (_ for _ in ()).throw(ResourceConflict('busy')))
            with pytest.raises(ResourceConflict): service.execute(payload)
        job = service.execute(payload)
        assert job['state'] == 'completed'
        assert job['artifact']['line_id'] == 'line-01'
        saved = resources.light_jobs()[0]
        assert saved['artifact']['id'] == job['artifact']['id']
        service.close()
        service = LightCpuService(tmp_path, control, resources, 'runner2', 'generation2')
        assert service.execute(payload)['artifact']['id'] == job['artifact']['id']
    finally:
        service.close()
        control.close()

@pytest.mark.asyncio
async def test_local_dependencies_and_unchanged_repairs_are_executed(tmp_path):
    store = Store(tmp_path)
    project = started(store, 'implementation')
    spec = ExperimentSpec(spec_id='spec', version=1, question='q', hypothesis='h', fixed={}, baseline='b', data={}, metrics=['m'], design={}, monitoring={}, completion_criteria='exit', deadline_at=project['deadline_at'])
    store.put('spec', 'spec', spec.model_dump(), project['id'])
    engine = ResearchEngine.__new__(ResearchEngine)
    engine.store = store
    async def line_open(*args): pass
    async def turn(*args, **kwargs):
        return {'files': {'main': 'import helper\nprint(helper.value)', 'helper.py': 'value=42'},
                'entrypoint': 'main', 'dependencies': ['helper', 'nonexistent-package>=1']}
    engine.line_open, engine.turn = line_open, turn
    state = {'research_id': project['id'], 'line_id': 'line-01', 'spec_id': 'spec', 'direction_epoch': 1,
             'revision': 1, 'task_id': 'task', 'cycle_id': 'cycle'}
    state.update(await engine.implement(state))
    state['code_attempt'] = 2
    result = await engine.implement(state)
    assert result['implementation_route'] == 'organize'
    implementation = store.get('implementation', result['implementation_id'])
    run = subprocess.run([sys.executable, implementation['entrypoint']], cwd=implementation['cwd'], capture_output=True)
    assert run.returncode == 0 and run.stdout.strip() == b'42'

def test_resource_limits_are_per_project_and_impossible_requests_return(tmp_path, monkeypatch):
    store = Store(tmp_path)
    capacity = {'cpu_cores': 8, 'memory_total_bytes': 8*1024**3, 'memory_available_bytes': 8*1024**3, 'gpus': []}
    monkeypatch.setattr('backend.runtime.runner.detect_resources', lambda *args: capacity)
    resources = ResourceService(store.path, SimpleNamespace(allows_progress=lambda *args: True), capacity)
    requests = []
    for name in ['a', 'b']:
        project = started(store, name)
        spec = ExperimentSpec(spec_id=name, version=1, question='q', hypothesis='h', fixed={}, baseline='b', data={}, metrics=['m'], design={}, monitoring={}, completion_criteria='exit', deadline_at=project['deadline_at'])
        store.put('spec', name, spec.model_dump(), project['id'])
        requests.append({'research_id': project['id'], 'implementation_agent_id': 'line-01/implementation', 'spec_id': name,
            'spec_version': 1, 'spec_hash': digest(spec.model_dump()), 'resource_request_id': name, 'cpu_cores': 1, 'memory_bytes': 128*1024**2})
    assert [resources.request_allocation(r)['state'] for r in requests] == ['granted', 'granted']
    assert resources.request_allocation({**requests[0], 'resource_request_id': 'too-large', 'cpu_cores': 9})['state'] == 'unsatisfiable'

def test_output_metadata_is_optional_and_concurrency_has_no_arbitrary_ceiling(tmp_path):
    from apps.api.main import Team, Research
    assert Implementation.model_validate({'files': {'main': 'pass'}, 'entrypoint': 'main', 'extra_note': 'allowed'}).actual_config is None
    assert Team(model_concurrency=1000).model_concurrency == 1000
    assert Research(title='test', topic='test', model_concurrency=1000).model_concurrency == 1000
    assert DshRuntimeAdapter(Store(tmp_path), None, None).semaphore is None


@pytest.mark.asyncio
async def test_measured_provider_capacity_is_shared_across_projects(tmp_path, monkeypatch):
    from contextlib import asynccontextmanager
    store = Store(tmp_path)
    projects = [started(store, name) for name in ('a', 'b')]
    provider = store.get('provider', 'deepseek-default')
    benchmark = {k: provider.get(k) for k in ('base_url', 'model', 'credential_version')}
    benchmark['recommended_limit'] = 96
    store.patch('provider', provider['id'], concurrency_benchmark=benchmark)
    adapter = DshRuntimeAdapter(store, None, None)
    monkeypatch.setattr(adapter, 'availability', lambda: {'available': True})
    observed = []
    class AdmissionObserved(Exception): pass
    @asynccontextmanager
    async def admission(*args):
        observed.append(args[3:])
        raise AdmissionObserved
        yield
    monkeypatch.setattr(adapter, '_admission', admission)
    for project in projects:
        with pytest.raises(AdmissionObserved):
            await adapter.execute_turn(research_id=project['id'], agent_id='line-01/implementation', role='implementation',
                task_id='task', turn_id='code', prompt='', schema=Implementation)
    assert observed[0][-1] is observed[1][-1]
    assert observed[0][-1]._value == 96
    store.patch('provider', provider['id'], credential_version=provider.get('credential_version', 0)+1)
    with pytest.raises(AdmissionObserved):
        await adapter.execute_turn(research_id=projects[0]['id'], agent_id='line-01/implementation', role='implementation',
            task_id='task', turn_id='code', prompt='', schema=Implementation)
    assert len(observed[-1]) == 2  # Session identity and project setting; stale measurement is ignored.
