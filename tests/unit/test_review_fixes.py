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
