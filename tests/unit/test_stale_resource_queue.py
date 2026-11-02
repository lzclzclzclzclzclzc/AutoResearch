"""Only currently admissible frozen requests participate in queue fairness."""
import sqlite3
from types import SimpleNamespace

import pytest

from backend.persistence.store import Store
from backend.runtime.control import ControlUnavailable
from backend.runtime.runner import ResourceConflict, ResourceService
from packages.contracts.research import ExperimentSpec, digest


@pytest.fixture
def waiting_request(tmp_path, monkeypatch):
    store = Store(tmp_path)
    capacity = {'cpu_cores': 1, 'memory_total_bytes': 1024**3,
        'memory_available_bytes': 1024**3, 'gpus': []}
    monkeypatch.setattr('backend.runtime.runner.detect_resources', lambda *args: capacity)
    service = ResourceService(store.path, SimpleNamespace(allows_progress=lambda *args: True), capacity)
    requests = []
    for name in ['holder', 'waiting']:
        project = store.start_project(store.create_project({
            'topic': name, 'mode': 'demo', 'client_request_id': name, 'max_runtime_seconds': 300,
            'team': {'n': 2, 'm1': 1, 'm2': 1, 'boss_members': 1},
        })['id'])
        store.update_line(project['id'], 'line-01', direction_epoch=1, cycle_id='cycle-1')
        spec = ExperimentSpec(spec_id='spec-' + name, version=1, question=name, fixed={},
            metrics=['value'], deadline_at=project['deadline_at'], discussion_protocol=2)
        store.put('spec', spec.spec_id, spec.model_dump(), project['id'])
        requests.append({'research_id': project['id'], 'resource_request_id': name,
            'implementation_agent_id': 'line-01/implementation', 'line_id': 'line-01',
            'spec_id': spec.spec_id, 'spec_version': spec.version, 'spec_hash': digest(spec.model_dump()),
            'direction_epoch': 1, 'cycle_id': 'cycle-1', 'control_generation': 1,
            'cpu_cores': 1, 'memory_bytes': 128 * 1024**2, 'gpu_count': 0, 'priority': 10})
    assert service.request_allocation(requests[0])['state'] == 'granted'
    queued = service.request_allocation(requests[1])
    assert queued['state'] == 'queued'
    service.cancel_unbound(requests[0]['research_id'], 'fixture_release')
    return store, service, project, spec, requests[1], queued


@pytest.mark.parametrize('changed', ['direction', 'cycle', 'direction_stop', 'finish'])
def test_stale_frozen_waiter_does_not_block_a_valid_new_request(waiting_request, changed):
    store, service, project, spec, original, queued = waiting_request
    rid = project['id']
    fresh = {**original, 'resource_request_id': 'fresh', 'priority': 0}
    if changed == 'direction':
        store.update_line(rid, 'line-01', direction_epoch=2)
        fresh['direction_epoch'] = 2
    elif changed == 'cycle':
        store.update_line(rid, 'line-01', cycle_id='cycle-2')
        fresh['cycle_id'] = 'cycle-2'
    else:
        store.update_line(rid, 'line-01', **({'direction_stop_epoch': 1} if changed == 'direction_stop' else {'finish_requested': True}))
        store.update_line(rid, 'line-02', direction_epoch=1, cycle_id='cycle-1')
        spec = spec.model_copy(update={'spec_id': 'other-line-spec'})
        store.put('spec', spec.spec_id, spec.model_dump(), rid)
        fresh.update(line_id='line-02', implementation_agent_id='line-02/implementation',
            spec_id=spec.spec_id, spec_hash=digest(spec.model_dump()))
    # The old request fails the actual frozen admission check, despite higher priority.
    with pytest.raises(ResourceConflict):
        service.request_allocation(original)
    granted = service.request_allocation(fresh)
    assert granted['state'] == 'granted'
    assert granted['allocation_id'] != queued['allocation_id']
    retained = store.get('allocation', queued['allocation_id'])
    assert retained['state'] == 'queued'
    assert retained['requested_at'] == queued['requested_at']
    assert not retained.get('stop_reason') and not retained.get('terminal_proof')


@pytest.mark.parametrize('binding', ['spec_version', 'spec_hash'])
def test_restored_waiter_with_inconsistent_plan_binding_does_not_block_valid_work(waiting_request, binding):
    store, service, _, spec, original, queued = waiting_request
    # Model a ledger restored with a plan binding that disagrees with the
    # immutable business spec. No frozen spec is edited or validation bypassed.
    restored = {**queued, binding: 2 if binding == 'spec_version' else 'obsolete-plan-hash'}
    with service.connect(write=True) as c:
        service._put_allocation(c, restored)
    with pytest.raises(ResourceConflict):
        service.request_allocation(restored)
    assert service.request_allocation({**original, 'resource_request_id': 'fresh', 'priority': 0})['state'] == 'granted'
    assert store.get('spec', spec.spec_id)['question'] == spec.question
    assert store.get('allocation', queued['allocation_id']) == restored


@pytest.mark.parametrize('failure', ['database', 'control'])
def test_transient_validation_failure_preserves_valid_waiter_and_its_priority(waiting_request, monkeypatch, failure):
    store, service, _, _, original, queued = waiting_request
    fresh = {**original, 'resource_request_id': 'fresh', 'priority': 0}
    validate = service._frozen_spec
    with monkeypatch.context() as patch:
        if failure == 'database':
            def unavailable(c, request):
                if request['resource_request_id'] == original['resource_request_id']:
                    raise sqlite3.OperationalError('isolated temporary read failure')
                return validate(c, request)
            patch.setattr(service, '_frozen_spec', unavailable)
            expected = sqlite3.OperationalError
        else:
            def unavailable(*args):
                raise ControlUnavailable('isolated temporary transport failure')
            patch.setattr(service.control, 'allows_progress', unavailable)
            expected = ControlUnavailable
        with pytest.raises(expected):
            service.request_allocation(fresh)
    retained = store.get('allocation', queued['allocation_id'])
    assert retained == queued
    # Once validation recovers, the valid older request still gets its turn.
    assert service.request_allocation(fresh)['state'] == 'queued'
    granted = service.request_allocation(original)
    assert granted['state'] == 'granted'
    assert granted['allocation_id'] == queued['allocation_id']
    assert granted['requested_at'] == queued['requested_at']
