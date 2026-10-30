from types import SimpleNamespace
from backend.persistence.store import Store
from backend.runtime.runner import ResourceService
from packages.contracts.research import ExperimentSpec, digest


def test_project_at_its_limit_cannot_block_other_projects(tmp_path, monkeypatch):
    store = Store(tmp_path)
    capacity = {'cpu_cores': 8, 'memory_total_bytes': 8*1024**3, 'memory_available_bytes': 8*1024**3, 'gpus': []}
    monkeypatch.setattr('backend.runtime.runner.detect_resources', lambda *args: capacity)
    resources = ResourceService(store.path, SimpleNamespace(allows_progress=lambda *args: True), capacity)
    requests = []
    for name in ['a', 'b']:
        project = store.start_project(store.create_project({'topic': name, 'mode': 'demo', 'client_request_id': name})['id'])
        spec = ExperimentSpec(spec_id=name, version=1, question='q', hypothesis='h', fixed={}, baseline='b', data={},
            metrics=['m'], design={}, monitoring={}, completion_criteria='exit', deadline_at=project['deadline_at'])
        store.put('spec', name, spec.model_dump(), project['id'])
        requests.append({'research_id': project['id'], 'implementation_agent_id': 'line-01/implementation', 'spec_id': name,
            'spec_version': 1, 'spec_hash': digest(spec.model_dump()), 'resource_request_id': name, 'cpu_cores': 1, 'memory_bytes': 128*1024**2})
    assert resources.request_allocation(requests[0])['state'] == 'granted'
    assert resources.request_allocation({**requests[0], 'resource_request_id': 'a-waiting', 'priority': 10})['state'] == 'queued'
    assert resources.request_allocation(requests[1])['state'] == 'granted'
