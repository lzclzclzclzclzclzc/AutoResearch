import json
from pathlib import Path
from types import SimpleNamespace
import pytest
from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor
from backend.runtime.light_cpu import LightCpuService
from backend.runtime.runner import ResourceService


@pytest.fixture
def runtime(tmp_path):
    store = Store(tmp_path)
    project = store.start_project(store.create_project({'topic': 'native output', 'mode': 'demo', 'client_request_id': 'native'})['id'])
    control = ControlSupervisor(tmp_path/'control')
    control.register_project(project['id'], project['deadline_at'])
    resources = ResourceService(store.path, control, {'cpu_cores': 8, 'memory_total_bytes': 8*1024**3, 'gpus': []})
    service = LightCpuService(tmp_path, control, resources, 'runner', 'generation')
    agent = 'line-01/implementation'
    store.put('agent', project['id']+':'+agent, {'role': 'implementation'}, project['id'])
    payload = {'research_id': project['id'], 'agent_id': agent, 'role': 'implementation', 'line_id': 'line-01', 'control_generation': 1}
    try: yield store, service, payload
    finally:
        service.close()
        control.close()


def test_binary_evidence_is_read_by_the_calculation_and_native_output_is_registered(runtime, tmp_path):
    import numpy as np
    store, service, payload = runtime
    source = tmp_path/'values.npy'
    np.save(source, np.array([1,2,3]))
    artifact = store.register_artifact(payload['research_id'], source, 'application/octet-stream', line_id='line-01')
    code = f"import numpy as np\nfrom pathlib import Path\nvalues=np.load(evidence[{artifact['id']!r}]['path'])\nnp.save(Path(workspace_dir)/'squared.npy', values**2)\nresult=int(values.sum())"
    job = service.execute({**payload, 'execution_id': 'binary', 'evidence_ids': [artifact['id']], 'code': code})
    assert job['state'] == 'completed', job
    assert job['result'] == 6
    output = next(a for a in job['output_artifacts'] if a['name'] == 'squared.npy')
    _, path = store.artifact(output['id'])
    assert np.load(path).tolist() == [1,4,9]
    assert output['source_evidence_ids'] == [artifact['id']]


def test_non_json_result_does_not_fail_a_successful_calculation(runtime):
    store, service, payload = runtime
    code = "from decimal import Decimal\nfrom pathlib import Path\nresult=Decimal('0.15')\nPath(workspace_dir,'answer.bin').write_bytes(b'\\x00\\x01')"
    job = service.execute({**payload, 'execution_id': 'decimal', 'code': code})
    assert job['state'] == 'completed', job
    assert "0.15" in job['result_text']
    assert {'result.txt', 'answer.bin'} <= {a['name'] for a in job['output_artifacts']}
