import pytest
from backend.graphs.engine import ResearchEngine
from packages.contracts.research import ExperimentSpec, Implementation


@pytest.mark.parametrize('entrypoint', ['main', './main'])
def test_recovery_accepts_extensionless_python_entrypoint(entrypoint):
    engine = ResearchEngine.__new__(ResearchEngine)
    spec = ExperimentSpec(spec_id='s', version=1, question='q', hypothesis='h', fixed={}, baseline='b', data={},
        metrics=['m'], design={}, monitoring={}, completion_criteria='exit', deadline_at='2030-01-01T00:00:00Z')
    output = {'files': {'main': 'print(42)'}, 'entrypoint': entrypoint}
    assert engine.validate_recovery_output('r', 'line-01/implementation', Implementation, output, {'spec': spec.model_dump()})['entrypoint'] == entrypoint
