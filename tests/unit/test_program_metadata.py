import pytest
from types import SimpleNamespace
from backend.adapters.output_contract import model_contract, bind_metadata
from backend.graphs.engine import ResearchEngine
from backend.persistence.store import Store
from packages.contracts.research import ExperimentSpec, Implementation, MonitorDecision, GodDecision, digest


def spec():
    return ExperimentSpec(spec_id='s', version=1, question='q', hypothesis='h', fixed={}, baseline='b', data={},
        metrics=['m'], design={}, monitoring={}, completion_criteria='exit', deadline_at='2030-01-01T00:00:00Z')


def test_system_fields_are_omitted_from_model_contract_and_bound_by_program():
    plan = spec()
    context = {'spec_id': 's', 'spec_version': 1, 'deadline_at': plan.deadline_at}
    contract = model_contract(ExperimentSpec, context)
    assert not {'spec_id','version','deadline_at'} & set(contract.model_json_schema()['properties'])
    value = plan.model_dump()
    for field in ['spec_id','version','deadline_at']: value.pop(field)
    assert contract.model_validate(value) == plan
    value.update(spec_id='wrong', version=99, deadline_at='wrong')
    assert contract.model_validate(value) == plan
    impl = model_contract(Implementation, {'spec': plan.model_dump()})
    assert 'spec_hash' not in impl.model_json_schema()['properties']
    assert impl.model_validate({'files': {'main':'pass'}, 'entrypoint':'main', 'spec_hash':'wrong'}).spec_hash == digest(plan.model_dump())


def test_god_action_needs_no_copied_system_identity_or_target_hash():
    context = {'arbitration_request': {'arbitration_id':'a', 'snapshot_hash':'snapshot', 'target_schema_hash':'target'}}
    contract = model_contract(GodDecision, context)
    for node in contract.model_json_schema()['$defs'].values():
        assert not {'arbitration_id','snapshot_hash','target_schema_hash'} & set(node.get('properties', {}))
    value = contract.model_validate({'action':'repair_output', 'content_markdown':'repair', 'rationale':'format', 'output':{}}).model_dump()
    assert value['arbitration_id']=='a' and value['snapshot_hash']=='snapshot' and value['target_schema_hash']=='target'


@pytest.mark.asyncio
async def test_monitor_uses_program_window_without_model_hash_echo(tmp_path):
    engine=ResearchEngine.__new__(ResearchEngine)
    engine.store=Store(tmp_path)
    project=engine.store.start_project(engine.store.create_project({'topic':'monitor','mode':'demo','client_request_id':'monitor'})['id'])
    plan=spec()
    engine.store.put('spec','s',plan.model_dump(),project['id'])
    async def logs(*args): return {'text':'unusable run', 'next_offset':12}
    async def turn(*args,**kwargs): return {'request_early_stop':True,'rule_id':'execution_unusable'}
    async def guard(*args): return project
    stops=[]
    engine.collect_run_logs,engine.turn,engine.guard=logs,turn,guard
    engine.runner=SimpleNamespace(request_stop=lambda *args:stops.append(args))
    output=await engine.monitor_run({'research_id':project['id'],'line_id':'line-01','spec_id':'s'}, {'experiment_run_id':'r'})
    assert output['evidence_log_hash'] and stops==[('r','early_stopped')]
