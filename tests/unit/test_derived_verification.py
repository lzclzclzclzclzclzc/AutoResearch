import pytest
from backend.persistence.store import Store
from backend.runtime.evidence import validate_execution_checks


def test_verify_accepts_its_traceable_outputs_and_rejects_unrelated_outputs(tmp_path):
    store=Store(tmp_path)
    project=store.create_project({'topic':'verify','mode':'demo','client_request_id':'verify'})
    rid=project['id']
    source=tmp_path/'source.json'
    source.write_text('{}')
    root=store.register_artifact(rid,source,'application/json',line_id='line-01')
    derived=store.register_artifact(rid,source,'application/json',line_id='line-01',kind='light_cpu_result',source_evidence_ids=[root['id']])
    other=store.register_artifact(rid,source,'application/json',line_id='line-02',kind='light_cpu_result',source_evidence_ids=[root['id']])
    submission={'spec':{'issues':[{'issue_id':'i','category':'must_fix','resolved':False,'acceptance_stage':'execution'}]},'artifacts':[root]}
    output={'passed':True,'execution_checks':[{'issue_id':'i','passed':True,'evidence':[{'source_id':derived['id'],'status':'observed'}]}]}
    validate_execution_checks(store,rid,submission,output,'line-01')
    output['execution_checks'][0]['evidence'][0]['source_id']=other['id']
    with pytest.raises(ValueError): validate_execution_checks(store,rid,submission,output,'line-01')
