import pytest

from backend.persistence.store import Store, StoreError
from backend.runtime.control import ControlSupervisor
from backend.runtime.evidence import validate_report_facts
from packages.contracts.research import Verification, LegacyVerification, Role
from packages.prompts.roles import role_prompt


@pytest.mark.parametrize('passed,k,expected', [(False, 3, 'exploration_and_critic'), (True, 3, 'boss'), (False, 1, 'boss'), (True, 1, 'boss')])
def test_three_field_report_counts_and_delivers_once(tmp_path, passed, k, expected):
    store = Store(tmp_path)
    project = store.start_project(store.create_project({'topic': 'report', 'client_request_id': 'report', 'mode': 'demo'})['id'])
    rid = project['id']
    line = store.lines(rid)[0]
    store.update_line(rid, line['line_id'], cycle_id='cycle')
    store.put('cycle', 'cycle', {'line_id': line['line_id'], 'direction_epoch': line['direction_epoch'], 'k': k, 'status': 'verifying'}, rid)
    store.put('run', 'run', {'line_id': line['line_id'], 'state': 'completed'}, rid)
    submission = {'cycle_id': 'cycle', 'line_id': line['line_id'], 'spec_id': 'spec', 'spec_version': 1,
                  'code_version': 'code', 'run_ids': ['run'], 'verification_contract_version': 2}
    store.put('submission', 'submission', submission, rid)
    text = '已查阅 run 的输出；本次为有效负结果。局限是只有一次运行，后续需增加样本。'
    report = {**Verification(content_markdown=text, passed=passed, scientific_observation='negative').model_dump(),
              **{key: submission[key] for key in ('spec_id', 'spec_version', 'code_version', 'run_ids')}}
    control = ControlSupervisor(tmp_path/'control')
    control.register_project(rid, project['deadline_at'])
    try:
        result = store.commit_verification('cycle', 'submission', report, 1, control)
        assert result['route'] == expected
        assert store.commit_verification('cycle', 'submission', report, 1, control) == result
        assert store.get('cycle', 'cycle')['completed_verify_rounds'] == 1
        store.dispatch_outbox(control)
        store.dispatch_outbox(control)
        inboxes = store.list('inbox', rid)
        assert len(inboxes) == (1 if expected == 'boss' else 2)
        messages = store.messages('cycle:feedback')['items']
        assert len(messages) == 1 and messages[0]['content_markdown'] == text
        assert 'checks' not in store.list('verification', rid)[0]
    finally:
        control.close()


@pytest.mark.parametrize('values', [{'content_markdown': ' ', 'passed': False, 'scientific_observation': 'inconclusive'},
                                  {'content_markdown': 'report', 'passed': 'true', 'scientific_observation': 'negative'}])
def test_new_contract_rejects_empty_report_and_non_boolean(values):
    with pytest.raises(ValueError):
        Verification.model_validate(values)


def test_legacy_contract_and_current_prompt_are_distinct():
    with pytest.raises(ValueError):
        LegacyVerification(content_markdown='report', passed=False, scientific_observation='inconclusive')
    assert set(Verification.model_json_schema()['properties']) == {'content_markdown', 'passed', 'scientific_observation'}
    assert '只输出 content_markdown、passed、scientific_observation' in role_prompt(Role.VERIFY, verification_contract_version=2)


def test_system_run_facts_reject_unexecuted_or_failed_pass(tmp_path):
    store = Store(tmp_path)
    project = store.create_project({'topic': 'facts', 'client_request_id': 'facts'})
    rid = project['id']
    report = {'passed': True, 'scientific_observation': 'negative'}
    for state in ('running', 'failed'):
        store.put('run', 'r', {'line_id': 'line-01', 'state': state}, rid)
        with pytest.raises(ValueError, match='real acceptable run'):
            validate_report_facts(store, rid, {'run_ids': ['r']}, report, 'line-01')
    store.put('run', 'r', {'line_id': 'line-01', 'state': 'completed'}, rid)
    validate_report_facts(store, rid, {'run_ids': ['r']}, report, 'line-01')
    with pytest.raises(ValueError, match='execution failure'):
        validate_report_facts(store, rid, {'run_ids': ['r']}, {**report, 'scientific_observation': 'execution_failed'}, 'line-01')
