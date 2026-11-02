"""Watch Dog diagnostics retain durable attempts and trusted Runner scope."""
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor
from backend.runtime.generation_budget import GenerationBudget
from backend.runtime.watchdog import WatchdogService


@pytest.fixture
def scene(tmp_path):
    store = Store(tmp_path)
    project = store.start_project(store.create_project({'topic': 'recovery', 'client_request_id': 'watchdog-recovery', 'mode': 'demo'})['id'])
    control = ControlSupervisor(tmp_path / 'control')
    control.register_project(project['id'], project['deadline_at'])
    calls, returned = [], []
    class Adapter:
        async def execute_turn(self, **arguments):
            calls.append(arguments)
            return returned.pop(0) if returned else {'content_markdown': '依据回执修复', 'actions': []}
    receipts = {}
    runner = SimpleNamespace(status=lambda run_id=None: receipts.get(run_id, {'runs': []} if run_id is None else {'state': 'unknown'}), resources=lambda: {'allocations': []})
    engine = SimpleNamespace(store=store, control=control, runner=runner, demo=Adapter(), live=Adapter())
    service = WatchdogService(engine)
    event = store.put('watchdog_event', 'event', {'line_id': 'line-01', 'direction_epoch': 1,
        'cycle_id': None, 'status': 'diagnosing', 'action_ids': [], 'diagnosis_round': 0, 'lease_token': 'first'}, project['id'])
    yield service, store, project, event, calls, returned, receipts
    control.close()


@pytest.mark.asyncio
async def test_invalid_diagnosis_gets_two_corrections_in_same_durable_budget(scene):
    service, store, project, event, calls, returned, _ = scene
    returned.extend([{'actions': []}, {'content_markdown': '纠正', 'actions': [{'action': 'invented', 'target_id': 'none'}]}])
    await service.diagnose(event)
    assert len(calls) == 3 and len({call['turn_id'] for call in calls}) == 3
    assert 'content_markdown' in calls[1]['prompt'] and '字段校验失败' in calls[1]['prompt']
    assert 'invented' in calls[2]['prompt']
    budget = store.list('generation_budget', project['id'])[0]
    assert [item['status'] for item in budget['attempts']] == ['rejected', 'rejected', 'accepted']
    assert store.get('watchdog_event', event['id'])['status'] == 'awaiting_progress'


@pytest.mark.asyncio
async def test_diagnosis_keeps_correcting_past_three_and_restart_reuses_acceptance(scene):
    service, store, project, event, calls, returned, _ = scene
    returned.extend([{'actions': []}] * 5)
    await service.diagnose(event)
    assert store.get('watchdog_event', event['id'])['status'] == 'awaiting_progress'
    assert len(store.list('generation_budget', project['id'])[0]['attempts']) == 6
    restarted = store.patch('watchdog_event', event['id'], status='diagnosing', diagnosis_round=0)
    await WatchdogService(service.engine).diagnose(restarted)
    assert len(calls) == 6


@pytest.mark.asyncio
async def test_returned_invalid_reply_is_revalidated_and_corrected_after_crash(scene, monkeypatch):
    service, store, project, event, calls, returned, _ = scene
    returned.append({'actions': []})
    record, crashed = GenerationBudget.record, False
    def crash_after_return(self, attempt, status, **fields):
        nonlocal crashed
        record(self, attempt, status, **fields)
        if status == 'returned' and not crashed:
            crashed = True
            raise RuntimeError('crash after raw reply persisted')
    monkeypatch.setattr(GenerationBudget, 'record', crash_after_return)
    with pytest.raises(RuntimeError, match='raw reply'):
        await service.diagnose(event)
    await WatchdogService(service.engine).diagnose(store.get('watchdog_event', event['id']))
    assert len(calls) == 2
    budget = store.list('generation_budget', project['id'])[0]
    assert [item['status'] for item in budget['attempts']] == ['rejected', 'accepted']


@pytest.mark.asyncio
async def test_accepted_diagnosis_is_reused_after_message_commit_crash(scene, monkeypatch):
    service, store, _, event, calls, _, _ = scene
    create, crashed = store.create_message, False
    def fail_once(*args, **kwargs):
        nonlocal crashed
        if kwargs.get('kind') == 'watchdog' and not crashed:
            crashed = True
            raise RuntimeError('message commit crashed')
        return create(*args, **kwargs)
    monkeypatch.setattr(store, 'create_message', fail_once)
    with pytest.raises(RuntimeError, match='message commit'):
        await service.diagnose(event)
    await service.diagnose(store.get('watchdog_event', event['id']))
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_late_diagnostic_cannot_apply_or_overwrite_new_lease(scene):
    service, store, project, event, _, _, _ = scene
    task = store.add_task(project['id'], 'line', {'line_id': 'line-01', 'direction_epoch': 1}, 'failed-task')
    store.patch('task', task['id'], status='failed')
    async def superseded(**arguments):
        assert arguments['runtime_context']['watchdog_lease_token'] == 'first'
        store.patch('watchdog_event', event['id'], lease_token='replacement', status='awaiting_progress')
        return {'content_markdown': '迟到的旧诊断', 'actions': [{'action': 'resume_task', 'target_id': task['id']}]}
    service.engine.demo.execute_turn = superseded
    await service.diagnose(event)
    current = store.get('watchdog_event', event['id'])
    assert current['lease_token'] == 'replacement' and current['status'] == 'awaiting_progress'
    assert store.get('task', task['id'])['status'] == 'failed'
    assert not store.list('watchdog_action', project['id'])


@pytest.mark.asyncio
async def test_final_diagnostic_commit_rechecks_original_direction(scene, monkeypatch):
    service, store, project, event, _, _, _ = scene
    create = store.create_message
    def change_direction(*args, **kwargs):
        message = create(*args, **kwargs)
        if kwargs.get('kind') == 'watchdog':
            store.update_line(project['id'], 'line-01', direction_epoch=2)
        return message
    monkeypatch.setattr(store, 'create_message', change_direction)
    await service.diagnose(event)
    assert store.get('watchdog_event', event['id'])['diagnosis_round'] == 0
    assert not store.list('watchdog_action', project['id'])


def current_line(store, project):
    return store.update_line(project['id'], 'line-01', cycle_id='cycle-2', direction_epoch=2, active_spec_id='spec-2')


@pytest.mark.asyncio
async def test_action_failure_is_corrected_and_successful_actions_are_not_reapplied(scene):
    service, store, project, event, calls, returned, _ = scene
    rid = project['id']
    task = store.add_task(rid, 'line', {'line_id': 'line-01'}, 'repair-target')
    store.patch('task', task['id'], status='failed')
    resume = {'action': 'resume_task', 'target_id': task['id']}
    returned.extend([
        {'content_markdown': 'Resume then inject', 'actions': [resume,
            {'action': 'inject_prompt', 'target_id': task['id']}]},
        {'content_markdown': 'Correct injection', 'actions': [resume,
            {'action': 'inject_prompt', 'target_id': task['id'], 'content_markdown': 'Fill next_step'}]},
    ])
    await service.diagnose(event)
    assert len(calls) == 2 and '注入必须说明' in calls[1]['prompt']
    assert store.get('task', task['id'])['watchdog_recovery_generation'] == 2
    assert store.get('watchdog_event', event['id'])['status'] == 'awaiting_progress'
    assert [item['status'] for item in store.list('generation_budget', rid)[0]['attempts']] == ['rejected', 'accepted']


@pytest.mark.asyncio
async def test_initial_context_only_contains_location_and_paths(scene):
    service, store, project, event, calls, _, _ = scene
    store.add_task(project['id'], 'boss', {'unrelated_history': 'not-in-initial-prompt' * 1000}, 'history')
    await service.diagnose(event)
    context = calls[0]['context']
    assert set(context) == {'location', 'paths'}
    assert context['paths']['business_db'] == str(store.path)
    assert 'not-in-initial-prompt' not in calls[0]['prompt']
    assert 'read_arbitration_context' not in calls[0]['prompt']
    assert 'snapshot_manifest' not in calls[0]['prompt']


@pytest.mark.parametrize('changed', [{'direction_epoch': 1}, {'cycle_id': 'cycle-1'}, {'spec_id': 'spec-1'}])
def test_old_runner_scope_and_allocations_cannot_mask_current_stall(scene, changed):
    service, store, project, _, _, _, _ = scene
    line = current_line(store, project)
    item = {'research_id': project['id'], 'line_id': 'line-01', 'direction_epoch': 2, 'cycle_id': 'cycle-2', 'spec_id': 'spec-2', **changed}
    observation = service.inspect(project, line, set(), {'runs': [{**item, 'state': 'running'}]},
        {'allocations': [{**item, 'state': 'queued', 'allocation_id': 'allocation'}]})
    assert observation['state'] == 'stalled'


@pytest.mark.parametrize('confirmed', ['completed', 'running', 'unknown'])
def test_unknown_runner_is_reconciled_without_authorizing_replacement(scene, confirmed):
    service, store, project, _, calls, _, receipts = scene
    line = current_line(store, project)
    run = {'research_id': project['id'], 'line_id': 'line-01', 'spec_id': 'spec-2', 'experiment_run_id': 'run', 'state': 'unknown'}
    store.put('run', 'run', run, project['id'])
    receipts['run'] = {**run, 'state': confirmed}
    observation = service.inspect(project, line, set(), {'runs': [run]}, {'allocations': []})
    assert observation['state'] == ('active' if confirmed == 'running' else 'stalled')
    if confirmed == 'unknown':
        assert observation['reason'] == 'runner_exit_unconfirmed'
        assert store.get('run', 'run')['state'] == 'unknown'
    else:
        assert store.get('run', 'run')['state'] == confirmed
    assert not calls


def test_boss_bound_to_one_submission_still_covers_other_lines(scene):
    service, store, project, _, calls, _, _ = scene
    task = store.add_task(project['id'], 'boss', {'line_id': 'line-01', 'submission_id': 'submission'}, 'boss-review')
    line = store.get('line', project['id'] + ':line-02')
    observation = service.inspect(project, line, {task['id']}, {'runs': []}, {'allocations': []})
    assert observation['state'] == 'active' and not calls
