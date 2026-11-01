import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from apps.api.main import Team
from backend.adapters.dsh import ResearchStopped
from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor
from backend.runtime.watchdog import WatchdogService
from backend.adapters.gateway import ToolGateway


@pytest.fixture
def scene(tmp_path):
    store = Store(tmp_path)
    project = store.start_project(store.create_project({'topic': 'watch', 'client_request_id': 'watch', 'mode': 'demo'})['id'])
    rid = project['id']
    control = ControlSupervisor(tmp_path/'control')
    control.register_project(rid, project['deadline_at'])
    runner = SimpleNamespace(status=lambda: {'runs': []}, resources=lambda: {'allocations': []})
    clock = [datetime.now(timezone.utc)]
    calls = []
    class Adapter:
        async def execute_turn(self, **kwargs):
            calls.append(kwargs)
            return {'content_markdown': '检查原任务丢失领取，恢复检查点。', 'actions': []}
    engine = SimpleNamespace(store=store, control=control, runner=runner, demo=Adapter(), live=Adapter())
    service = WatchdogService(engine, clock=lambda: clock[0])
    yield service, store, project, clock, calls
    control.close()


def test_interval_is_positive_and_defaults_to_ten():
    assert Team().watchdog_interval_minutes == 10
    for value in (0, -1, 1.5, '2'):
        with pytest.raises(ValueError):
            Team(watchdog_interval_minutes=value)


@pytest.mark.asyncio
async def test_clock_restart_missed_periods_and_concurrent_claim(scene):
    service, store, project, clock, calls = scene
    await service.poll()
    assert calls == []
    clock[0] += timedelta(minutes=31)
    second = WatchdogService(service.engine, clock=service.clock)
    await asyncio.gather(service.poll(), second.poll())
    await asyncio.gather(*service.active.values(), *second.active.values())
    assert len(calls) == 2  # one diagnosis for each actually stalled line
    assert len(store.list('watchdog_event', project['id'])) == 2
    assert datetime.fromisoformat(store.get('watchdog_schedule', project['id'])['next_due_at']) == clock[0] + timedelta(minutes=10)
    await service.close()
    await second.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['boss', 'organizer', 'paused', 'resources'])
async def test_healthy_shared_waiting_and_paused_projects_do_not_call_model(scene, kind):
    service, store, project, clock, calls = scene
    rid = project['id']
    await service.poll()
    active = set()
    if kind in {'boss', 'organizer'}:
        task = store.add_task(rid, kind, {}, 'shared')
        active.add(task['id'])
    elif kind == 'paused':
        store.update_project(rid, status='paused')
    else:
        service.runner.resources = lambda: {'allocations': [{'research_id': rid, 'line_id': line['line_id'], 'state': 'queued', 'allocation_id': line['line_id']} for line in store.lines(rid)]}
    clock[0] += timedelta(minutes=10)
    await service.poll(active)
    assert not calls and not service.active


@pytest.mark.asyncio
async def test_one_busy_line_does_not_hide_other_stalled_line(scene):
    service, store, project, clock, calls = scene
    rid = project['id']
    task = store.add_task(rid, 'line', {'line_id': 'line-01', 'direction_epoch': 1}, 'busy')
    await service.poll()
    clock[0] += timedelta(minutes=10)
    await service.poll({task['id']})
    await asyncio.gather(*service.active.values())
    assert len(calls) == 1 and calls[0]['context']['event']['line_id'] == 'line-02'
    await service.close()


def test_prompt_injection_and_resume_are_idempotent_and_control_fenced(scene):
    service, store, project, clock, calls = scene
    rid = project['id']
    task = store.add_task(rid, 'line', {'line_id': 'line-01', 'direction_epoch': 1}, 'lost')
    store.patch('task', task['id'], status='failed')
    event = store.put('watchdog_event', 'event', {'line_id': 'line-01', 'direction_epoch': 1, 'cycle_id': None, 'status': 'diagnosing'}, rid)
    action = {'action': 'inject_prompt', 'target_id': task['id'], 'content_markdown': 'experiment_spec 需补齐 question、fixed、metrics'}
    first = service.apply(event['id'], action)
    assert service.apply(event['id'], action) == first
    assert len(store.get('task', task['id'])['watchdog_prompts']) == 1
    assert first['before']['status'] == 'failed' and first['after']['status'] == 'pending'
    store.update_project(rid, status='terminated')
    with pytest.raises(ResearchStopped):
        service.apply(event['id'], {'action': 'resume_task', 'target_id': task['id']})


@pytest.mark.asyncio
async def test_custom_interval_survives_restart(scene):
    service, store, project, clock, calls = scene
    config = project['config_snapshot']
    config['team']['watchdog_interval_minutes'] = 2
    store.update_project(project['id'], config_snapshot=config)
    await service.poll()
    clock[0] += timedelta(minutes=1)
    await WatchdogService(service.engine, clock=service.clock).poll()
    assert not calls
    clock[0] += timedelta(minutes=1)
    await service.poll()
    await asyncio.gather(*service.active.values())
    assert len(calls) == 2
    await service.close()


def test_watchdog_tools_are_project_scoped_and_only_for_bound_diagnosis(scene):
    service, store, project, _, _ = scene
    rid = project['id']
    store.put('watchdog_event', 'event', {'line_id': 'line-01', 'direction_epoch': 1, 'cycle_id': None, 'status': 'diagnosing'}, rid)
    gateway = ToolGateway(store, service.control, service.runner)
    context = {'research_id': rid, 'role': 'watchdog', 'agent_id': 'watchdog', 'control_generation': 1,
               'watchdog_event_id': 'event', 'execution_id': 'diagnostic'}
    try:
        token = gateway.register(**context)
        response = gateway.call(token, {'name': 'read_watchdog_state', 'arguments': {'section': 'line'}})
        assert rid in response['text']
        ordinary = gateway.register(**{**context, 'role': 'implementation'})
        with pytest.raises(PermissionError):
            gateway.call(ordinary, {'name': 'read_watchdog_state', 'arguments': {'section': 'line'}})
        with pytest.raises(ValueError):
            gateway.call(token, {'name': 'read_watchdog_state', 'arguments': {'section': 'credentials'}})
        store.patch('watchdog_event', 'event', status='repaired')
        with pytest.raises(PermissionError):
            gateway.call(token, {'name': 'read_watchdog_state', 'arguments': {'section': 'line'}})
    finally:
        gateway.close()


def test_restore_missing_parent_is_once_and_preserves_original_scope(scene):
    service, store, project, _, _ = scene
    rid = project['id']
    store.update_line(rid, 'line-01', cycle_id='cycle')
    line = store.get('line', rid + ':line-01')
    store.put('watchdog_event', 'event', {'line_id': 'line-01', 'direction_epoch': 1, 'cycle_id': 'cycle', 'status': 'diagnosing'}, rid)
    action = {'action': 'restore_line_task', 'target_id': line['id']}
    first = service.apply('event', action)
    assert service.apply('event', action) == first
    tasks = [task for task in store.list('task', rid) if task['kind'] == 'line']
    assert len(tasks) == 1 and tasks[0]['payload'] == {'line_id': 'line-01', 'cycle_id': 'cycle', 'direction_epoch': 1}


def test_progress_requires_the_repaired_target_to_be_taken_over(scene):
    service, store, project, _, _ = scene
    rid = project['id']
    target = store.add_task(rid, 'line', {'line_id': 'line-01'}, 'target')
    other = store.add_task(rid, 'line', {'line_id': 'line-02'}, 'other')
    store.claim_task(other['id'], 'worker')
    operation = {'status': 'applied', 'action': {'action': 'resume_task'}, 'after': target}
    assert not service._actual_progress(operation)
    store.claim_task(target['id'], 'worker')
    assert service._actual_progress(operation)
    handoff = {'research_id': rid, 'status': 'applied', 'action': {'action': 'redeliver_handoff'},
               'before': {'id': 'outbox'}, 'after': {'status': 'delivered'}}
    assert not service._actual_progress(handoff)
    next_task = store.add_task(rid, 'boss', {}, 'next')
    store.patch('task', next_task['id'], outbox_id='outbox')
    store.claim_task(next_task['id'], 'worker')
    assert service._actual_progress(handoff)


def test_scope_is_rechecked_between_action_registration_and_task_change(scene, monkeypatch):
    service, store, project, _, _ = scene
    rid = project['id']
    task = store.add_task(rid, 'line', {'line_id': 'line-02'}, 'other-line')
    store.patch('task', task['id'], status='failed')
    store.put('watchdog_event', 'event', {'line_id': 'line-01', 'direction_epoch': 1, 'cycle_id': None, 'status': 'diagnosing'}, rid)
    original = store._put
    def change_scope(c, kind, object_id, data, research_id=None):
        result = original(c, kind, object_id, data, research_id)
        if kind == 'watchdog_action' and data.get('status') == 'pending':
            line = store._get(c, 'line', rid + ':line-01')
            original(c, 'line', line['id'], {**line, 'direction_epoch': 2}, rid)
        return result
    monkeypatch.setattr(store, '_put', change_scope)
    with pytest.raises(ResearchStopped):
        service.apply('event', {'action': 'resume_task', 'target_id': task['id']})
    assert store.get('task', task['id'])['status'] == 'failed'
