"""Independent project inspections and auditable, fenced online recovery."""
from __future__ import annotations

import asyncio
import json
import secrets
from datetime import datetime, timedelta, timezone
from typing import Literal

from pydantic import Field

from backend.adapters.dsh import ResearchStopped, RuntimeBlocked
from packages.contracts.research import Contract, digest


class WatchdogAction(Contract):
    action: Literal['inject_prompt', 'resume_task', 'restore_line_task', 'redeliver_handoff', 'reconcile_run']
    target_id: str
    content_markdown: str = ''


class WatchdogDecision(Contract):
    content_markdown: str = Field(min_length=1)
    actions: list[WatchdogAction] = Field(default_factory=list)


WATCHDOG_PROMPT = """你是独立的 Watch Dog，不占科研组发言顺序。先分析本线阻塞快照、最后成功步骤、错误、票、产物版本、任务、Runner 和已试动作，再选择在线修复。
可以跨组读取本项目状态和证据，通过 read_watchdog_state 分页读取。可组合注入原 Agent 下次生成的具体修订提示(inject_prompt，target_id 为原任务)、从原检查点恢复任务(resume_task)、补送已提交交接(redeliver_handoff)、按Runner真实回执同步运行(reconcile_run)。目标必须来自本项目快照。不要重复没有进展的动作；无法修复时说明待补条件。
不能代投、修改冻结产物、改写科学结论或编造实验结果。用户停止、暂停、总截止时间和方向失效优先。仅输出指定 JSON，正文说明诊断依据和每项动作的目的。"""


def clock_time():
    return datetime.now(timezone.utc)


def parse_time(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00')) if value else None


class WatchdogService:
    def __init__(self, engine, clock=clock_time):
        self.engine, self.store = engine, engine.store
        self.control, self.runner, self.clock = engine.control, engine.runner, clock
        self.active = {}

    def allowed(self, project):
        return (project['status'] == 'running' or project['status'] == 'blocked'
                and project.get('blocked_reason') == 'all_lines_blocked') and not project.get('stop_reason') and (
                parse_time(project.get('deadline_at')) is not None and parse_time(project['deadline_at']) > self.clock())

    def snapshot(self, rid):
        kinds = ('line', 'task', 'execution', 'run', 'allocation', 'outbox', 'discussion', 'discussion_state', 'group_discussion',
                 'discussion_model_call', 'generation_budget', 'verification', 'submission', 'watchdog_action', 'watchdog_event')
        return {kind: self.store.list(kind, rid) for kind in kinds}

    @staticmethod
    def _current_scope(item, line):
        if item.get('direction_epoch') is not None and item['direction_epoch'] != line['direction_epoch']:
            return False
        if item.get('cycle_id') is not None and item['cycle_id'] != line.get('cycle_id'):
            return False
        current_spec = line.get('active_spec_id')
        if current_spec and item.get('spec_id') != current_spec:
            return False
        # Legacy Runner records bind a spec rather than copying direction and
        # cycle. The line's frozen spec is the equivalent trusted scope binding.
        if line.get('cycle_id') and item.get('cycle_id') is None and not (current_spec and item.get('spec_id') == current_spec):
            return False
        return True

    @staticmethod
    def _same_run_identity(previous, current):
        return all(previous.get(key) is None or current.get(key) == previous[key] for key in
            ('research_id', 'line_id', 'experiment_run_id', 'allocation_id', 'start_operation_id', 'spec_id', 'code_version'))

    def inspect(self, project, line, active_tasks, runner_state, resources):
        rid, lid = project['id'], line['line_id']
        if line.get('finish_requested') or line.get('phase') == 'completed':
            return {'state': 'skip'}
        tasks = [t for t in self.store.list('task', rid) if t['kind'] != 'watchdog' and (
            t['kind'] in {'boss', 'organizer', 'supervisor'} or
            t.get('payload', {}).get('line_id') == lid and
            t['payload'].get('direction_epoch', line['direction_epoch']) == line['direction_epoch'] and
            t['payload'].get('cycle_id', line.get('cycle_id')) == line.get('cycle_id'))]
        live_tasks = [t for t in tasks if t['id'] in active_tasks]
        if live_tasks:
            return {'state': 'active', 'task_ids': [t['id'] for t in live_tasks]}
        for execution in self.store.list('execution', rid):
            agent = execution.get('agent_id', '')
            if execution.get('status') not in {'running', 'registered'} or not (agent == 'organizer' or agent.startswith('boss/')):
                continue
            owned = any(execution.get('task_id', '').startswith(task_id) for task_id in active_tasks)
            if execution.get('pid'):
                import psutil
                try:
                    owned = abs(psutil.Process(execution['pid']).create_time() - execution.get('pid_create_time', -1)) < .1
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    owned = False
            if owned:
                return {'state': 'active', 'shared_execution_id': execution['id']}
        runs = [r for r in runner_state.get('runs', []) if r.get('research_id') == rid and r.get('line_id') == lid
                and self._current_scope(r, line)
                and r.get('state') in {'starting', 'running', 'registered', 'reconciling', 'unknown'}]
        unresolved = []
        for index, run in enumerate(runs):
            if run.get('state') not in {'unknown', 'reconciling'}:
                continue
            run_id = run.get('experiment_run_id', run.get('id'))
            try:
                receipt = self.runner.status(run_id) if run_id else {}
            except Exception:
                receipt = {}
            if receipt and self._same_run_identity(run, receipt) and receipt.get('state') not in {None, 'unknown', 'reconciling'}:
                runs[index] = receipt
                if self.store.get('run', run_id):
                    self.store.put('run', run_id, receipt, rid)
            else:
                unresolved.append(run)
        if unresolved:
            # An unknown process is never healthy and never authorizes a new
            # run. Persist the blockage so diagnosis can inspect the receipts.
            return {'state': 'stalled', 'reason': 'runner_exit_unconfirmed', 'runs': unresolved,
                'line_phase': line.get('phase')}
        runs = [run for run in runs if run.get('state') in {'starting', 'running', 'registered'}]
        if runs:
            return {'state': 'active', 'runs': runs}
        allocations = [a for a in resources.get('allocations', []) if a.get('research_id') == rid and a.get('line_id') == lid
                       and self._current_scope(a, line)
                       and a.get('state') in {'queued', 'granted', 'starting'}]
        if allocations:
            return {'state': 'waiting_resources', 'allocations': [a['allocation_id'] for a in allocations]}
        for task in tasks:
            if task['status'] == 'retry_wait' and parse_time(task.get('retry_not_before')) and parse_time(task['retry_not_before']) > self.clock():
                return {'state': 'waiting_retry', 'until': task['retry_not_before']}
            if task['status'] == 'waiting_recovery':
                wait = self.store.get('recovery_wait', task.get('resume_token', '')) or {}
                dependency = self.store.get('dependency_wait', wait.get('dependency_wait_id', '')) or {}
                evidence = self.store.get('recovery_evidence', wait.get('evidence_job_id', '')) or {}
                plan = self.store.get('decomposition_plan', wait.get('plan_id', '')) or {}
                children = [t for t in self.store.list('task', rid) if t.get('payload', {}).get('parent_task_id') == task['id'] or
                    t['id'] in {dependency.get('boss_task_id'), evidence.get('child_task_id')} or
                    t.get('payload', {}).get('plan_id') == plan.get('id', '__missing__') or
                    t.get('payload', {}).get('line_id') in dependency.get('dependency_line_ids', [])]
                if any(t['id'] in active_tasks for t in children):
                    return {'state': 'waiting_dependency', 'task_id': task['id']}
        for outbox in self.store.list('outbox', rid):
            attempted = parse_time(outbox.get('dispatch_attempted_at'))
            if outbox.get('status') == 'pending' and attempted and self.clock() - attempted < timedelta(seconds=30):
                cycle = self.store.get('cycle', outbox.get('payload', {}).get('cycle_id', '')) or {}
                if cycle.get('line_id') == lid:
                    return {'state': 'delivering', 'outbox_id': outbox['id']}
        # A pending/running label alone does not establish activity. The Worker
        # gets another scheduling pass before the second independent inspection.
        return {'state': 'stalled', 'reason': 'no_live_step', 'tasks': [
            {key: t.get(key) for key in ('id', 'version', 'kind', 'status', 'error')} for t in tasks],
            'outboxes': [o['id'] for o in self.store.list('outbox', rid) if o.get('status') == 'pending'],
            'line_phase': line.get('phase')}

    def _assert_scope(self, connection, event, lease_token=None):
        rid = event['research_id']
        current = self.store._get(connection, 'watchdog_event', event['id'])
        project = self.store._project(connection, rid)
        line = self.store._get(connection, 'line', rid + ':' + event['line_id'])
        token = lease_token if lease_token is not None else event.get('lease_token')
        if (not self.allowed(project) or not self.control.allows_progress(rid, project['control_generation']) or not line or
                line.get('finish_requested') or line['direction_epoch'] != event['direction_epoch'] or
                line.get('cycle_id') != event.get('cycle_id') or not current or
                token is not None and current.get('lease_token') != token):
            raise ResearchStopped('Watch Dog 原诊断范围或租约已失效')

    def _actual_progress(self, operation):
        if operation.get('status') != 'applied':
            return False
        if operation.get('consumed_by'):
            return True
        action = operation.get('action', {}).get('action')
        before, after = operation.get('before', {}), operation.get('after', {})
        if action == 'reconcile_run':
            return (after.get('state') in {'completed', 'succeeded', 'failed', 'execution_failed', 'early_stopped', 'cancelled', 'terminated', 'interrupted'}
                    and before.get('state') != after.get('state'))
        if action == 'redeliver_handoff':
            rid = operation['research_id']
            inboxes = self.store.list('inbox', rid, outbox_id=before['id'])
            if any(inbox.get('consumed_at') or inbox.get('consumed_by') for inbox in inboxes):
                return True
            tasks = [task for task in self.store.list('task', rid) if task.get('outbox_id') == before['id']]
            return any(task.get('attempt', 0) > 0 and task['status'] in {'running', 'completed'} for task in tasks)
        if action in {'resume_task', 'restore_line_task'}:
            task = self.store.get('task', after.get('id', '')) or {}
            return (task.get('attempt', 0) > after.get('attempt', 0) and task.get('status') in {'running', 'completed'})
        return False

    async def poll(self, active_tasks=()):
        for key, task in list(self.active.items()):
            if task.done():
                try:
                    await task
                except Exception as error:
                    with self.store.connect(True) as c:
                        current = self.store._get(c, 'watchdog_event', key)
                        if current and current.get('lease_token') == getattr(task, 'watchdog_lease_token', None) and current.get('status') == 'diagnosing':
                            event = self.store._put(c, 'watchdog_event', key, {**current, 'status': 'needs_attention', 'error': str(error)}, current['research_id'])
                            self.store._emit(c, event['research_id'], 'watchdog.diagnostic_failed', {'event': event}, line_id=event['line_id'])
                self.active.pop(key, None)
        for project in self.store.projects():
            if not self.allowed(project) or not await asyncio.to_thread(self.control.allows_progress, project['id'], project['control_generation']):
                continue
            rid = project['id']
            interval = project['config_snapshot']['team'].get('watchdog_interval_minutes', 10)
            with self.store.connect(True) as c:
                schedule = self.store._get(c, 'watchdog_schedule', rid)
                if schedule and parse_time(schedule['next_due_at']) > self.clock():
                    continue
                if not schedule:
                    self.store._put(c, 'watchdog_schedule', rid, {'next_due_at': (self.clock() + timedelta(minutes=interval)).isoformat()}, rid)
                    continue
                self.store._put(c, 'watchdog_schedule', rid, {'last_checked_at': self.clock().isoformat(),
                    'next_due_at': (self.clock() + timedelta(minutes=interval)).isoformat()}, rid)
            # All runtime facts are reread on each due inspection; a missed
            # interval after restart produces one check, never a catch-up burst.
            runner_state, resources = await asyncio.gather(asyncio.to_thread(self.runner.status), asyncio.to_thread(self.runner.resources))
            for line in self.store.lines(rid):
                observation = self.inspect(project, line, active_tasks, runner_state, resources)
                pending = [e for e in self.store.list('watchdog_event', rid) if e['line_id'] == line['line_id']
                           and e['direction_epoch'] == line['direction_epoch'] and e.get('cycle_id') == line.get('cycle_id')
                           and e['status'] != 'repaired']
                for event in list(pending):
                    operations = [self.store.get('watchdog_action', aid) or {} for aid in event.get('action_ids', [])]
                    progress = next((op for op in operations if self._actual_progress(op)), None)
                    if progress:
                        updated = self.store.patch('watchdog_event', event['id'], status='repaired', verified_progress=progress['id'])
                        self.store.emit(rid, 'watchdog.progress_verified', {'event': updated}, line_id=line['line_id'])
                        pending.remove(event)
                if observation['state'] != 'stalled':
                    continue
                # Recheck scheduler/Runner after yielding, before claiming a blockage.
                await asyncio.sleep(0)
                fresh = self.store.project(rid)
                runner_again = await asyncio.to_thread(self.runner.status)
                if not self.allowed(fresh) or self.inspect(fresh, line, active_tasks, runner_again, resources)['state'] != 'stalled':
                    continue
                event_id = pending[0]['id'] if pending else 'watchdog-' + digest([rid, line['line_id'], line['direction_epoch'], line.get('cycle_id'), observation])[:32]
                with self.store.connect(True) as c:
                    event = self.store._get(c, 'watchdog_event', event_id)
                    if event and event.get('status') == 'diagnosing' and event_id in self.active:
                        continue
                    if not event:
                        event = self.store._put(c, 'watchdog_event', event_id, {'line_id': line['line_id'], 'direction_epoch': line['direction_epoch'],
                            'cycle_id': line.get('cycle_id'), 'observation': observation, 'status': 'pending', 'action_ids': [], 'diagnosis_round': 0}, rid)
                        self.store._emit(c, rid, 'watchdog.blocked', {'watchdog_event_id': event_id, 'observation': observation, 'event': event}, line_id=line['line_id'])
                    # Cross-poller claim is durable. Startup separately reconciles
                    # executions before releasing an abandoned diagnostic lease.
                    if event.get('status') == 'diagnosing':
                        executions = [x for x in self.store.list('execution', rid) if x.get('task_id') == event_id]
                        if any(x.get('status') in {'registered', 'running', 'unknown'} for x in executions):
                            continue
                        if parse_time(event.get('lease_until')) and parse_time(event['lease_until']) > self.clock():
                            continue
                    event = self.store._put(c, 'watchdog_event', event_id, {**event, 'status': 'diagnosing',
                        'lease_token': secrets.token_hex(16),
                        'lease_until': (self.clock() + timedelta(minutes=interval)).isoformat()}, rid)
                    self.store._emit(c, rid, 'watchdog.diagnosing', {'event': event}, line_id=line['line_id'])
                self.active[event_id] = asyncio.create_task(self.diagnose(event))
                self.active[event_id].watchdog_lease_token = event['lease_token']

    def _diagnostic_current(self, event):
        current = self.store.get('watchdog_event', event['id']) or {}
        return current.get('status') == 'diagnosing' and current.get('lease_token') == event.get('lease_token')

    def _commit_diagnostic(self, event, **changes):
        """Only the diagnostic lease that observed this scope can finish it."""
        with self.store.connect(True) as connection:
            current = self.store._get(connection, 'watchdog_event', event['id']) or {}
            if current.get('status') != 'diagnosing' or current.get('lease_token') != event.get('lease_token'):
                return None
            try:
                self._assert_scope(connection, event, event.get('lease_token'))
            except ResearchStopped:
                return None
            return self.store._put(connection, 'watchdog_event', event['id'], {**current, **changes}, event['research_id'])

    async def diagnose(self, event):
        from backend.runtime.generation_budget import GenerationBudget
        rid, eid = event['research_id'], event['id']
        project = self.store.project(rid)
        if not self.allowed(project):
            return
        conversation = eid + ':watchdog'
        self.store.create_conversation(rid, conversation, title='Watch Dog 诊断与在线修复', kind='watchdog', participants=[{'type': 'agent', 'id': 'watchdog'}], line_id=event['line_id'])
        round_no = event.get('diagnosis_round', 0) + 1
        binding = {'kind': 'watchdog_generation', 'key': [eid, round_no], 'limit': 3}
        budget = GenerationBudget(self.store, rid, binding)
        from backend.graphs.engine import god_prompt_context
        context = {'event': event, 'project_state': god_prompt_context(self.snapshot(rid)),
                   'read_instruction': '完整项目记录使用 read_watchdog_state(section, offset) 分页，snapshot_manifest 的 section 名即读取参数。'}
        adapter = self.engine.demo if project['mode'] == 'demo' else self.engine.live
        prompt = WATCHDOG_PROMPT + '\n' + json.dumps(context, ensure_ascii=False, default=str) + '\n' + json.dumps(WatchdogDecision.model_json_schema(), ensure_ascii=False)
        if budget.corrections():
            prompt += '\n先前诊断回复未获采纳，请根据字段错误纠正完整 JSON：' + json.dumps(budget.corrections(), ensure_ascii=False)
        from backend.adapters.token_limits import MAX_OUTPUT_TOKENS, output_tokens
        token_limit = output_tokens()
        while True:
            attempt, raw = None, None
            try:
                if not self._diagnostic_current(event):
                    return
                latest = self.store.project(rid)
                latest_line = self.store.get('line', rid + ':' + event['line_id'])
                if (not self.allowed(latest) or not self.control.allows_progress(rid, latest['control_generation']) or
                        not latest_line or latest_line.get('finish_requested') or
                        latest_line['direction_epoch'] != event['direction_epoch'] or latest_line.get('cycle_id') != event.get('cycle_id')):
                    raise ResearchStopped('Watch Dog 诊断范围已停止或失效')
                attempt, cached = budget.acquire('watchdog', eid, 'diagnose-' + str(round_no))
                raw = cached if cached is not None else await adapter.execute_turn(research_id=rid, agent_id='watchdog', role='watchdog', task_id=eid,
                    turn_id=attempt['turn_id'], schema=WatchdogDecision, prompt=prompt, context=context, direction_epoch=event['direction_epoch'], model=project['config_snapshot']['provider'],
                    runtime_context={'conversation_id': conversation, 'watchdog_event_id': eid,
                        'watchdog_lease_token': event.get('lease_token'), 'line_id': event['line_id'], 'max_output_tokens': token_limit})
                budget.record(attempt, 'returned', output=raw)
                decision = WatchdogDecision.model_validate(raw).model_dump()
                budget.record(attempt, 'accepted', output=decision)
                break
            except (ConnectionError, TimeoutError) as error:
                if attempt:
                    budget.release_unstarted_service_failure(attempt, error)
                raise
            except RuntimeBlocked as error:
                if error.code == 'generation_budget_exhausted':
                    updated = self._commit_diagnostic(event, status='needs_attention', error={
                        'code': error.code, 'message': 'Watch Dog 诊断回复纠正失败，三次生成已耗尽',
                        'attempts': budget.corrections()})
                    if updated:
                        self.store.emit(rid, 'watchdog.diagnostic_exhausted', {'event': updated}, line_id=event['line_id'])
                    return
                if error.code == 'execution_unknown':
                    raise
                if attempt:
                    unstarted = error.code in {'rate_limited', 'model_transport_error', 'model_timeout'} and budget.release_unstarted_service_failure(attempt, error)
                    if not unstarted:
                        budget.record(attempt, 'rejected', error=str(error), error_code=error.code)
                if error.code not in {'invalid_output', 'model_response_invalid', 'model_output_truncated'}:
                    raise
                if error.code == 'model_output_truncated':
                    token_limit = min(MAX_OUTPUT_TOKENS, token_limit * 2)
                prompt += '\n上次诊断未获采纳，请纠正并返回完整 JSON：' + str(error)
            except ValueError as error:
                if attempt:
                    budget.record(attempt, 'rejected', output=raw, error=str(error))
                prompt += '\n上次诊断字段校验失败，请纠正并返回完整 JSON：' + str(error)
        if not self._diagnostic_current(event):
            return
        self.store.create_message(rid, conversation, 'watchdog', decision['content_markdown'], source_event_id=eid + ':' + str(round_no), kind='watchdog')
        applied = []
        for action in decision['actions']:
            if not self._diagnostic_current(event):
                return
            applied.append(await asyncio.to_thread(self.apply, eid, action, event.get('lease_token')))
        action_ids = [a['id'] for a in self.store.list('watchdog_action', rid, event_id=eid)]
        updated = self._commit_diagnostic(event, status='awaiting_progress', diagnosis_round=round_no,
                         action_ids=list(dict.fromkeys(event.get('action_ids', []) + action_ids)))
        if updated:
            self.store.emit(rid, 'watchdog.diagnosed', {'event': updated}, line_id=event['line_id'])

    def apply(self, event_id, action, lease_token=None):
        event = self.store.get('watchdog_event', event_id)
        rid = event['research_id']
        project = self.store.project(rid)
        line = self.store.get('line', rid + ':' + event['line_id'])
        if (not self.allowed(project) or not self.control.allows_progress(rid, project['control_generation']) or not line
                or line.get('finish_requested') or line['direction_epoch'] != event['direction_epoch'] or line.get('cycle_id') != event.get('cycle_id')):
            raise ResearchStopped('Watch Dog 修复范围已停止或失效')
        action = WatchdogAction.model_validate(action).model_dump()
        aid = 'watchdog-action-' + digest([event_id, action])[:32]
        prior = self.store.get('watchdog_action', aid)
        if prior and prior['status'] == 'applied':
            return prior
        kind = {'inject_prompt': 'task', 'resume_task': 'task', 'restore_line_task': 'line', 'redeliver_handoff': 'outbox', 'reconcile_run': 'run'}[action['action']]
        target = self.store.get(kind, action['target_id'])
        if not target or target.get('research_id') != rid:
            raise ValueError('Watch Dog target must exist in the current project')
        if kind == 'task' and target['kind'] == 'watchdog':
            raise ValueError('Watch Dog cannot repair itself')
        with self.store.connect(True) as c:
            self._assert_scope(c, event, lease_token)
            operation = self.store._get(c, 'watchdog_action', aid)
            if operation and operation['status'] == 'applied':
                return operation
            if not operation:
                operation = self.store._put(c, 'watchdog_action', aid, {'event_id': event_id, 'action': action, 'before': target, 'status': 'pending'}, rid)
        if kind == 'line':
            if target['id'] != line['id'] or not line.get('cycle_id'):
                raise ValueError('恢复研究线必须有原方向和周期检查点绑定')
            with self.store.connect(True) as c:
                self._assert_scope(c, event, lease_token)
                current_project = self.store._project(c, rid)
                current_line = self.store._get(c, 'line', line['id'])
                if (not self.allowed(current_project) or not self.control.allows_progress(rid, project['control_generation']) or
                        current_line.get('finish_requested') or current_line['direction_epoch'] != event['direction_epoch'] or
                        current_line.get('cycle_id') != event.get('cycle_id')):
                    raise ResearchStopped('恢复提交前范围已失效')
                existing = [t for t in self.store.list('task', rid) if t['kind'] == 'line' and
                    t.get('payload', {}).get('line_id') == line['line_id'] and
                    t['payload'].get('cycle_id') == line['cycle_id'] and t['payload'].get('direction_epoch') == line['direction_epoch']]
                if existing:
                    raise ValueError('原任务仍存在，使用 resume_task 接续原身份')
                task_id = 'watchdog-restored-' + digest([rid, line['line_id'], line['direction_epoch'], line['cycle_id']])[:32]
                after = self.store._get(c, 'task', task_id) or self.store._put(c, 'task', task_id, {'kind': 'line', 'status': 'pending',
                    'owner': None, 'attempt': 0, 'control_generation': project['control_generation'],
                    'payload': {key: line[key] for key in ('line_id', 'cycle_id', 'direction_epoch')}}, rid)
                self.store._put(c, 'watchdog_action', aid, {**operation, 'status': 'applied', 'after': after}, rid)
                if current_project['status'] == 'blocked':
                    self.store._update_project(c, rid, status='running', blocked_reason=None, error=None)
        elif kind == 'task':
            with self.store.connect(True) as c:
                self._assert_scope(c, event, lease_token)
                operation = self.store._get(c, 'watchdog_action', aid)
                if operation['status'] == 'applied':
                    return operation
                project = self.store._project(c, rid)
                if not self.allowed(project) or not self.control.allows_progress(rid, project['control_generation']):
                    raise ResearchStopped('修复提交前研究已停止')
                current = self.store._get(c, kind, target['id'])
                target_line = self.store._get(c, 'line', rid + ':' + current.get('payload', {}).get('line_id', ''))
                if target_line and (target_line.get('finish_requested') or
                        current['payload'].get('direction_epoch', target_line['direction_epoch']) != target_line['direction_epoch'] or
                        current['payload'].get('cycle_id', target_line.get('cycle_id')) != target_line.get('cycle_id')):
                    raise ResearchStopped('原任务方向或周期已失效')
                if current['status'] in {'completed', 'cancelled'}:
                    after = current
                else:
                    # A running execution must be reconciled before any resumption.
                    executions = [x for x in self.store.list('execution', rid) if x.get('task_id', '').startswith(current['id'])]
                    if any(x.get('status') in {'running', 'registered', 'unknown'} for x in executions):
                        raise ValueError('原执行尚未确认退出，先对账')
                    prompts = list(current.get('watchdog_prompts', []))
                    if action['action'] == 'inject_prompt' and not any(p['operation_id'] == aid for p in prompts):
                        if not action['content_markdown'].strip():
                            raise ValueError('注入必须说明具体字段或修订要求')
                        prompts.append({'operation_id': aid, 'content_markdown': action['content_markdown']})
                    after = self.store._put(c, 'task', current['id'], {**current, 'status': 'pending', 'owner': None,
                        'error': None, 'watchdog_prompts': prompts}, rid)
                self.store._put(c, 'watchdog_action', aid, {**operation, 'status': 'applied', 'after': after}, rid)
                if project['status'] == 'blocked' and project.get('blocked_reason') == 'all_lines_blocked':
                    self.store._update_project(c, rid, status='running', blocked_reason=None, error=None)
        elif kind == 'run':
            after = self.runner.status(target.get('experiment_run_id', target['id']))
            if after.get('state') == 'unknown' or after.get('research_id') != rid:
                raise ValueError('Runner 尚未确认运行身份或真实终态')
            with self.store.connect(True) as c:
                self._assert_scope(c, event, lease_token)
                after = self.store._put(c, 'run', target['id'], after, rid)
                self.store._put(c, 'watchdog_action', aid, {**operation, 'status': 'applied', 'after': after}, rid)
        else:
            self.store.dispatch_outbox(self.control, 100, rid, only_outbox_id=target['id'],
                scope_guard=lambda connection: self._assert_scope(connection, event, lease_token))
            after = self.store.get('outbox', target['id'])
            with self.store.connect(True) as c:
                self._assert_scope(c, event, lease_token)
                self.store._put(c, 'watchdog_action', aid, {**operation, 'status': 'applied', 'after': after}, rid)
        self.store.emit(rid, 'watchdog.action_applied', {'operation_id': aid, 'action': action,
            'operation': self.store.get('watchdog_action', aid)}, line_id=event['line_id'])
        return self.store.get('watchdog_action', aid)

    async def close(self):
        for task in self.active.values():
            task.cancel()
        await asyncio.gather(*self.active.values(), return_exceptions=True)
