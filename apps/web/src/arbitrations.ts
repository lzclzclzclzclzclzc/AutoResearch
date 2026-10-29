import type { Data } from './api';

export const arbitrationStatuses: Record<string, string> = {
  requested: '等待模型容量', running: 'God 正在仲裁', retry_wait: '等待仲裁重试',
  decided: '已裁决，等待应用', applying: '正在应用仲裁', applied: '已采用', stale: '现场已变化',
  cancelled: '已取消', exhausted: '自动恢复已耗尽', needs_external_input: '缺少外部条件',
};
export const arbitrationActions: Record<string, string> = {
  select_candidate: '选定候选', repair_output: '修复输出', request_revision: '修订任务',
  request_evidence: '补充证据', decompose_task: '拆分任务', finish_scope: '收束当前范围', needs_external_input:'补充外部条件',
};
export const arbitrationReasons: Record<string, string> = {
  decision_blocked:'聚焦讨论后投票仍无严格多数', design_deadlock:'审查或设计修订未取得进展',
  invalid_group_decision:'主持人总结与已选候选不一致', invalid_boss_assignment:'Boss 分派缺项或身份不合法',
  invalid_output:'输出经常规纠正后仍不符合要求', model_output_truncated:'输出达到上限，需拆分执行',
};
export const activeArbitration = (request: Data) => ['requested', 'running', 'retry_wait', 'decided', 'applying'].includes(request.status);
const transitionStatuses: Record<string, string> = {
  'arbitration.requested':'requested','arbitration.started':'running','arbitration.retry_wait':'retry_wait',
  'arbitration.decided':'decided','arbitration.applying':'applying','arbitration.applied':'applied',
  'arbitration.stale':'stale','arbitration.cancelled':'cancelled','arbitration.exhausted':'exhausted',
  'arbitration.needs_external_input':'needs_external_input',
};
const capacityPhases: Record<string, string> = {'arbitration.capacity_wait':'waiting_capacity','arbitration.capacity_acquired':'judging'};

/** Reconstruct arbitration state from committed transitions, never current live fields. */
export function projectArbitrations(requests: Data[], events: Data[], seq: number): Data[] {
  return requests.flatMap(request => {
    const id = request.arbitration_id || request.id;
    const refs = events.filter(event => event.seq <= seq && event.type.startsWith('arbitration.') && event.payload?.arbitration_id === id);
    const transitions = refs.filter(event => event.type in transitionStatuses);
    const history = (request.history || []).filter((entry: Data) => entry.event_seq && entry.event_seq <= seq);
    if (!transitions.length && !history.length) return [];
    const last = transitions.at(-1);
    const status = last ? transitionStatuses[last.type] : history.at(-1).status;
    const result: Data = {};
    for (const field of ['id','arbitration_id','research_id','source_task_id','scope_kind','scope_id','origin_node','reason_code',
      'scope_revision','direction_epoch','snapshot_id','snapshot_hash','target_schema_name','target_schema_hash',
      'recovery_chain_id','created_at','judge_deadline_at','expires_at','conversation_id','line_id','requested_event_seq']) {
      if (field in request) result[field] = request[field];
    }
    result.status = status;
    if (status === 'running') {
      const phase = [...refs].reverse().find(event => event.type in capacityPhases && event.seq > (last?.seq || 0));
      result.execution_phase = phase ? capacityPhases[phase.type] : 'judging';
    }
    result.history = history;
    result.latest_event_seq = refs.at(-1)?.seq || history.at(-1).event_seq;
    result.attempts = transitions.filter(event => ['arbitration.started','arbitration.retry_wait'].includes(event.type))
      .map(event => ({type:event.type,event_seq:event.seq,occurred_at:event.occurred_at,...event.payload}));
    for (const state of ['decided','applied']) {
      const event = [...transitions].reverse().find(event => event.type === `arbitration.${state}`);
      if (event) {
        result[`${state}_event_seq`] = event.seq;
        for (const field of ['decision_id','decision_hash','effect_id','applied_action']) if (field in event.payload) result[field] = event.payload[field];
      }
    }
    const recovery = events.filter(event => event.seq <= seq && /^(task\.(waiting_recovery|recovery_ready|decomposed|reassembled)|dependency\.(waiting|ready|coordination_requested|cancelled))$/.test(event.type) &&
      (event.payload?.arbitration_id === id || event.payload?.task_id === request.source_task_id));
    if (recovery.length) result.resolution_status = recovery.at(-1)!.type === 'dependency.cancelled' ? 'cancelled' : ['dependency.waiting','dependency.coordination_requested'].includes(recovery.at(-1)!.type) ? 'waiting_dependencies' : ['task.waiting_recovery','task.decomposed'].includes(recovery.at(-1)!.type) ? 'waiting_children' : 'fulfilled';
    if (result.applied_event_seq && request.applied_action) result.applied_action = request.applied_action;
    return [result];
  });
}

export function arbitrationPhase(request: Data): string {
  if (['cancelled','stale','exhausted'].includes(request.status)) return arbitrationStatuses[request.status];
  if (request.resolution_status === 'cancelled') return '恢复已取消';
  if (request.resolution_status === 'waiting_dependencies') return '等待内部依赖，由 Boss 协调';
  if (request.status === 'running' && request.execution_phase === 'waiting_capacity') return '等待模型容量';
  if (request.status === 'applied' && request.resolution_status === 'waiting_children')
    return request.applied_action === 'decompose_task' ? '正在执行拆分任务' : '正在按仲裁修订';
  return arbitrationStatuses[request.status] || request.status || '—';
}
