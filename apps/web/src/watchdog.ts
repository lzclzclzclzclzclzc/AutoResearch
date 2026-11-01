import type { Data } from './api';

export const watchdogStatus: Record<string,string> = {
  pending: '待诊断', diagnosing: '诊断中', awaiting_progress: '等待实际进展',
  repaired: '已验证修复', needs_attention: '需要处理', cancelled: '已取消',
};
export const watchdogAction: Record<string,string> = {
  inject_prompt: '向原 Agent 补充修订提示', resume_task: '接续原任务', restore_line_task: '恢复研究线检查点',
  redeliver_handoff: '补投已提交的交接', reconcile_run: '核对实验真实状态',
};

/** A replay never borrows the current repair result or operation receipts. */
export function projectWatchdog(events: Data[], seq: number): { watchdog_events: Data[]; watchdog_actions: Data[] } {
  const records = new Map<string,Data>();
  const operations = new Map<string,Data>();
  for (const event of events.filter(event => event.seq <= seq).sort((a,b) => a.seq-b.seq)) {
    if (!event.type?.startsWith('watchdog.')) continue;
    const record = event.payload?.event;
    const id = record?.id || record?.event_id;
    if (id) {
      const prior = records.get(id);
      records.set(id, {...record,created_at:prior?.created_at || event.occurred_at,
        created_event_seq:prior?.created_event_seq || event.seq,last_activity_at:event.occurred_at,last_activity_seq:event.seq});
    }
    const operation = event.payload?.operation;
    if (event.type === 'watchdog.action_applied' && operation?.id) {
      operations.set(operation.id, {...operation,applied_at:event.occurred_at,applied_event_seq:event.seq});
    }
  }
  return {watchdog_events:[...records.values()],watchdog_actions:[...operations.values()]};
}
