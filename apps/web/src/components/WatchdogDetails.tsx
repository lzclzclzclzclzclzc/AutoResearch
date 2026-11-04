import type { Data } from '../api';
import { time } from '../api';
import { Empty } from '../App';
import { watchdogAction, watchdogStatus } from '../watchdog';

export default function WatchdogDetails({ events, actions, choose }: { events: Data[]; actions: Data[]; choose: (id: string) => void }) {
  if (!events.length) return <Empty title="尚无停滞诊断记录">Watch Dog 按配置定时巡检，发现研究线停滞后在这里记录诊断与修复。</Empty>;
  return <div className="trace-scroll" aria-label="Watch Dog 诊断与修复记录">{[...events].reverse().map(record => {
    const id = record.id || record.event_id;
    const operations = actions.filter(action => action.event_id === id || record.action_ids?.includes(action.id));
    return <details className="trace" key={id}>
      <summary><span>{String(record.line_id || '全局').replace('line-', '线 ')} · 方向 {record.direction_epoch || 1}</span><span>{watchdogStatus[record.status] || record.status}</span></summary>
      {record.created_at && <p className="small muted">{time(record.created_at)}</p>}
      <p>{record.observation?.reason === 'no_live_step' ? '没有可接续的有效步骤，已检查该线任务与实验状态。' : record.observation?.reason || '已登记需要诊断的停滞。'}</p>
      <button className="text-button" onClick={() => choose(id + ':watchdog')}>查看诊断会话</button>
      {record.error && <p className="notice error">{typeof record.error === 'string' ? record.error : record.error.message || JSON.stringify(record.error)}</p>}
      {operations.map(operation => <details key={operation.id}>
        <summary>{watchdogAction[operation.action?.action] || operation.action?.action || '修复操作'} · {operation.status === 'applied' ? '已执行' : '待执行'}</summary>
        <p className="small muted">操作 {operation.id} · 目标 {operation.action?.target_id}</p>
        {operation.action?.content_markdown && <p>{operation.action.content_markdown}</p>}
        {operation.consumed_by && <p>原 Agent 已读取修订提示。</p>}
        <details><summary>操作前后的记录</summary><pre>{JSON.stringify({before:operation.before,after:operation.after},null,2)}</pre></details>
      </details>)}
      {record.status === 'repaired' && <p>已核对后续实际进展。</p>}
      <details><summary>诊断现场与验证记录</summary><pre>{JSON.stringify(record,null,2)}</pre></details>
    </details>;
  })}</div>;
}
