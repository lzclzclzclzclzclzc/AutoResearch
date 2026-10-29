import type { Data } from './api';
import { activeArbitration, projectArbitrations } from './arbitrations.ts';

const at = (value: unknown) => typeof value === 'string' ? Date.parse(value) : Number.NaN;
const ident = (x: Data) => x.run_id || x.experiment_run_id || x.id;

/** Project historical views only from facts already committed at the cursor. */
export function projectHistory(snapshot: Data, allEvents: Data[], seq: number): Data {
  const events = allEvents.filter(e => e.seq <= seq);
  const arbitrations = projectArbitrations(snapshot.arbitrations || [], allEvents, seq);
  const asOf = at(events.at(-1)?.occurred_at || snapshot.project?.created_at);
  const phase = (id: string) => events.filter(e => e.line_id === id && e.type === 'line.phase_changed').at(-1)?.payload || {};
  const executions = (snapshot.executions || []).filter((x: Data) => at(x.started_at || x.created_at) <= asOf).map((x: Data) => {
    const refs = events.filter(e => e.execution_id === (x.execution_attempt_id || x.execution_id || x.id));
    const last = refs.filter(e => /^agent\.(completed|failed|cancelled)$/.test(e.type)).at(-1);
    return { id:x.id,execution_id:x.execution_id,execution_attempt_id:x.execution_attempt_id,agent_id:x.agent_id,task_id:x.task_id,
      started_at:x.started_at,attempt:x.attempt,status:last?.type.split('.')[1] || 'running',events:refs };
  });
  const agents = (snapshot.agents || []).map((a: Data) => ({...a,status:executions.filter((x: Data) => x.agent_id === (a.agent_id || a.id)).at(-1)?.status || 'idle'}));
  const nodes = (snapshot.topology?.nodes || []).map((n: Data) => {
    const messageEvents = events.filter(e => e.type === 'message.completed' && (e.agent_id === n.id || e.agent_id?.startsWith(n.id + '/')));
    return {...n,data:{...n.data,status:'idle',active_arbitrations:n.id === 'god' ? arbitrations.filter(activeArbitration).length : 0,
      message_count:messageEvents.length,latest_message_seq:messageEvents.at(-1)?.seq || 0}};
  });
  const edges = (snapshot.topology?.edges || []).map((e: Data) => ({...e,data:{...e.data,message_count:events.filter(x => x.type === 'message.completed' && x.edge_id === e.id).length}}));
  const runs = (snapshot.runs || []).filter((r: Data) => at(r.started_at) <= asOf).map((r: Data) => {
    const runEvents = events.filter(e => (e.experiment_run_id || e.run_id || ident(e.payload || {})) === ident(r));
    const ended = at(r.ended_at) <= asOf;
    const verification = events.filter(e => e.type === 'verification.completed' && e.payload?.run_ids?.includes(ident(r))).at(-1);
    return { id:r.id,run_id:ident(r),line_id:r.line_id,spec_id:r.spec_id,spec_version:r.spec_version,code_version:r.code_version,
      started_at:r.started_at,status:ended ? r.status || r.state : 'running',
      ...(ended ? {ended_at:r.ended_at,exit_code:r.exit_code,process_tree_exited:r.process_tree_exited} : {}),
      metrics:runEvents.filter(e => e.type === 'experiment.metric').map(e => e.payload),
      verification_status:verification ? verification.payload.passed ? 'passed' : 'rejected' : 'verification_pending' };
  });
  const submissions = (snapshot.submissions || []).filter((s: Data) => s.created_event_seq ? s.created_event_seq <= seq : at(s.created_at) <= asOf).map((s: Data) => {
    const verifications = events.filter(e => e.type === 'verification.completed' && e.payload?.submission_version_id === (s.submission_id || s.id)).map(e => e.payload);
    // The original package is immutable; live projection fields are removed.
    return {...s,verifications,verification_status:verifications.length ? verifications.at(-1).passed ? 'passed' : 'rejected' : 'verification_pending'};
  });
  const lifecycleEvent = events.filter(e => /^research\.(created|started|resumed|paused|pausing|blocked|terminating|terminated|completed)$/.test(e.type)).at(-1);
  const lifecycle = lifecycleEvent?.type.split('.')[1] || 'created';
  const status = ['started','resumed'].includes(lifecycle) ? 'running' : lifecycle;
  return {...snapshot,project:{...snapshot.project,status,
      error:status === 'blocked' ? lifecycleEvent?.payload?.error : undefined,
      blocked_reason:status === 'blocked' ? lifecycleEvent?.payload?.reason : undefined},snapshot_seq:seq,recent_events:events,
    topology:{...snapshot.topology,nodes,edges},agents,executions,runs,submissions,arbitrations,
    lines:(snapshot.lines || []).map((l: Data) => {
      const state = phase(l.line_id); const verify = events.filter(e => e.line_id === l.line_id && e.type === 'verification.completed').at(-1)?.payload;
      return {...l,...state,phase:state.phase || 'idle',direction_epoch:state.direction_epoch || 1,completed_verify_rounds:verify?.round_no || verify?.round || 0};
    }),
    reports:(snapshot.reports || []).filter((r: Data) => r.created_event_seq ? r.created_event_seq <= seq : at(r.created_at) <= asOf)};
}
