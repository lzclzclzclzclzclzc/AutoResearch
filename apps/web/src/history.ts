import type { Data } from './api';
import { activeArbitration, projectArbitrations } from './arbitrations.ts';
import { projectWatchdog } from './watchdog.ts';

const at = (value: unknown) => typeof value === 'string' ? Date.parse(value) : Number.NaN;
const ident = (x: Data) => x.run_id || x.experiment_run_id || x.id;

function projectLines(lines: Data[], events: Data[]): Data[] {
  const projected = new Map<string,Data>(lines.map((line): [string,Data] => [line.line_id, {
    // Only the line identity and fixed display settings come from the live row.
    ...Object.fromEntries(['id','research_id','line_id','title','k'].filter(key => key in line).map(key => [key,line[key]])),
    phase:'idle',direction_epoch:1,cycle_id:null,completed_verify_rounds:0,
  }]));
  for (const event of events) {
    const payload = event.payload || {};
    const line = projected.get(event.line_id || payload.line_id);
    if (!line) continue;
    if (event.type === 'line.phase_changed') {
      const changes = {...payload};
      for (const field of ['cycle_id','direction_epoch']) {
        if (!(field in changes) && event[field] !== undefined) changes[field] = event[field];
      }
      const directionChanged = changes.direction_epoch !== undefined && changes.direction_epoch !== line.direction_epoch;
      const cycleChanged = 'cycle_id' in changes && changes.cycle_id !== line.cycle_id;
      if (directionChanged || cycleChanged) {
        line.completed_verify_rounds = 0;
        delete line.active_spec_id;
        if (directionChanged && !('cycle_id' in changes)) line.cycle_id = null;
      }
      Object.assign(line,changes);
    } else if (event.type === 'verification.completed') {
      const cycleId = payload.cycle_id ?? event.cycle_id;
      const directionEpoch = payload.direction_epoch ?? event.direction_epoch;
      // Old records may omit scope metadata; explicit old-scope receipts never
      // advance the currently visible cycle, including late recovery receipts.
      if (payload.historical || (cycleId != null && line.cycle_id != null && cycleId !== line.cycle_id) ||
          (directionEpoch != null && directionEpoch !== line.direction_epoch)) continue;
      const round = payload.round_no ?? payload.round;
      if (round !== undefined) line.completed_verify_rounds = round;
    }
  }
  return [...projected.values()];
}

/** Project historical views only from facts already committed at the cursor. */
export function projectHistory(snapshot: Data, allEvents: Data[], seq: number): Data {
  const events = allEvents.filter(e => e.seq <= seq).sort((a,b) => a.seq-b.seq);
  const arbitrations = projectArbitrations(snapshot.arbitrations || [], allEvents, seq);
  const watchdog = projectWatchdog(allEvents, seq);
  const asOf = at(events.at(-1)?.occurred_at || snapshot.project?.created_at);
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
    topology:{...snapshot.topology,nodes,edges},agents,executions,runs,submissions,arbitrations,...watchdog,
    lines:projectLines(snapshot.lines || [],events),
    reports:(snapshot.reports || []).filter((r: Data) => r.created_event_seq ? r.created_event_seq <= seq : at(r.created_at) <= asOf)};
}
