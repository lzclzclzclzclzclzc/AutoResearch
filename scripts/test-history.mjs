import assert from 'node:assert/strict';
import test from 'node:test';
import { projectHistory } from '../apps/web/src/history.ts';
import { arbitrationPhase } from '../apps/web/src/arbitrations.ts';

const t = s => `2026-10-25T00:00:${String(s).padStart(2,'0')}Z`;
const events = [
  {seq:1,type:'research.started',occurred_at:t(1),payload:{}},
  {seq:2,type:'agent.started',execution_id:'e-1',agent_id:'line-01/implementation',occurred_at:t(2),payload:{}},
  {seq:3,type:'experiment.started',run_id:'r',occurred_at:t(3),payload:{}},
  {seq:4,type:'submission.created',occurred_at:t(6),payload:{submission_id:'s'}},
  {seq:5,type:'agent.completed',execution_id:'e-1',agent_id:'line-01/implementation',occurred_at:t(7),payload:{}},
  {seq:6,type:'verification.completed',line_id:'line-01',occurred_at:t(8),payload:{submission_version_id:'s',run_ids:['r'],passed:true,round_no:1}},
  {seq:7,type:'report.completed',occurred_at:t(9),payload:{report_id:'p'}},
];
const snapshot = {
  project:{created_at:t(0),status:'completed'},
  topology:{nodes:[{id:'line-01/implementation',data:{status:'completed',message_count:500}}],edges:[]},
  agents:[{agent_id:'line-01/implementation',status:'completed'}],
  executions:[{id:'e',execution_attempt_id:'e-1',agent_id:'line-01/implementation',started_at:t(2),status:'completed',output:'future conclusion'}],
  lines:[{line_id:'line-01',phase:'completed',completed_verify_rounds:10}],
  runs:[{id:'r',started_at:t(3),ended_at:t(5),status:'completed',exit_code:0,scientific_observation:'future result',verification_status:'passed'}],
  submissions:[{id:'s',created_event_seq:4,created_at:t(6),verifications:[{passed:true}]}],
  reports:[{id:'p',created_event_seq:7,created_at:t(9)}],
};
test('mid-run replay hides exit, future verification, report and execution output',()=>{
  const p=projectHistory(snapshot,events,3);
  assert.equal(p.project.status,'running');
  assert.equal(p.runs[0].status,'running');
  assert.equal(p.runs[0].exit_code,undefined);
  assert.equal(p.runs[0].scientific_observation,undefined);
  assert.equal(p.runs[0].verification_status,'verification_pending');
  assert.equal(p.submissions.length,0); assert.equal(p.reports.length,0);
  assert.equal(p.executions[0].status,'running'); assert.equal(p.executions[0].output,undefined);
  assert.equal(p.topology.nodes[0].data.message_count,0);
});
test('package replay keeps verification pending until its actual event',()=>{
  assert.equal(projectHistory(snapshot,events,4).submissions[0].verification_status,'verification_pending');
  const verified=projectHistory(snapshot,events,6);
  assert.equal(verified.submissions[0].verification_status,'passed');
  assert.equal(verified.lines[0].completed_verify_rounds,1);
  assert.equal(verified.reports.length,0);
  assert.equal(projectHistory(snapshot,events,7).reports.length,1);
});

test('arbitration replay retains real timing and never leaks a future decision or effect',()=>{
  const arbitrationEvents = [
    {seq:8,type:'arbitration.requested',occurred_at:t(10),payload:{arbitration_id:'arb-1'}},
    {seq:9,type:'arbitration.started',occurred_at:t(11),payload:{arbitration_id:'arb-1',attempt:1}},
    {seq:10,type:'arbitration.retry_wait',occurred_at:t(12),payload:{arbitration_id:'arb-1',retry_not_before:t(13)}},
    {seq:11,type:'arbitration.started',occurred_at:t(13),payload:{arbitration_id:'arb-1',attempt:2}},
    {seq:12,type:'arbitration.decided',occurred_at:t(14),payload:{arbitration_id:'arb-1',decision_id:'decision-1',decision_hash:'hash'}},
    {seq:13,type:'arbitration.applied',occurred_at:t(15),payload:{arbitration_id:'arb-1',effect_id:'effect-1',applied_action:'select_candidate'}},
  ];
  const all = events.concat(arbitrationEvents);
  const live = {...snapshot,topology:{...snapshot.topology,nodes:[...snapshot.topology.nodes,{id:'god',data:{role:'god',active_arbitrations:99,status:'running'}}]},
    arbitrations:[{id:'arb-1',arbitration_id:'arb-1',status:'applied',scope_id:'boss',
      decision:{content_markdown:'future answer'},effects:[{payload:'future business effect'}],
      decision_id:'decision-1',effect_id:'effect-1',error:'future failure',resolution_status:'fulfilled',
      attempts:[{attempt:2}],history:arbitrationEvents.map(event=>({status:event.type.split('.')[1],event_seq:event.seq}))}]};
  assert.deepEqual(projectHistory(live,all,7).arbitrations,[]);
  const pending = projectHistory(live,all,9);
  assert.equal(pending.arbitrations[0].status,'running');
  assert.equal(pending.arbitrations[0].attempts.length,1);
  assert.equal(pending.topology.nodes.find(node=>node.id==='god').data.active_arbitrations,1);
  assert.equal(pending.arbitrations[0].decision,undefined);
  assert.equal(pending.arbitrations[0].effects,undefined);
  assert.equal(pending.arbitrations[0].decision_id,undefined);
  assert.equal(pending.arbitrations[0].error,undefined);
  assert.equal(pending.arbitrations[0].history.length,2);
  assert.equal(projectHistory(live,all,10).arbitrations[0].status,'retry_wait');
  const decided=projectHistory(live,all,12).arbitrations[0];
  assert.equal(decided.status,'decided');
  assert.equal(decided.decision_id,'decision-1');
  assert.equal(decided.effect_id,undefined);
  const applied=projectHistory(live,all,13);
  assert.equal(applied.arbitrations[0].status,'applied');
  assert.equal(applied.arbitrations[0].effect_id,'effect-1');
  assert.equal(applied.topology.nodes.find(node=>node.id==='god').data.active_arbitrations,0);
});

test('recovery child waiting phase only appears between suspension and validated wakeup',()=>{
  const request={id:'arb',arbitration_id:'arb',source_task_id:'root',status:'applied',applied_action:'decompose_task',resolution_status:'fulfilled'};
  const progress=[
    {seq:8,type:'arbitration.requested',occurred_at:t(10),payload:{arbitration_id:'arb'}},
    {seq:9,type:'arbitration.applied',occurred_at:t(11),payload:{arbitration_id:'arb'}},
    {seq:10,type:'task.waiting_recovery',occurred_at:t(12),payload:{arbitration_id:'arb',task_id:'root'}},
    {seq:11,type:'task.recovery_ready',occurred_at:t(13),payload:{task_id:'root'}},
  ];
  const live={...snapshot,arbitrations:[request]};
  assert.equal(projectHistory(live,events.concat(progress),9).arbitrations[0].resolution_status,undefined);
  assert.equal(projectHistory(live,events.concat(progress),10).arbitrations[0].resolution_status,'waiting_children');
  assert.equal(projectHistory(live,events.concat(progress),11).arbitrations[0].resolution_status,'fulfilled');
});

test('capacity waits preserve running status and active count through acquisition',()=>{
  const progress=[
    {seq:8,type:'arbitration.requested',occurred_at:t(10),payload:{arbitration_id:'arb'}},
    {seq:9,type:'arbitration.started',occurred_at:t(11),payload:{arbitration_id:'arb'}},
    {seq:10,type:'arbitration.capacity_wait',occurred_at:t(12),payload:{arbitration_id:'arb'}},
    {seq:11,type:'arbitration.context_read',occurred_at:t(13),payload:{arbitration_id:'arb'}},
    {seq:12,type:'arbitration.capacity_acquired',occurred_at:t(14),payload:{arbitration_id:'arb'}},
    {seq:13,type:'arbitration.decided',occurred_at:t(15),payload:{arbitration_id:'arb'}},
    {seq:14,type:'arbitration.capacity_wait',occurred_at:t(16),payload:{arbitration_id:'arb'}},
  ];
  const live={...snapshot,arbitrations:[{id:'arb',arbitration_id:'arb',status:'applied',execution_phase:'future'}],
    topology:{nodes:[{id:'god',data:{role:'god',active_arbitrations:0}}],edges:[]}};
  const all=events.concat(progress);
  for (const cursor of [10,11]) {
    const waiting=projectHistory(live,all,cursor);
    assert.equal(waiting.arbitrations[0].status,'running');
    assert.equal(waiting.arbitrations[0].execution_phase,'waiting_capacity');
    assert.equal(arbitrationPhase(waiting.arbitrations[0]),'等待模型容量');
    assert.equal(waiting.topology.nodes[0].data.active_arbitrations,1);
  }
  const judging=projectHistory(live,all,12).arbitrations[0];
  assert.equal(judging.status,'running');
  assert.equal(judging.execution_phase,'judging');
  assert.equal(arbitrationPhase(judging),'God 正在仲裁');
  const decided=projectHistory(live,all,14).arbitrations[0];
  assert.equal(decided.status,'decided');
  assert.equal(decided.execution_phase,undefined);
});

test('internal dependency waits and cancellation replay without future project blocking',()=>{
  const request={id:'arb',arbitration_id:'arb',source_task_id:'root',status:'applied',resolution_status:'cancelled'};
  const progress=[
    {seq:8,type:'arbitration.requested',occurred_at:t(10),payload:{arbitration_id:'arb'}},
    {seq:9,type:'arbitration.applied',occurred_at:t(11),payload:{arbitration_id:'arb'}},
    {seq:10,type:'dependency.waiting',occurred_at:t(12),payload:{arbitration_id:'arb',task_id:'root'}},
    {seq:11,type:'dependency.ready',occurred_at:t(13),payload:{arbitration_id:'arb',task_id:'root'}},
    {seq:12,type:'dependency.cancelled',occurred_at:t(14),payload:{arbitration_id:'arb',task_id:'root'}},
    {seq:13,type:'research.blocked',occurred_at:t(15),payload:{reason:'all_lines_blocked',error:{code:'all_lines_blocked'}}},
    {seq:14,type:'research.inputs_changed',occurred_at:t(16),payload:{}},
    {seq:15,type:'research.resumed',occurred_at:t(17),payload:{reason:'automatic_recovery'}},
  ];
  const live={...snapshot,project:{...snapshot.project,status:'blocked',error:{code:'future error'}},arbitrations:[request]};
  const all=events.concat(progress);
  const wait=projectHistory(live,all,10);
  assert.equal(wait.project.status,'running');
  assert.equal(wait.project.error,undefined);
  assert.equal(arbitrationPhase(wait.arbitrations[0]),'等待内部依赖，由 Boss 协调');
  assert.equal(projectHistory(live,all,11).arbitrations[0].resolution_status,'fulfilled');
  assert.equal(arbitrationPhase(projectHistory(live,all,12).arbitrations[0]),'恢复已取消');
  const blocked=projectHistory(live,all,14);
  assert.equal(blocked.project.status,'blocked');
  assert.equal(blocked.project.error.code,'all_lines_blocked');
  const resumed=projectHistory(live,all,15);
  assert.equal(resumed.project.status,'running');
  assert.equal(resumed.project.error,undefined);
});
