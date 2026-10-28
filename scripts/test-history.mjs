import assert from 'node:assert/strict';
import test from 'node:test';
import { projectHistory } from '../apps/web/src/history.ts';

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
