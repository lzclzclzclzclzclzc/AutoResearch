import assert from 'node:assert/strict';
import test from 'node:test';
import { projectHistory } from '../apps/web/src/history.ts';
import { projectWatchdog } from '../apps/web/src/watchdog.ts';

const base = {id:'wd-1',line_id:'line-02',direction_epoch:1};
const operation = {id:'op-1',event_id:'wd-1',status:'applied',action:{action:'resume_task',target_id:'task-1'},before:{status:'failed'},after:{status:'pending'}};
const events = [
  {seq:2,type:'watchdog.blocked',occurred_at:'2026-11-01T01:00:00Z',payload:{event:{...base,status:'pending'}}},
  {seq:3,type:'watchdog.diagnosing',occurred_at:'2026-11-01T01:01:00Z',payload:{event:{...base,status:'diagnosing'}}},
  {seq:4,type:'watchdog.action_applied',occurred_at:'2026-11-01T01:02:00Z',payload:{operation}},
  {seq:5,type:'watchdog.diagnosed',occurred_at:'2026-11-01T01:03:00Z',payload:{event:{...base,status:'awaiting_progress',action_ids:['op-1']}}},
  {seq:6,type:'watchdog.progress_verified',occurred_at:'2026-11-01T01:04:00Z',payload:{event:{...base,status:'repaired',verified_progress:'op-1'}}},
];
test('watchdog history exposes only diagnosis and operations committed by cursor', () => {
  assert.deepEqual(projectWatchdog(events,1),{watchdog_events:[],watchdog_actions:[]});
  const diagnosing=projectWatchdog(events,3);
  assert.equal(diagnosing.watchdog_events[0].status,'diagnosing');
  assert.equal(diagnosing.watchdog_events[0].verified_progress,undefined);
  assert.equal(diagnosing.watchdog_actions.length,0);
  const applied=projectWatchdog(events,4);
  assert.deepEqual(applied.watchdog_actions[0].after,{status:'pending'});
  assert.equal(applied.watchdog_events[0].status,'diagnosing');
  assert.equal(projectWatchdog(events,5).watchdog_events[0].status,'awaiting_progress');
  assert.equal(projectWatchdog(events,6).watchdog_events[0].status,'repaired');
});
test('historical workspace discards current repair facts and retains initial detection time', () => {
  const current={project:{created_at:'2026-11-01T00:00:00Z'},watchdog_events:[{...base,status:'repaired',error:'future'}],watchdog_actions:[operation]};
  const view=projectHistory(current,events,2);
  assert.equal(view.watchdog_events[0].status,'pending');
  assert.equal(view.watchdog_events[0].error,undefined);
  assert.deepEqual(view.watchdog_actions,[]);
  assert.equal(projectHistory(current,events,6).watchdog_events[0].created_at,events[0].occurred_at);
});
