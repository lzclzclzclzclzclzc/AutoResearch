import assert from 'node:assert/strict';
import test from 'node:test';
import { deduplicateGodMessages, godMessageValidation, projectArbitrations } from '../apps/web/src/arbitrations.ts';

const message = (id, source, body='同一回复') => ({message_id:id, sender:{id:'god/arb'}, conversation_id:'run:god:arb',
  source_system:source, content_markdown:body});

test('pair legacy stream/final duplicates while keeping identical replies from separate attempts', () => {
  const raw = [message('s1','dsh'), message('f1','god_generation'), message('s2','dsh'), message('f2','god_generation'),
    message('intro','dsh','我先检查上下文'), {...message('other','dsh'),sender:{id:'boss/member-01'}}];
  assert.deepEqual(deduplicateGodMessages(raw).map(m=>m.message_id),['f1','f2','intro','other']);
  assert.equal(raw.length,6);
});

test('legacy attempt rejection does not turn generated text into an adopted judgment or leak future status', () => {
  const reply = {...message('f1','god_generation'),source_event_id:'arb:judge:1'};
  const running = {id:'arb',status:'running',attempts:[{type:'arbitration.started',attempt:1}]};
  assert.equal(godMessageValidation(reply,[running],[]).validation_status,'pending');
  const retry = {...running,status:'retry_wait',attempts:[...running.attempts,{type:'arbitration.retry_wait',attempt:1,reason:'角色不符'}]};
  assert.equal(godMessageValidation(reply,[retry],[]).validation_status,'rejected');
  assert.equal(godMessageValidation(reply,[retry],[]).validation_diagnostic,'角色不符');
  const applied = {...retry,status:'applied',decided_event_seq:30,attempts:[...retry.attempts,{type:'arbitration.started',attempt:2}]};
  assert.equal(godMessageValidation(reply,[applied],[]).validation_status,'rejected');
  assert.equal(godMessageValidation({...reply,source_event_id:'arb:judge:2'},[applied],[]).validation_status,'applied');
});

test('replay enriches legacy failure reasons only after the matching transition', () => {
  const request={id:'arb',status:'exhausted',diagnostic:'final failure',attempts:[{event_seq:2,reason:'first failure'}]};
  const events=[{seq:1,type:'arbitration.started',payload:{arbitration_id:'arb',attempt:1}},
    {seq:2,type:'arbitration.retry_wait',payload:{arbitration_id:'arb',attempt:1}},
    {seq:3,type:'arbitration.exhausted',payload:{arbitration_id:'arb',attempt:2}}];
  const before=projectArbitrations([request],events,1)[0];
  assert.equal(before.diagnostic,undefined);
  assert.equal(before.attempts[0].reason,undefined);
  assert.equal(projectArbitrations([request],events,2)[0].attempts[1].reason,'first failure');
  assert.equal(projectArbitrations([request],events,3)[0].diagnostic,'final failure');
});
