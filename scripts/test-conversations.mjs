import assert from 'node:assert/strict';
import test from 'node:test';
import { selectedConversation, sortConversations } from '../apps/web/src/conversations.ts';

const row = (id, minute, active = false) => ({ conversation_id: id, created_at: `2026-11-01T01:${String(minute).padStart(2,'0')}:00Z`, is_active: active });
const scope = JSON.stringify(['project', 'node', 'line-01/critic']);

test('unordered sessions select the active one; no activity selects the most recent', () => {
  const rows = [row('old', 1), row('latest', 5), row('active', 2, true)];
  assert.equal(selectedConversation(sortConversations(rows), null, scope).conversation_id, 'active');
  rows[2].is_active = false;
  assert.equal(selectedConversation(sortConversations(rows), null, scope).conversation_id, 'latest');
});
test('automatic selection follows new work, manual selection stays at the chosen old session', () => {
  const rows = sortConversations([row('old', 1), row('latest', 5), row('new-active', 6, true)]);
  assert.equal(selectedConversation(rows, null, scope).conversation_id, 'new-active');
  assert.equal(selectedConversation(rows, {scope, id:'old'}, scope).conversation_id, 'old');
  assert.equal(selectedConversation(rows, {scope, id:'old'}, scope + '-other-agent').conversation_id, 'new-active');
});
test('replay falls back when the pinned item did not yet exist and empty lists stay empty', () => {
  const rows = [row('visible-then', 1)];
  assert.equal(selectedConversation(rows, {scope, id:'created-later'}, scope).conversation_id, 'visible-then');
  assert.equal(selectedConversation([], null, scope), undefined);
});
test('equal times use event sequence then conversation ID for deterministic order', () => {
  const rows = [row('a',1), row('c',1), {...row('b',1),last_activity_seq:8}];
  assert.deepEqual(sortConversations(rows).map(item=>item.conversation_id), ['b','c','a']);
});
