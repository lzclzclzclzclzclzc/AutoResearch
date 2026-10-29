import assert from 'node:assert/strict';
import test from 'node:test';
import { apply } from '../packages/dsh-plugin-research/index.js';

async function recorded(chunks, options = {}, privateTurn = false) {
  const previous = { fetch: globalThis.fetch, env: { ...process.env } };
  const records = [], output = [];
  let handler;
  process.env.AUTORESEARCH_TOOL_GATEWAY = 'http://127.0.0.1:1/tool';
  process.env.AUTORESEARCH_TOOL_CAPABILITY = 'test-capability';
  process.env.AUTORESEARCH_ROLE = 'exploration';
  process.env.AUTORESEARCH_PRIVATE_TURN = privateTurn ? '1' : '0';
  globalThis.fetch = async (_url, request) => { records.push(JSON.parse(request.body).arguments); return { ok: true }; };
  apply({ on: (_event, value) => { handler = value; }, tools: { register() {}, guard() {} } });
  try {
    const source = typeof chunks === 'function' ? chunks : async function* () { yield* chunks; };
    for await (const chunk of handler(options, source)) output.push(chunk);
    return { records, output };
  } finally {
    globalThis.fetch = previous.fetch;
    for (const key of Object.keys(process.env)) if (!(key in previous.env)) delete process.env[key];
    Object.assign(process.env, previous.env);
  }
}

test('batches many token chunks without changing the provider stream or losing trailing text', async () => {
  const originalNow = Date.now;
  Date.now = () => 1000;
  try {
    const text = '{"content_markdown":"' + '中文 \\n \\"'.repeat(1000) + '"}';
    const chunks = [...text].map(text => ({ type: 'text-delta', text }));
    chunks.push({ type: 'usage', input: 1, output: 2 }, { type: 'finish', reason: { kind: 'stop' } });
    const { records, output } = await recorded(chunks);
    assert.deepEqual(output, chunks);
    assert.equal(records.filter(row => row.chunk.type === 'text-delta').map(row => row.chunk.text).join(''), text);
    assert.ok(records.length < chunks.length / 50);
    assert.deepEqual(records.map(row => row.cursor), records.map((_, index) => index + 1));
    assert.equal(records.at(-1).chunk.type, 'finish');
  } finally { Date.now = originalNow; }
});

test('purpose and private scheduling streams still reach the SDK without per-token recording', async () => {
  const chunks = [{ type: 'text-delta', text: 'private score' }, { type: 'finish' }];
  for (const [options, privateTurn] of [[{ purpose: 'title' }, false], [{}, true]]) {
    const result = await recorded(chunks, options, privateTurn);
    assert.deepEqual(result.output, chunks);
    assert.deepEqual(result.records, []);
  }
});

test('records complete text around tool boundaries, leaving partial tool arguments to SDK session events', async () => {
  const chunks = [{ type: 'text-delta', text: 'before ' }, { type: 'text-delta', text: 'tool' },
    { type: 'tool-call-delta', arguments: '{"partial":' }, { type: 'finish' }];
  const { records, output } = await recorded(chunks);
  assert.deepEqual(output, chunks);
  assert.equal(records.filter(row => row.chunk.type === 'text-delta').map(row => row.chunk.text).join(''), 'before tool');
  assert.ok(!records.some(row => row.chunk.type === 'tool-call-delta'));
});
