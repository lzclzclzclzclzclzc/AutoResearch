import assert from 'node:assert/strict';
import test from 'node:test';
import { apply } from '../packages/dsh-plugin-research/index.js';

test('Watch Dog registers project inspection and repair tools without exposing them to research roles', () => {
  const saved = { ...process.env };
  process.env.AUTORESEARCH_TOOL_GATEWAY = 'http://127.0.0.1:1/tool';
  process.env.AUTORESEARCH_TOOL_CAPABILITY = 'fixture';
  try {
    for (const role of ['watchdog', 'critic', 'god']) {
      process.env.AUTORESEARCH_ROLE = role;
      const tools = [];
      let guard;
      apply({ on() {}, tools: { register(tool) { tools.push(tool); }, guard(value) { guard = value; } } });
      const names = tools.map(tool => tool.name);
      assert.equal(names.includes('read_watchdog_state'), role === 'watchdog');
      assert.equal(names.includes('apply_watchdog_action'), role === 'watchdog');
      assert.equal(guard({ name: 'apply_watchdog_action' }) === undefined, role === 'watchdog');
      if (role === 'watchdog') {
        assert.equal(names.includes('run_light_cpu'), false);
        const action = tools.find(tool => tool.name === 'apply_watchdog_action');
        assert.ok(action.parameters.properties.action.enum.includes('restore_line_task'));
      }
    }
  } finally {
    for (const key of Object.keys(process.env)) if (!(key in saved)) delete process.env[key];
    Object.assign(process.env, saved);
  }
});
