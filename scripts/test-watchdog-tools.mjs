import assert from 'node:assert/strict';
import test from 'node:test';
import { apply } from '../packages/dsh-plugin-research/index.js';

function registeredTools(role, hasInputs = false) {
  const environment = {
    AUTORESEARCH_TOOL_GATEWAY: 'http://127.0.0.1:1/tool',
    AUTORESEARCH_TOOL_CAPABILITY: 'fixture',
    AUTORESEARCH_ROLE: role,
    AUTORESEARCH_HAS_INPUTS: hasInputs ? '1' : '0',
  };
  const saved = Object.fromEntries(Object.keys(environment).map(key => [key, process.env[key]]));
  Object.assign(process.env, environment);
  try {
    const tools = [], guards = [];
    apply({ on() {}, tools: { register(tool) { tools.push(tool); }, guard(value) { guards.push(value); } } });
    return { tools, guards, names: tools.map(tool => tool.name) };
  } finally {
    for (const [key, value] of Object.entries(saved)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  }
}

test('Watch Dog keeps native tools unrestricted and registers its project repair tools', () => {
  for (const hasInputs of [false, true]) {
    const { tools, guards, names } = registeredTools('watchdog', hasInputs);
    assert.deepEqual(guards, [], 'Watch Dog must not install a native-tool allowlist');
    assert.ok(names.includes('read_watchdog_state'));
    assert.ok(names.includes('apply_watchdog_action'));
    for (const name of ['run_light_cpu', 'list_research_inputs', 'read_research_input', 'read_arbitration_context']) {
      assert.equal(names.includes(name), false, `${name} is not a Watch Dog domain tool`);
    }
    const action = tools.find(tool => tool.name === 'apply_watchdog_action');
    assert.ok(action.parameters.properties.action.enum.includes('restore_line_task'));
    assert.ok(action.parameters.required.includes('target_id'));
  }
});

test('research roles retain their domain-tool boundaries and cannot invoke Watch Dog repairs', () => {
  for (const role of ['exploration', 'critic', 'implementation', 'verify', 'boss', 'god']) {
    for (const hasInputs of [false, true]) {
      const { guards, names } = registeredTools(role, hasInputs);
      assert.equal(guards.length, 1, `${role} retains its role guard`);
      const [guard] = guards;
      for (const name of names) assert.equal(guard({ name }), undefined, `${role} can use ${name}`);
      for (const name of ['read_watchdog_state', 'apply_watchdog_action', 'future_native_tool']) {
        assert.equal(names.includes(name), false, `${role} does not register ${name}`);
        assert.match(guard({ name }), /outside the registered research role policy/, `${role} cannot invoke ${name}`);
      }
      const canRunCpu = ['exploration', 'critic', 'implementation', 'verify'].includes(role);
      for (const [name, allowed] of [
        ['run_light_cpu', canRunCpu],
        ['list_research_inputs', hasInputs && role !== 'god'],
        ['read_research_input', hasInputs && role !== 'god'],
        ['read_arbitration_context', role === 'god'],
      ]) {
        assert.equal(names.includes(name), allowed, `${role} registration for ${name}, inputs=${hasInputs}`);
        assert.equal(guard({ name }) === undefined, allowed, `${role} permission for ${name}, inputs=${hasInputs}`);
      }
      for (const name of ['web_search', 'web_fetch', 'todo_write']) {
        assert.equal(guard({ name }) === undefined, role !== 'god', `${role} native permission for ${name}`);
      }
    }
  }
});
