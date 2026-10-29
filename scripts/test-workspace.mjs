// Run against a local API serving the built web app and an existing run with tools:
// NODE_PATH=<bundled node_modules> node scripts/test-workspace.mjs <research-id>
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { mkdir } from 'node:fs/promises';
const { chromium } = createRequire(import.meta.url)('playwright');
const researchId = process.argv[2];
assert.ok(researchId, 'Pass the ID of a research run containing tool calls.');
const base = process.env.AUTORESEARCH_TEST_URL || 'http://127.0.0.1:8766';
const browser = await chromium.launch({ headless: true, ...(process.env.PLAYWRIGHT_CHANNEL ? { channel: process.env.PLAYWRIGHT_CHANNEL } : {}) });
try {
  const page = await browser.newPage({ viewport: { width: 1440, height: 1050 } });
  const errors = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.goto(`${base}/research/${researchId}`);
  await page.waitForLoadState('networkidle');
  const separator = page.getByRole('separator', { name: '调整团队交流宽度' });
  await separator.waitFor({ state: 'visible' });
  const width = async () => (await page.locator('.chat-pane').boundingBox()).width;
  const before = await width();
  let handle = await separator.boundingBox();
  await page.mouse.move(handle.x + handle.width / 2, handle.y + 50);
  await page.mouse.down();
  await page.mouse.move(handle.x - 160, handle.y + 50, { steps: 8 });
  await page.mouse.up();
  const wider = await width();
  assert.ok(wider > before + 140, `Drag should widen chat: ${before} -> ${wider}`);
  assert.ok((await page.locator('.research-main').boundingBox()).width >= 360);
  await page.reload();
  await page.waitForLoadState('networkidle');
  assert.ok(Math.abs(await width() - wider) < 2, 'Width survives reload');
  await separator.focus();
  await page.keyboard.press('ArrowRight');
  assert.ok(Math.abs(await width() - wider + 24) < 2);
  await page.keyboard.press('Home');
  assert.equal(Math.round(await width()), 280);
  await page.keyboard.press('End');
  assert.equal(Math.round(await width()), Number(await separator.getAttribute('aria-valuemax')));
  await separator.dblclick();
  assert.equal(Math.round(await width()), 360);

  const search = page.getByRole('textbox', { name: '搜索完整会话' });
  const filtered = page.waitForResponse(response => response.url().includes('/tool-calls?') && response.url().includes('q=list_research_inputs'));
  await search.fill('list_research_inputs');
  await filtered;
  await page.locator('.tool-call').first().waitFor({ state: 'visible' });
  const result = page.locator('.tool-call').first();
  assert.equal(await result.getAttribute('open'), null, 'Results start collapsed');
  assert.equal(await result.locator('pre').count(), 0);
  await result.locator('summary').click();
  await result.locator('.tool-result pre').waitFor({ state: 'visible' });
  assert.equal(await result.locator('pre').count(), 2, 'One expansion reveals both arguments and result');
  assert.ok((await result.locator('.tool-result pre').innerText()).length > 0);
  await result.locator('summary').click();
  assert.equal(await result.locator('pre').count(), 0);
  const timeline = await (await page.request.get(`${base}/api/research/${researchId}/timeline?limit=2000`)).json();
  assert.ok(timeline.items.length < 2000, 'Historical activity should fit a compact page');
  assert.ok(!timeline.items.some(event => ['agent.output_delta', 'agent.runtime_event', 'message.delta'].includes(event.type)));

  const calls = await (await page.request.get(`${base}/api/research/${researchId}/tool-calls?limit=200`)).json();
  const firstCall = calls.items.find(call => call.tool_name === 'list_research_inputs' && call.result !== undefined);
  assert.ok(firstCall);
  await page.getByRole('link', { name: '历史回放', exact: true }).click();
  const replay = page.locator('#replay');
  await page.waitForFunction(() => { const input = document.querySelector('#replay'); return input && Number(input.max) > 1 && input.value === input.max; });
  await page.locator('.tool-call').first().waitFor({ state: 'visible' });
  const setReplay = async seq => {
    const loaded = page.waitForResponse(response => response.url().includes('/tool-calls?') && response.url().includes(`at_seq=${seq}`));
    await replay.evaluate((input, value) => {
      Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set.call(input, String(value));
      input.dispatchEvent(new Event('input', { bubbles: true }));
      input.dispatchEvent(new Event('change', { bubbles: true }));
    }, seq);
    await loaded;
    assert.equal(await replay.inputValue(), String(seq));
  };
  await setReplay(firstCall.event_seq);
  await page.locator('.tool-call').first().locator('summary').click();
  await page.locator('.tool-call-status.pending').waitFor({ state: 'visible' });
  assert.equal(await page.locator('.tool-result').count(), 0, 'Replay must not expose future results');
  assert.ok((await page.locator('.tool-call').allTextContents()).join('').includes('尚无结果'));
  await setReplay(firstCall.latest_event_seq);
  await page.locator('.tool-call').first().waitFor({ state: 'visible' });
  await page.getByRole('link', { name: '返回实时工作台' }).click();
  await page.waitForLoadState('networkidle');
  if (await search.inputValue() !== 'list_research_inputs') {
    const loaded = page.waitForResponse(response => response.url().includes('/tool-calls?') && response.url().includes('q=list_research_inputs'));
    await search.fill('list_research_inputs');
    await loaded;
  }
  await page.locator('.tool-call').first().waitFor({ state: 'visible' });

  handle = await separator.boundingBox();
  await page.mouse.move(handle.x + 5, handle.y + 50);
  await page.mouse.down();
  await page.mouse.move(handle.x - 160, handle.y + 50, { steps: 8 });
  await page.mouse.up();
  await page.locator('.tool-call').first().waitFor({ state: 'visible' });
  const expanded = page.locator('.tool-call').first();
  if (await expanded.getAttribute('open') === null) await expanded.locator('summary').click();
  await page.locator('.tool-result').first().locator('pre').waitFor({ state: 'visible' });
  await page.evaluate(() => window.scrollTo(0, 0));
  await page.waitForLoadState('networkidle');
  await mkdir('.test-data', { recursive: true });
  await page.screenshot({ path: '.test-data/workspace-tools.png', fullPage: true });
  for (const viewportWidth of [1100, 820, 768, 767, 390]) {
    await page.setViewportSize({ width: viewportWidth, height: 950 });
    await page.waitForFunction(() => document.documentElement.scrollWidth <= innerWidth);
    if (viewportWidth < 768) {
      assert.equal(await separator.isVisible(), false);
      await page.locator('.mobile-pane-switch').getByRole('button', { name: '交流记录' }).click();
      assert.equal(await page.locator('.chat-pane').isVisible(), true);
    } else assert.equal(await separator.isVisible(), true);
  }
  await page.screenshot({ path: '.test-data/workspace-mobile.png', fullPage: true });
  assert.deepEqual(errors, []);
  console.log(JSON.stringify({ passed: true, drag: { before, after: wider }, compactEvents: timeline.items.length, viewportWidths: [1440, 1100, 820, 768, 767, 390] }));
} finally { await browser.close(); }
