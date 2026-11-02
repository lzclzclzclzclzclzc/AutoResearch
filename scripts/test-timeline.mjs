import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import test from 'node:test';
import { setImmediate } from 'node:timers/promises';
import { timelineQueryOptions } from '../apps/web/src/timeline.ts';

const { QueryClient, QueryObserver } = createRequire(new URL('../apps/web/package.json', import.meta.url))('@tanstack/react-query');

function fixture(t, initialEvents = [{ seq: 1 }]) {
  const events = [...initialEvents];
  const requests = [];
  t.mock.method(globalThis, 'fetch', async path => {
    const url = new URL(path, 'http://localhost');
    if (url.pathname === '/api/session') return Response.json({ csrf_token: 'test-token' });
    assert.equal(url.pathname, '/api/research/review/timeline');
    requests.push(url);
    const after = Number(url.searchParams.get('after'));
    const limit = Number(url.searchParams.get('limit'));
    return Response.json({ items: events.filter(event => event.seq > after).slice(0, limit) });
  });
  const client = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: 1000, refetchOnWindowFocus: false } } });
  const observers = [];
  const observe = (history = true) => {
    const observer = new QueryObserver(client, timelineQueryOptions('review', history));
    const unsubscribe = observer.subscribe(() => {});
    observers.push(() => { unsubscribe(); observer.destroy(); });
    return { observer, unsubscribe };
  };
  t.after(() => { observers.forEach(close => close()); client.clear(); });
  return { client, events, requests, observe };
}

async function settledData(observer) {
  await setImmediate();
  const result = observer.getCurrentResult();
  assert.equal(result.fetchStatus, 'idle');
  assert.equal(result.error, null);
  return result.data;
}

test('returning from live mode refreshes the cached timeline in a reused workspace', async t => {
  const { client, events, requests, observe } = fixture(t);
  const { observer } = observe(false);
  assert.equal(await settledData(observer), undefined);
  assert.equal(requests.length, 0);

  observer.setOptions(timelineQueryOptions('review', true));
  assert.deepEqual(await settledData(observer), [{ seq: 1 }]);
  observer.setOptions(timelineQueryOptions('review', false));
  events.push({ seq: 2 });
  client.setQueryData(['snapshot', 'review'], { snapshot_seq: 2 });
  await client.invalidateQueries({ queryKey: ['snapshot', 'review'] });
  await settledData(observer);
  assert.equal(requests.length, 1, 'live mode does not fetch the timeline');

  observer.setOptions(timelineQueryOptions('review', true));
  assert.deepEqual(await settledData(observer), events);
  assert.equal(requests.length, 2);
});

test('reopening a history workspace refreshes an existing timeline cache', async t => {
  const { events, requests, observe } = fixture(t);
  const first = observe();
  assert.deepEqual(await settledData(first.observer), [{ seq: 1 }]);
  first.unsubscribe();
  first.observer.destroy();
  events.push({ seq: 2 });

  const reopened = observe();
  assert.deepEqual(await settledData(reopened.observer), events);
  assert.equal(requests.length, 2);
});

test('ordinary renders and live data updates do not repeatedly fetch full history', async t => {
  const { client, events, requests, observe } = fixture(t);
  const { observer } = observe();
  await settledData(observer);
  events.push({ seq: 2 });
  for (let render = 0; render < 3; render++) {
    observer.setOptions(timelineQueryOptions('review', true));
    await settledData(observer);
  }
  assert.equal(requests.length, 1, 'staying in history does not repeatedly fetch');

  observer.setOptions(timelineQueryOptions('review', false));
  for (const key of ['snapshot', 'conversations', 'edge-messages', 'agent-executions', 'tool-calls', 'arbitrations', 'arbitration', 'messages']) {
    client.setQueryData([key, 'review'], {});
    await client.invalidateQueries({ queryKey: [key, 'review'] });
    observer.setOptions(timelineQueryOptions('review', false));
    await settledData(observer);
  }
  assert.equal(requests.length, 1);
});

test('history refresh reads every timeline page using the last event sequence', async t => {
  const events = Array.from({ length: 2001 }, (_, index) => ({ seq: (index + 1) * 2 }));
  const { requests, observe } = fixture(t, events);
  const { observer } = observe();
  assert.deepEqual(await settledData(observer), events);
  assert.deepEqual(requests.map(url => url.search), ['?after=0&limit=2000', '?after=4000&limit=2000']);
});
