"""Measure a configured Messages endpoint without logging credentials or responses."""
import argparse
import asyncio
import json
import math
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import httpx
from backend.persistence.store import Store, now
from backend.services.credentials import CredentialService
from backend.adapters.provider_endpoints import messages_url


async def measure(provider, secret, levels):
    rows = []
    async with httpx.AsyncClient(timeout=45, limits=httpx.Limits(max_connections=max(levels), max_keepalive_connections=max(levels))) as client:
        for level in levels:
            ready = asyncio.Event()
            async def one():
                await ready.wait()
                started = time.monotonic()
                try:
                    response = await client.post(messages_url(provider.get('base_url')),
                        headers={'x-api-key': secret, 'anthropic-version': '2023-06-01'},
                        json={'model': provider['model'], 'messages': [{'role': 'user', 'content': 'Reply exactly OK.'}],
                              'max_tokens': 1024, 'thinking': {'type': 'disabled'}, 'stream': False})
                    valid = response.is_success and any(p.get('type') == 'text' and p.get('text', '').strip() for p in response.json().get('content', []))
                    return response.status_code, valid, time.monotonic()-started
                except Exception:
                    return 0, False, time.monotonic()-started
            tasks = [asyncio.create_task(one()) for _ in range(level)]
            started = time.monotonic()
            ready.set()
            values = await asyncio.gather(*tasks)
            latencies = sorted(v[2] for v in values)
            row = {'concurrency': level, 'successes': sum(v[1] for v in values),
                   'statuses': {str(code): sum(v[0] == code for v in values) for code in sorted({v[0] for v in values})},
                   'p95_seconds': round(latencies[math.ceil(.95*level)-1], 3), 'seconds': round(time.monotonic()-started, 3)}
            rows.append(row)
            print(json.dumps(row), flush=True)
            if row['successes'] != level:
                break
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data-dir', default='data')
    parser.add_argument('--provider', default='deepseek-default')
    parser.add_argument('--levels', default='1,8,16,32,64,128')
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    levels = [int(v) for v in args.levels.split(',')]
    if not levels or min(levels) < 1:
        parser.error('Concurrency must be positive')
    store = Store(args.data_dir)
    provider = store.get('provider', args.provider)
    secret = CredentialService(args.data_dir, store).get_secret(args.provider)
    if not provider or not secret:
        raise SystemExit('Configured provider credential required')
    rows = asyncio.run(measure(provider, secret, levels))
    previous = provider.get('concurrency_benchmark') or {}
    if all(previous.get(k) == provider.get(k) for k in ('base_url', 'model', 'credential_version')):
        rows = previous.get('rows', []) + rows
    failed = min((r['concurrency'] for r in rows if r['successes'] != r['concurrency']), default=float('inf'))
    stable = max((r['concurrency'] for r in rows if r['successes'] == r['concurrency'] and r['concurrency'] < failed), default=0)
    report = {'tested_at': now(), 'base_url': provider.get('base_url'), 'model': provider['model'],
              'credential_version': provider.get('credential_version'), 'rows': rows,
              'highest_successful_concurrency': stable, 'upper_bound_found': any(r['statuses'].get('429', 0) for r in rows),
              'observed_incomplete_response': failed != float('inf'),
              'recommended_limit': max(1, math.floor(stable*.75)) if stable else None,
              'scope': 'short Messages requests; long contexts and streaming may have lower capacity'}
    if args.apply and stable:
        store.patch('provider', args.provider, concurrency_benchmark=report)
    print(json.dumps(report), flush=True)


if __name__ == '__main__':
    main()
