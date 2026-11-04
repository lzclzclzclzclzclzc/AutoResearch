# Startup recovery implementation plan

**Goal:** Restore local scheduling after slow Runner startup, report unhealthy
services accurately, and reconcile Disaster-Tweets10's existing stop.

**Architecture:** Keep Runner recovery and Control's fail-closed journal checks.
The launcher continues probing Runner after its initial readiness timeout and
starts the missing Worker only after Runner is ready. Health includes Worker
heartbeat freshness and Control journal integrity.

**Tech stack:** Existing Python launcher, FastAPI, SQLite, pytest; no dependencies.

**Spec:** The diagnosis and user's repair request in this chat.

## Constraints

- Preserve all research data and stop tombstones; do not revive stopped research.
- Move only the identified non-journal backup, with a SHA-256 integrity check.
- Repair the existing local checkout and running installation; no automatic commit.
- Keep independent stop/API access available during delayed Runner startup.

## Tasks

1. Launcher (`scripts/launch.py`, `tests/unit/test_worker_resilience.py`)
   - [x] Reproduce an 80-second Runner startup with a virtual clock, including
     the initial 25-second timeout, and assert Worker eventually starts once.
   - [x] Check missing/exited Worker backoff, Runner readiness gating, and no
     duplicate Worker when one is alive.
   - [x] Add readiness-gated supervision for the initially missing Worker;
     retain the existing exited-Worker restart path.
2. Health (`apps/api/main.py`, `tests/unit/test_api.py`)
   - [x] Assert degraded for missing, stale, malformed, or stopped Worker state
     and an incomplete Control journal, and ok for a fresh running Worker.
   - [x] Expose Worker availability and check heartbeat age (30 seconds),
     retaining the saved diagnostic fields.
3. Recovery and verification
   - [x] Verify malformed journal files still fail closed and archiving an
     unrelated backup preserves stopped projects while permitting new ones.
   - [x] Run affected tests and the full Python suite; inspect all failures.
   - [x] Confirm no active research before restarting the local services.
   - [x] Archive the identified backup outside the journal root, verify its hash,
     and restart only this installation's verified launcher/process tree.
   - [x] Confirm fresh Worker heartbeat, healthy Control, synchronized stop for
     Disaster-Tweets10, and no work admitted for that stopped research.

## Review focus

- A live Runner that is still recovering must not be restarted or bypassed.
- Repeated checks must not create duplicate Workers or bypass the Worker lock.
- Missing or invalid heartbeat timestamps must not produce a healthy response.
- Truly corrupt journal records must continue to close admission.
- Service restoration must preserve the user's existing stop decisions.

## Evidence

- The initial regression run reproduced the startup/health failures (10 failed,
  2 passed); after the fix, all 64 tests in the three affected modules passed.
- Independent read-only review found no actionable issues.
- Full Python suite: 726 passed, 5 dependency warnings in 1018.44 seconds;
  exit code 0. Log: `.test-data/startup-full.log`.
- Production verification at 2026-11-04 19:57 Asia/Shanghai: `status=ok`,
  `worker.available=true`, advancing heartbeat, `control.journal_unknown=false`,
  Runner available. All 14 original stop identities preserved. Disaster-Tweets10
  is terminated, its supervisor task is cancelled with attempt 0, and it has no
  model executions. See `.test-data/startup-repair-after.json`.
- The identified backup is preserved under
  `data/diagnostics/control-backups/line01-plan-repair-20261031.json` with SHA-256
  `0933a409d5eae15e7054d51c2fd6eb8f8d08dfd2a33b891237edf487ecb4e135`.
