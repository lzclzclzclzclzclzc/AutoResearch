# AutoResearch

## Overview

AutoResearch is a local automated research workbench. AI team members collaborate on research exploration, design review, implementation, experiment verification, and report writing. Follow discussions, votes, progress, and evidence in the web interface.

Each project runs one or more research tracks, coordinated by a shared team:

- **Exploration and Critic** discuss research questions, compare approaches, and review experiment plans. Members can initiate polls; ending a discussion requires unanimous approval of the deliverable and agreement on the next step. After three failed ending votes, **God** produces the final deliverable and handoff.
- **Implementation and Verify** turn approved plans into code and check actual experiment results against the plan. Experiments run locally through a supervised Runner, with logs and artifacts retained as evidence.
- **Boss** coordinates research tracks, changes directions, maintains shared knowledge, and decides when to request reports. **Organizer** coordinates machine resources, while **Paper** writes reports from frozen evidence snapshots.
- **Watchdog** independently checks research tracks for stalled work and can diagnose and repair recoverable execution problems. Its inspections and recovery actions are visible in the organization graph.

The application has a **React web interface**, a **FastAPI backend**, and a **LangGraph worker** that advances research workflows. A separate **Control Supervisor** handles execution permissions and immediate termination, while the **Runner** manages experiment processes. SQLite stores project state, messages, and workflow checkpoints; files, logs, and evidence remain in the local `data/` directory by default.

Research tracks can run in parallel under a shared project deadline. The workbench supports pausing new work, terminating active processes, history replay, and checkpoint-based failure recovery. Live research uses the DeepSeek API through the official DSH SDK and runtime; demo mode lets you explore the workflow.

## Watchdog and recovery

Each project has a configurable inspection interval, defaulting to **10 minutes** in the research team settings. Watchdog checks task ownership, model executions, experiments, dependencies, and resource state. Healthy tracks and normal waits do not trigger a diagnostic model call.

When a track is confirmed stalled, Watchdog receives its identifiers, errors, and local database, checkpoint, log, and workspace paths. It uses the official DSH SDK's native local tools to investigate and repair the running project. This role has `danger-full-access` permissions and can read and modify local code and research records; run it under a Windows account whose access you intend to grant.

Recovery actions include injecting corrective instructions into an existing task, resuming a failed task, restoring a missing track task, redelivering a handoff, and reconciling an experiment's actual state. A running execution must be confirmed exited before a replacement can start. Checkpoints, discussion history, votes, and evidence are retained; a recovered generation continues under the original direction and project deadline. The graph shows the diagnosis and actions, and marks a recovery as repaired only after subsequent inspections observe progress.

Watchdog respects pause, termination, direction changes, and the total runtime limit. It cannot resume a terminated or expired project, supply missing external data or credentials, or guarantee that every failure can be repaired. Existing stop records must be preserved.

## Execution and evidence

- **Formal experiments:** Implementation submits source files, an entry point, and resource requirements; Organizer and Runner authorize and supervise execution. Confirmed host-memory-limit failures can retry with twice the memory budget, at most twice and within available capacity. GPU memory errors or an error message containing "OOM" alone do not trigger this expansion.
- **Light CPU tools:** short Python checks run with one CPU core and a **512 MiB limit for the entire process tree**, including subprocesses and their imports. Each call has its own writable workspace; the supplied data directory is read-only by role policy. Larger training jobs belong in the formal Runner workflow. A tool transport error does not prove the underlying calculation has exited; inspect saved state and logs before retrying.
- **Model recovery:** output-format corrections, truncation recovery, and response-protocol retries have no fixed attempt-count limit. Every attempt is recorded; pause, termination, and deadlines still take precedence. God arbitration also retains its own judgment deadline. Old executions must have exit proof before their session can be reused.
- **Messages and history:** messages have durable recipient inboxes and delivery receipts. Conversation views identify their purpose and research round, preserve manual selection, and support historical replay. Exports respect the selected scope. Completed, terminated, or quarantined records can be soft-deleted and restored without deleting their evidence.
- **Reports:** Boss decides when to request a report. Paper uses frozen evidence, including authorized attachments, and can produce a stage report without completed experiments; failed and negative results remain part of the evidence record.

## Deployment

Requirements: Windows, Python 3.12+, Node.js 22.12+, and [uv](https://docs.astral.sh/uv/).

Run in PowerShell:

```powershell
git clone --branch release --single-branch https://github.com/lzclzclzclzclzclzc/AutoResearch.git
cd AutoResearch
powershell -ExecutionPolicy Bypass -File scripts/setup.ps1
powershell -ExecutionPolicy Bypass -File start.ps1
```

Open the [local workbench](http://127.0.0.1:8000), save your DeepSeek API key in the model and credentials settings, configure your research team, and create a task. Setup installs backend dependencies and the official DSH runtime, then builds the frontend. For subsequent launches, run `start.ps1`.

Default ports are `8000` (web), `8766` (control), and `8767` (experiment runner). Services listen on localhost only. To change ports or the data directory:

```powershell
powershell -ExecutionPolicy Bypass -File start.ps1 --port 8080 --control-port 8866 --runner-port 8867 --data-dir D:/ResearchData
```

Press `Ctrl+C` to stop services and active research. Stop services before backing up the entire data directory. Configure your API key again after moving to another Windows account or machine.

## Operations and upgrades

The launcher keeps Control and the web API available while Runner recovers historical work. If Runner exceeds its first readiness timeout, the launcher continues probing and starts the missing Worker when Runner becomes ready. An exited Worker is restarted with backoff and reconciles execution state before scheduling new work.

The [health endpoint](http://127.0.0.1:8000/api/health) reports `degraded` when the Worker heartbeat is missing, invalid, or older than 30 seconds, or when the Control journal is incomplete. Service logs are `data/control.log`, `data/api.log`, `data/runner.log`, and `data/worker.log` (under the chosen data directory). The `data/control/` root is reserved for application-generated control records: save diagnostic JSON and manual backups elsewhere, such as `data/diagnostics/`. An unrelated JSON file in the journal directory can close execution admission.

Pausing prevents new work at safe boundaries while existing experiments remain monitored; the project clock continues during pauses, queueing, and restart gaps. Immediate termination uses the independent Control service and stops process trees without waiting for business database writes. A Control restart invalidates old execution permissions and isolates unfinished projects. To continue terminated or expired research, create a new project.

To upgrade, stop the launcher, back up the complete data directory, update the `release` checkout, rerun `scripts/setup.ps1`, and start the application again. Shutting down the launcher terminates active research, so schedule upgrades accordingly. Existing projects keep their frozen model configuration; changing credentials requires explicitly rebinding the project's credential version through its recovery controls.

This branch contains runtime sources and deployment/build dependencies. Development documentation, tests, fixtures, generated schema exports, and developer verification scripts live on `main`; this README is maintained separately for deployment users.
