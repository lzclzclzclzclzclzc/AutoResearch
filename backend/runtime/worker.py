"""Separate process for research orchestration, never inside HTTP requests."""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import inspect
import os
import signal
import sys
import time
import uuid
from pathlib import Path

from backend.adapters.dsh import ResearchStopped, RuntimeBlocked, kill_tree
from backend.persistence.store import Store, now
from backend.graphs.tasks import current_task, TaskSuspended
from backend.runtime.recovery import retryable_failure, recover_infrastructure_tasks, execution_exit_proven


@contextlib.contextmanager
def worker_lock(data_dir):
    path = Path(data_dir) / "worker.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        handle.seek(0)
        handle.write(b"0")
        handle.flush()
        handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, BlockingIOError) as error:
            raise RuntimeError("已有 Worker 持有本机调度锁") from error
        try:
            yield
        finally:
            if os.name == "nt":
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)


async def sync_control_stops(store, control):
    """Created/queued projects can be stopped before they have a start time."""
    for project in store.projects(include_deleted=True):
        if project["status"] == "completed":
            continue
        try:
            status = await asyncio.to_thread(control.status, project["id"])
            if status.get("stop_id"):
                await asyncio.to_thread(store.sync_stop, project["id"], status)
                await asyncio.to_thread(control.mark_db_synced, project["id"], status["stop_id"],
                    resources_reconciled=store.project(project["id"])["status"] == "terminated")
        except Exception:
            # Unavailable control never grants admission; dispatch has its own gate.
            continue


async def dispatch_pending_handoffs(store, control, limit=25):
    """Bound one drain; failed delivery remains durable for the next iteration."""
    try:
        return await asyncio.to_thread(store.dispatch_outbox, control, limit)
    except Exception as error:
        print(json.dumps({"event": "verification_delivery_retry", "code": type(error).__name__}), flush=True)
        return {"retry_pending": True}


async def recover_running_tasks(store, control):
    for task in store.list("task"):
        if task.get("status") == "running":
            project = store.project(task["research_id"])
            allowed = await asyncio.to_thread(control.allows_progress, project["id"], project["control_generation"])
            # Initial supervision reconciles registration before checking the
            # execution gate; its saved clock alone is not a Control identity.
            unknown = any(x.get("status") == "unknown" for x in store.list("execution", project["id"]))
            recoverable = (allowed or task["kind"] == "supervisor") and not unknown
            store.patch("task", task["id"], status="pending" if recoverable else "blocked", owner=None,
                        error=None if recoverable else {"code": "recovery_quarantined", "message": "控制服务不允许自动恢复"})


async def recover_arbitration_leases(store, control, source_task_id=None):
    """Startup only, after the exclusive Worker lock and process exit reconciliation."""
    for request in store.list("arbitration_request", status="running"):
        if source_task_id is not None and request.get("source_task_id") != source_task_id:
            continue
        executions = [x for x in store.list("execution", request["research_id"])
            if x.get("agent_id") == "god/" + request["id"]]
        if any(x.get("status") in {"registered", "running", "unknown"} for x in executions):
            continue
        project = store.project(request["research_id"])
        if project["status"] in {"terminated", "terminating", "quarantined", "completed"}:
            store.transition_arbitration(request["id"], "cancelled", owner=request.get("owner"),
                lease_epoch=request["lease_epoch"], reason="worker_recovery_scope_closed")
            continue
        if not await asyncio.to_thread(control.allows_progress, project["id"], project["control_generation"]):
            continue
        recovered = None
        from datetime import datetime
        from packages.contracts.research import GodDecision, digest
        attempt = request.get("generation_attempts", request.get("attempt", 0))
        for execution in executions:
            if (execution.get("status") != "completed" or execution.get("turn_id") != f"judge:{attempt}" or
                    not execution.get("completed_at") or not isinstance(execution.get("output"), dict)):
                continue
            try:
                completed = datetime.fromisoformat(execution["completed_at"].replace("Z", "+00:00"))
                deadline = datetime.fromisoformat(request["judge_deadline_at"].replace("Z", "+00:00"))
                output = GodDecision.model_validate(execution["output"]).model_dump()
                if (completed > deadline or output["arbitration_id"] != request["id"] or
                        output["snapshot_hash"] != request["snapshot_hash"]):
                    continue
            except (ValueError, TypeError, KeyError):
                continue
            recovered = {"output": output, "output_hash": digest(output), "completed_at": execution["completed_at"],
                "source_execution_id": execution.get("execution_attempt_id", execution["id"]), "attempt": attempt}
            break
        # Release the old owner's lease without extending the request's deadline,
        # retry delay or generation budget. No replacement starts in this scan.
        store.transition_arbitration(request["id"], "retry_wait", owner=request.get("owner"),
            lease_epoch=request["lease_epoch"], retry_not_before=request.get("retry_not_before") or now(),
            reason="worker_restarted_after_confirmed_process_exit", processes_reconciled=True,
            **({"recovered_response": recovered} if recovered else {}))


async def migrate_arbitrable_tasks(engine, graphs, dry_run=False):
    """Adopt only existing, checkpointed failures with an open control generation."""
    from backend.graphs.arbitration import ARBITRABLE
    proposals = []
    store = engine.store
    for task in store.list("task"):
        error = task.get("error") or {}
        if task["status"] != "failed" or error.get("code") not in ARBITRABLE:
            continue
        project = store.project(task["research_id"])
        if project["status"] not in {"running", "blocked"} or project.get("stop_reason"):
            continue
        if not await asyncio.to_thread(engine.control.allows_progress, project["id"], project["control_generation"]):
            continue
        if any(x.get("status") == "unknown" for x in store.list("execution", project["id"])):
            continue
        payload = task["payload"]
        if payload.get("line_id"):
            line = store.get("line", f"{project['id']}:{payload['line_id']}")
            if not line or line.get("finish_requested") or payload.get("direction_epoch", line["direction_epoch"]) != line["direction_epoch"]:
                continue
        if task["kind"] == "line":
            graph = engine.line_graph
            thread = f"research/{project['id']}/line/{payload['line_id']}/{payload['direction_epoch']}/{payload['cycle_id']}"
        elif task["kind"] in graphs:
            graph = graphs[task["kind"]].graph
            thread = f"research/{project['id']}/{task['kind']}/{task['id']}"
        else:
            continue
        snapshot = await graph.aget_state({"configurable": {"thread_id": thread}})
        if not snapshot.values or not snapshot.next:
            continue
        proposal = {"task_id": task["id"], "origin_error": error, "graph_thread_id": thread,
            "origin_checkpoint": snapshot.config.get("configurable", {}).get("checkpoint_id")}
        proposals.append(proposal)
        if dry_run:
            continue
        store.put("arbitration_migration", task["id"], proposal, project["id"])
        store.patch("task", task["id"], status="pending", owner=None, error=None,
            execution_phase="arbitrating", arbitration_migration=proposal)
        if payload.get("line_id"):
            store.update_line(project["id"], payload["line_id"], phase="arbitrating", error=None)
        # Do not clear unrelated failures on the same project.
        remaining = [t for t in store.list("task", project["id"]) if t["status"] == "failed"]
        if project["status"] == "blocked" and not remaining and (project.get("error") or {}).get("code") in ARBITRABLE:
            store.update_project(project["id"], status="running", error=None)
        store.emit(project["id"], "arbitration.migrated", proposal, line_id=payload.get("line_id"))
    return proposals


async def retry_startup(operation, stop=None):
    """Retry initialization under the Worker lock, before any task admission."""
    delay = .4
    while True:
        if stop and stop.is_set():
            raise asyncio.CancelledError
        try:
            result = operation()
            return await result if inspect.isawaitable(result) else result
        except Exception as error:
            if not retryable_failure(error):
                raise
            print(json.dumps({"event": "worker_startup_retry", "code": getattr(error, "code", type(error).__name__)}), flush=True)
            if stop:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    pass
            else:
                await asyncio.sleep(delay)
            delay = min(5, delay * 2)


async def reconcile_worker_executions(store):
    for execution in store.list("execution"):
        if execution.get("status") in {"registered", "running", "unknown"}:
            pid = execution.get("pid")
            confirmed = bool(pid) or execution.get("mode") == "demo"
            if pid:
                import psutil
                if psutil.pid_exists(pid):
                    process = psutil.Process(pid)
                    if abs(process.create_time() - execution.get("pid_create_time", -1)) < 0.1:
                        confirmed = await asyncio.to_thread(kill_tree, pid)
                    else:
                        confirmed = False
            store.patch("execution", execution["id"], status="interrupted" if confirmed else "unknown")


async def run_worker(data_dir, control_url, runner_url):
    from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
    from backend.graphs.engine import ResearchEngine
    from backend.graphs.tasks import BossDecisionGraph, PaperGraph, ResearchSupervisorGraph, CommunicationGraph, DecompositionPartGraph, RecoveryRoleGraph
    from backend.runtime.control import ControlClient, ControlUnavailable
    from backend.runtime.runner import RunnerClient
    from backend.services.credentials import CredentialService

    stop = asyncio.Event()
    def shutdown(*_):
        stop.set()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, shutdown)
    store = await retry_startup(lambda: asyncio.to_thread(Store, data_dir), stop)
    control, runner = ControlClient(control_url), RunnerClient(runner_url)
    credentials = CredentialService(data_dir, store)
    await retry_startup(lambda: asyncio.to_thread(store.recover_confirmed_operations, control), stop)
    owner = f"worker-{os.getpid()}-{uuid.uuid4().hex[:8]}"
    active = {}
    last_confirmation_recovery = time.monotonic()
    handoff_drain, last_handoff_drain = None, 0
    # A previous SDK owner cannot survive unnoticed. Its children also have an
    # owner-death watchdog; unknown process identities keep execution blocked.
    await retry_startup(lambda: reconcile_worker_executions(store), stop)
    await retry_startup(lambda: recover_arbitration_leases(store, control), stop)
    await retry_startup(lambda: recover_running_tasks(store, control), stop)

    async with AsyncSqliteSaver.from_conn_string(str(Path(data_dir).resolve() / "checkpoints.sqlite")) as saver:
        engine = ResearchEngine(store, control, runner, credentials, saver)
        graphs = {"supervisor": ResearchSupervisorGraph(engine.supervisor, saver),
                  "boss": BossDecisionGraph(engine.boss, saver), "paper": PaperGraph(engine.paper, saver),
                  "communication": CommunicationGraph(engine.communication, saver),
                  "decomposition_part": DecompositionPartGraph(engine.decompositions.execute_part, saver),
                  "recovery_role": RecoveryRoleGraph(engine.decompositions.execute_evidence, saver)}
        await retry_startup(lambda: migrate_arbitrable_tasks(engine, graphs), stop)
        await retry_startup(lambda: recover_infrastructure_tasks(store, control), stop)
        await retry_startup(lambda: engine.dependencies.recover_legacy(), stop)
        capacity = max(1, int(os.environ.get("AUTORESEARCH_WORKER_CONCURRENCY", "32")))
        deferred_retries = {}
        async def execute_once(task):
            research_id = task["research_id"]
            task_token = current_task.set(task)
            try:
                result = await (engine.line(task) if task["kind"] == "line" else graphs[task["kind"]].run(task))
                store.finish_task(task["id"], result=result)
            except TaskSuspended as suspended:
                wait = store.get("recovery_wait", suspended.resume_token) or {}
                phase = "waiting_dependencies" if wait.get("dependency_wait_id") else "waiting_subtasks"
                store.patch("task", task["id"], status="waiting_recovery", owner=None,
                    execution_phase=phase, graph_thread_id=suspended.graph_thread_id,
                    resume_token=suspended.resume_token, recovery_outcome=None)
                if task.get("payload", {}).get("line_id"):
                    store.update_line(research_id, task["payload"]["line_id"], phase=phase)
                store.emit(research_id, "task.suspended", {"task_id": task["id"], "resume_token": suspended.resume_token},
                    line_id=task.get("payload", {}).get("line_id"))
            except ResearchStopped:
                store.patch("task", task["id"], status="cancelled", completed_at=now())
                if task["kind"] == "line":
                    # Programmatic archival only: stopping never launches a Verify,
                    # Boss or Paper model turn to produce a closing message.
                    archive_id = task["id"] + ":stopped"
                    store.put("submission_archive", archive_id, {"line_id": task["payload"]["line_id"],
                        "cycle_id": task["payload"].get("cycle_id"), "status": "verification_pending",
                        "runs": store.list("run", research_id), "recorded_at": now()}, research_id)
                for submission in store.list("submission", research_id):
                    if submission.get("status") == "awaiting_verification":
                        store.put("submission_archive", submission["id"], {"submission_id": submission["id"], "status": "verification_pending"}, research_id)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                failure = retryable_failure(error)
                if failure:
                    deferred_retries[task["id"]] = (task, failure)
                    # Leave the running row recoverable if the retry write also
                    # encounters contention. The scheduler drains this after exit.
                    return
                from backend.graphs.arbitration import ArbitrationRequired
                if isinstance(error, ArbitrationRequired) and error.recovery.get("resume_point"):
                    try:
                        if await engine.recover_required_task(task, error):
                            return
                    except TaskSuspended as suspended:
                        store.patch("task", task["id"], status="waiting_recovery", owner=None,
                            resume_token=suspended.resume_token, graph_thread_id=suspended.graph_thread_id)
                        return
                    except Exception as recovery_error:
                        error = recovery_error
                failure = retryable_failure(error)
                if failure:
                    deferred_retries[task["id"]] = (task, failure)
                    return
                code = getattr(error, "code", type(error).__name__)
                message = str(error) if isinstance(error, (RuntimeBlocked, ControlUnavailable)) else "任务执行失败，已保存状态；请检查依赖、运行时与实验环境后恢复"
                if code == "research_paused":
                    store.patch("task", task["id"], status="pending", owner=None)
                    if task.get("payload", {}).get("line_id"):
                        store.update_line(research_id, task["payload"]["line_id"], phase="paused")
                    return
                if code == "line_finished":
                    store.finish_task(task["id"], result={"scientific_end_requested": True})
                    if task.get("payload", {}).get("line_id"):
                        store.update_line(research_id, task["payload"]["line_id"], phase="completed")
                    return
                if code == "task_superseded":
                    store.finish_task(task["id"], result={"superseded": True})
                    return
                store.finish_task(task["id"], error={"code": code, "message": message})
                if task.get("payload", {}).get("dependency_wait_id"):
                    store.emit(research_id, "dependency.coordination_failed", {"task_id": task["id"],
                        "dependency_wait_id": task["payload"]["dependency_wait_id"], "code": code, "message": message})
                    return
                if task["kind"] in {"decomposition_part", "recovery_role"}:
                    # The waiting parent owns recovery and its cumulative budget.
                    # A child failure must not close the project's admission gate.
                    store.emit(research_id, "recovery.child_failed", {"task_id": task["id"], "code": code,
                        "message": message}, line_id=task.get("payload", {}).get("line_id"))
                    return
                line_id = task.get("payload", {}).get("line_id")
                if line_id:
                    store.update_line(research_id, line_id, phase="blocked", error={"code": code, "message": message})
                    if code == "design_deadlock":
                        store.add_task(research_id, "boss", {"line_id": line_id, "reason": "design_deadlock",
                            "cycle_id": task["payload"].get("cycle_id"), "details": message}, task["id"] + ":design_deadlock")
                elif code not in {"research_paused"} and task["kind"] != "communication":
                    store.update_project(research_id, status="blocked", error={"code": code, "message": message})
                store.emit(research_id, "task.blocked", {"task_id": task["id"], "code": code, "message": message}, line_id=line_id)
            finally:
                current_task.reset(task_token)
                active.pop(task["id"], None)
        async def execute(task):
            try:
                await execute_once(task)
            except Exception as error:
                # A retryable write can fail inside an exception handler too,
                # including suspension and failure bookkeeping. Keep that task
                # in the same durable recovery path after its execution exits.
                failure = retryable_failure(error)
                if not failure:
                    raise
                deferred_retries[task["id"]] = (task, failure)
        try:
            while not stop.is_set():
                try:
                    store.put("system", "worker", {"status": "running", "pid": os.getpid(), "owner": owner,
                        "heartbeat_at": now(), "active_tasks": len(active), "dsh": engine.live.availability()})
                    await sync_control_stops(store, control)
                    for task_id, (task, failure) in list(deferred_retries.items()):
                        p = store.project(task["research_id"])
                        if p["status"] in {"terminated", "terminating", "completed", "quarantined"}:
                            deferred_retries.pop(task_id, None)
                            continue
                        if await execution_exit_proven(store, control, task):
                            await recover_arbitration_leases(store, control, source_task_id=task_id)
                            saved = store.get("task", task_id)
                            delay = min(30, 2 ** min((saved or {}).get("infrastructure_retries", 0), 5))
                            store.retry_task(task_id, *failure, delay_seconds=delay)
                            deferred_retries.pop(task_id, None)
                    await engine.decompositions.dispatch()
                    await engine.dependencies.dispatch()
                    if time.monotonic() - last_confirmation_recovery >= 5:
                        await asyncio.to_thread(store.recover_confirmed_operations, control)
                        last_confirmation_recovery = time.monotonic()
                    if handoff_drain is not None and handoff_drain.done():
                        await handoff_drain
                        handoff_drain = None
                    if handoff_drain is None and time.monotonic() - last_handoff_drain >= 1:
                        handoff_drain = asyncio.create_task(dispatch_pending_handoffs(store, control))
                        last_handoff_drain = time.monotonic()
                    # Never let unavailable handoff storage/control monopolize
                    # admission of unrelated work; at most one bounded drain runs.
                    if handoff_drain is not None:
                        try:
                            await asyncio.wait_for(asyncio.shield(handoff_drain), timeout=0.05)
                        except asyncio.TimeoutError:
                            pass
                    for task in store.pending_tasks():
                        if len(active) >= capacity:
                            break
                        if task["kind"] == "recovery_role":
                            job = store.get("recovery_evidence", task["payload"]["evidence_job_id"])
                            request = store.get("arbitration_request", job["arbitration_id"]) if job else None
                            if not request or request["status"] != "applied":
                                continue
                        p = store.project(task["research_id"])
                        if p["status"] in {"pausing", "paused", "blocked", "completed", "terminating", "terminated", "quarantined"}:
                            continue
                        if task["kind"] != "supervisor" and not await asyncio.to_thread(control.allows_progress, p["id"], p["control_generation"]):
                            continue
                        claimed = store.claim_task(task["id"], owner)
                        if claimed:
                            active[task["id"]] = asyncio.create_task(execute(claimed))
                    for project in store.projects():
                        if project["status"] in {"running", "blocked"}:
                            project = store.reconcile_project_liveness(project["id"])
                        if project["status"] == "pausing" and not any(t["research_id"] == project["id"] and t["status"] == "running" for t in store.list("task")):
                            store.update_project(project["id"], status="paused")
                            store.emit(project["id"], "research.paused", {"deadline_at": project["deadline_at"]})
                        if project["status"] == "running" and all(line["phase"] == "completed" for line in store.lines(project["id"])):
                            if not any(t["research_id"] == project["id"] and t["status"] in {"pending", "running", "waiting_recovery", "retry_wait"} for t in store.list("task")):
                                store.update_project(project["id"], status="completed", completed_at=now())
                                if hasattr(control, "complete_project"):
                                    await asyncio.to_thread(control.complete_project, project["id"])
                                store.emit(project["id"], "research.completed", {})
                except Exception as error:
                    # Busy or unavailable business storage never blocks the independent stop service.
                    print(json.dumps({"event": "worker_storage_unavailable", "code": type(error).__name__}), flush=True)
                try:
                    await asyncio.wait_for(stop.wait(), timeout=0.4)
                except asyncio.TimeoutError:
                    pass
        finally:
            for task in list(active.values()):
                task.cancel()
            await asyncio.gather(*list(active.values()), return_exceptions=True)
            await engine.close()
            if handoff_drain is not None:
                await handoff_drain


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--control-url", default="http://127.0.0.1:8766")
    parser.add_argument("--runner-url", default="http://127.0.0.1:8767")
    args = parser.parse_args()
    with worker_lock(args.data_dir):
        asyncio.run(run_worker(args.data_dir, args.control_url, args.runner_url))


if __name__ == "__main__":
    main()
