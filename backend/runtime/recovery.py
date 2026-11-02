"""Recovery of infrastructure failures, fenced by Control and process exit proof."""
import asyncio
import json
import sqlite3

import psutil

from backend.persistence.resilience import is_storage_busy
from backend.adapters.model_errors import classify_model_failure


def execution_process_exited(execution, *, control_finished=False):
    """Require a complete cleanup snapshot or an independent terminal receipt.

    A disappeared root PID is insufficient after partial tree cleanup: the
    SDK interpreter or native runtime may still be alive below that launcher.
    """
    if execution.get("exit_confirmed") is True or execution.get("exit_reconciled") is True:
        return True
    if not execution.get("process_tree_complete") and not control_finished:
        return False
    identities = list(execution.get("process_tree") or [])
    if execution.get("pid"):
        identities.append(execution)
    if not identities:
        return control_finished
    for identity in identities:
        try:
            process = psutil.Process(identity["pid"])
            created = identity.get("pid_create_time")
            if created is None or abs(process.create_time() - created) < .1:
                return False
        except psutil.NoSuchProcess:
            continue
        except (psutil.AccessDenied, OSError, TypeError, ValueError, KeyError):
            return False
    return True


def retryable_failure(error):
    from backend.runtime.control import ControlUnavailable
    if isinstance(error, ControlUnavailable) and error.code == "control_unavailable":
        return "local_service_unavailable", "本机控制或执行服务暂时不可用，确认执行状态后自动恢复"
    if is_storage_busy(error):
        return "storage_busy", "数据库写入暂时繁忙，保留检查点并自动重试"
    if getattr(error, "code", None) == "confirmation_pending":
        return "local_service_unavailable", "业务操作等待控制确认，自动重试"
    if getattr(error, "code", None) == "direction_stop_pending":
        return "local_service_unavailable", "等待旧研究方向的执行退出后自动落实新安排"
    if getattr(error, "code", None) == "discussion_technical_failure":
        return "discussion_output_correction", "当前成员回复未通过字段校验，保留已收票，在原逻辑回合内继续纠正"
    if getattr(error, "code", None) == "stream_recorder_unavailable":
        return "stream_recorder_unavailable", "本机流式记录暂时不可用，确认旧执行退出后自动恢复"
    if getattr(error, "code", None) == "execution_unknown":
        return "execution_unknown", "旧模型执行退出尚未确认，继续等待回执后自动恢复"
    return None


def legacy_failure(store, task):
    error = task.get("error") or {}
    if error.get("code") in {"storage_busy", "stream_recorder_unavailable", "local_service_unavailable", "discussion_output_correction", "execution_unknown"}:
        return error["code"], error.get("message", "基础设施恢复")
    if error.get("code") == "OperationalError":
        path = store.data_dir / "checkpoints.sqlite"
        if path.is_file():
            with sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True) as c:
                if not c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='writes'").fetchone():
                    return None
                line_id = task.get("payload", {}).get("line_id")
                prefix = f"research/{task['research_id']}/line/{line_id}/%" if line_id else f"research/{task['research_id']}/{task['kind']}/{task['id']}"
                rows = c.execute("SELECT value FROM writes WHERE thread_id LIKE ? AND channel='__error__' ORDER BY rowid DESC LIMIT 16", (prefix,))
                if any(b"OperationalError('database is locked')" in bytes(row[0]) for row in rows):
                    return "storage_busy", "已从旧检查点确认数据库锁冲突，自动恢复"
    if error.get("code") == "model_execution_failed":
        with store.connect() as c:
            prefix = task["id"] + ":"
            rows = c.execute("SELECT data FROM events WHERE research_id=? AND json_extract(data,'$.payload.notification.payload.event.type')='turn/end' AND (json_extract(data,'$.task_id')=? OR substr(json_extract(data,'$.task_id'),1,?)=?) ORDER BY seq DESC LIMIT 1",
                (task["research_id"], task["id"], len(prefix), prefix))
            for row in rows:
                event = json.loads(row[0])
                reason = event["payload"]["notification"]["payload"]["event"]["data"].get("reason", {})
                if reason.get("kind") == "error" and reason.get("error", {}).get("message") in {
                    "Research stream recorder is unavailable or stopped", "AUTORESEARCH_STREAM_RECORDER_UNAVAILABLE"}:
                    return "stream_recorder_unavailable", "旧记录确认本机流式记录失败，自动恢复"
                # Only the final boundary of this failed task may authorize recovery.
                failure = classify_model_failure([event["payload"]["notification"]["payload"]["event"]])
                if failure["code"] == "model_response_invalid":
                    return "model_response_invalid", "旧记录确认模型响应协议异常，执行退出对账后自动重试"
                return None
    return None


async def execution_exit_proven(store, control, task):
    """Never start a replacement merely because the task or lease is old."""
    project = store.project(task["research_id"])
    if not await asyncio.to_thread(control.allows_progress, project["id"], project["control_generation"]):
        return False
    god_agents = {"god/" + request["id"] for request in store.list("arbitration_request", task["research_id"])
        if request.get("source_task_id") == task["id"]}
    executions = [e for e in store.list("execution", task["research_id"]) if e.get("status") in {"registered", "running", "unknown"}
        and (e.get("task_id") == task["id"] or e.get("task_id", "").startswith(task["id"] + ":") or e.get("agent_id") in god_agents)]
    status = await asyncio.to_thread(control.status, project["id"]) if executions else {}
    for execution in executions:
        execution_id = execution.get("execution_attempt_id", execution["id"])
        registered = next((x for x in status.get("unfinished", []) if x.get("execution_id") == execution_id), None)
        pid = execution.get("pid") or (registered or {}).get("pid")
        identity = {**execution, "pid": pid,
            "pid_create_time": execution.get("pid_create_time", (registered or {}).get("create_time"))}
        if not execution_process_exited(identity, control_finished=registered is None):
            return False
        # Admission registered with the same live Control before spawning; an
        # absent unfinished entry means no start or a durable finished receipt.
        await asyncio.to_thread(control.complete_execution, execution_id)
        store.patch("execution", execution["id"], status="interrupted", exit_reconciled=True, exit_confirmed=True)
    return True


async def recover_infrastructure_tasks(store, control):
    recovered = []
    for task in store.list("task", status="failed"):
        project = store.project(task["research_id"])
        if project["status"] not in {"running", "blocked"} or project.get("stop_reason"):
            continue
        if project["status"] == "blocked" and project.get("blocked_reason") != "all_lines_blocked" and (project.get("error") or {}).get("code") not in {"OperationalError", "storage_busy", "model_execution_failed", "stream_recorder_unavailable", "local_service_unavailable", "execution_unknown"}:
            continue
        payload = task.get("payload", {})
        if payload.get("line_id"):
            line = store.get("line", f"{project['id']}:{payload['line_id']}")
            if not line or line.get("finish_requested") or line["direction_epoch"] != payload.get("direction_epoch", line["direction_epoch"]) or line.get("cycle_id") != payload.get("cycle_id", line.get("cycle_id")):
                continue
        failure = legacy_failure(store, task)
        if failure and await execution_exit_proven(store, control, task):
            store.assert_progress(project)
            store.retry_task(task["id"], *failure, delay_seconds=0)
            if project["status"] == "blocked" and project.get("blocked_reason") != "all_lines_blocked":
                store.update_project(project["id"], status="running", error=None, blocked_reason=None)
            recovered.append(task["id"])
    return recovered
