"""Durable task retry and project liveness; no model or control calls in transactions."""
from datetime import datetime, timedelta, timezone


class RuntimeRecoveryStoreMixin:

    def effective_boss_task_id(self, task_id):
        while revision := self.get("boss_reconsideration", task_id):
            task_id = revision["replacement_task"]["id"]
        return task_id

    def complete_model_turn(self, execution_key, output, *, payload, fields=None, session_id=None):
        """Cache the result, session history and completion event as one fact."""
        from backend.persistence.store import now
        with self.connect(True) as c:
            execution = self._get(c, "execution", execution_key)
            if execution["status"] == "completed":
                return execution
            rid = execution["research_id"]
            result = self._put(c, "execution", execution_key, {**execution, **(fields or {}),
                "status": "completed", "output": output, "completed_at": now()}, rid)
            if session_id:
                session = self._get(c, "dsh_session", session_id)
                self._put(c, "dsh_session", session_id, {**session, "has_history": True}, rid)
            self._emit(c, rid, "agent.completed", payload, agent_id=execution["agent_id"],
                execution_id=execution["execution_attempt_id"], task_id=execution["task_id"])
            return result

    def retry_task(self, task_id, code, message, *, delay_seconds=1):
        from backend.persistence.store import now
        with self.connect(True) as c:
            task = self._get(c, "task", task_id)
            if not task or task["status"] in {"completed", "cancelled", "waiting_recovery"}:
                return task
            project = self._project(c, task["research_id"])
            self.assert_progress(project, task.get("control_generation"))
            result = self._put(c, "task", task_id, {**task, "status": "retry_wait", "owner": None,
                "error": None, "last_error": task.get("error") or {"code": code, "message": message},
                "execution_phase": "retry_wait", "infrastructure_retries": task.get("infrastructure_retries", 0) + 1,
                "retry_not_before": (datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)).isoformat().replace("+00:00", "Z")}, task["research_id"])
            line_id = task.get("payload", {}).get("line_id")
            if line_id:
                key = f"{task['research_id']}:{line_id}"
                line = self._get(c, "line", key)
                if line and not line.get("finish_requested"):
                    self._put(c, "line", key, {**line, "phase": "retry_wait", "error": None}, task["research_id"])
                    self._emit(c, task["research_id"], "line.phase_changed", {"phase": "retry_wait"}, line_id=line_id)
            self._emit(c, task["research_id"], "task.retry_wait", {"task_id": task_id, "code": code,
                "message": message, "retry_not_before": result["retry_not_before"]}, line_id=line_id)
            return result

    def reconcile_project_liveness(self, research_id):
        """An idle set of failed lines is blocked, even if the Worker is alive."""
        import json
        with self.connect(True) as c:
            project = self._project(c, research_id)
            aggregate_block = project.get("blocked_reason") == "all_lines_blocked" and (project.get("error") or {}).get("code") == "all_lines_blocked"
            if project["status"] != "running" and not (project["status"] == "blocked" and aggregate_block):
                return project
            self.assert_progress(project)
            docs = [(row[0], json.loads(row[1])) for row in c.execute("SELECT kind,data FROM documents WHERE research_id=? AND kind IN ('task','line','run','light_job')", (research_id,))]
            tasks = [v for k, v in docs if k == "task"]
            lines = [v for k, v in docs if k == "line"]
            live = any(t["status"] in {"pending", "running", "retry_wait"} for t in tasks) or any(
                k in {"run", "light_job"} and v.get("state") in {"starting", "running", "registered"} for k, v in docs)
            if aggregate_block and live:
                result = self._update_project(c, research_id, status="running", blocked_reason=None, error=None)
                self._emit(c, research_id, "research.resumed", {"reason": "automatic_recovery", "deadline_at": project.get("deadline_at")})
                return result
            if not live and lines and all(line["phase"] in {"blocked", "completed", "waiting_dependencies"} for line in lines) and any(line["phase"] != "completed" for line in lines):
                details = [{"line_id": line["line_id"], "phase": line["phase"], "error": line.get("error")} for line in lines if line["phase"] != "completed"]
                if aggregate_block:
                    return project
                error = {"code": "all_lines_blocked", "message": "所有未完成研究线均已阻塞，当前没有可执行任务", "lines": details}
                result = self._update_project(c, research_id, status="blocked", blocked_reason="all_lines_blocked", error=error)
                self._emit(c, research_id, "research.blocked", {"reason": "all_lines_blocked", "error": error})
                return result
            return project
