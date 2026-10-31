"""Execute one durable, explicit Boss plan using idempotent business effects."""
from __future__ import annotations

import asyncio
import json

from backend.adapters.dsh import RuntimeBlocked
from packages.contracts.research import digest


def normalize_operations(final_output, next_step, lines):
    """Resolve user-facing targets and reject conflicts before any vote or effect."""
    catalog = {}
    for line in lines:
        for name in (line["line_id"], line.get("title"), line.get("label")):
            if name:
                catalog.setdefault(name, []).append(line["line_id"])

    def target(value):
        matches = set(catalog.get(value, []))
        if len(matches) != 1:
            raise ValueError(f"Boss 操作须选择目录中明确的一条研究线：{value}")
        return matches.pop()

    raw = final_output.get("operations", [])
    if next_step == "report":
        raw = [{"operation": "report"}]
    elif next_step == "finish_lines":
        raw = [{"operation": "finish_line", "target": item} for item in final_output.get("targets", [])]
        if not raw:
            raise ValueError("结束研究线必须明确提供 targets")
    elif next_step == "finish_project":
        if raw:
            raise ValueError("结束项目不能同时启动新的操作")
        return []
    if not isinstance(raw, list) or (next_step == "dispatch" and not raw):
        raise ValueError("Boss dispatch 必须提供 operations 列表")
    result, by_line, seen = [], {}, set()
    for item in raw:
        if not isinstance(item, dict):
            raise ValueError("Boss operation 必须是对象")
        kind = item.get("operation")
        if kind not in {"research", "change_direction", "finish_line", "report"}:
            raise ValueError("未知的 Boss operation")
        operation = {"operation": kind}
        if kind != "report":
            identity = target(item.get("target"))
            operation["target"] = identity
            objective = item.get("objective", "")
            if not isinstance(objective, str):
                raise ValueError("研究任务 objective 必须是正文")
            line = next(line for line in lines if line["line_id"] == identity)
            if (kind == "change_direction" or (kind == "research" and not line.get("cycle_id"))) and not objective.strip():
                raise ValueError("首次分派或换方向须提供研究目标 objective")
            if objective.strip():
                operation["objective"] = objective
            if kind in {"research", "change_direction"} and line.get("finish_requested"):
                raise ValueError("已结束的研究线不能重新分派")
            previous = by_line.get(identity)
            if previous and previous != operation:
                raise ValueError(f"同一研究线有冲突的操作，请作者明确意图：{identity}")
            by_line[identity] = operation
        fingerprint = digest(operation)
        if fingerprint not in seen:
            seen.add(fingerprint)
            result.append(operation)
    return result


class BossOperationService:
    def __init__(self, engine):
        self.engine, self.store = engine, engine.store

    async def stop_line(self, research_id, line_id, operation_id):
        """Latch the old direction closed, then prove its executors have exited."""
        from backend.persistence.store import now
        with self.store.connect(True) as connection:
            key = f"{research_id}:{line_id}"
            line = self.store._get(connection, "line", key)
            if line.get("direction_stop_operation") != operation_id:
                self.store._put(connection, "line", key, {**line,
                    "finish_requested": True, "direction_stop_operation": operation_id,
                    "direction_stop_epoch": line["direction_epoch"]}, research_id)
                for row in connection.execute("SELECT id,data FROM documents WHERE kind='task' AND research_id=?", (research_id,)).fetchall():
                    task = json.loads(row["data"])
                    if task.get("kind") == "line" and task.get("payload", {}).get("line_id") == line_id and task.get("status") in {"pending", "running", "retry_wait", "waiting_recovery"}:
                        self.store._put(connection, "task", row["id"], {**task, "status": "cancelled",
                            "cancelled_at": now(), "cancel_reason": "direction_changed"}, research_id)
                self.store._cancel_recovery(connection, research_id, line_id, reason="direction_changed")
        for execution in self.store.list("execution", research_id):
            if execution.get("agent_id", "").startswith(line_id + "/") and execution.get("status") in {"registered", "running", "unknown"}:
                stopped = await self.engine.live.cancel_execution(execution.get("execution_attempt_id", execution["id"]))
                if not stopped.get("exit_confirmed"):
                    raise RuntimeBlocked("direction_stop_pending", "等待旧方向模型执行退出，暂不创建新方向")
                self.store.patch("execution", execution["id"], status="interrupted")
        for run in self.store.list("run", research_id):
            if run.get("line_id") != line_id or run.get("state") not in {"starting", "running", "stopping", "reconciling", "unknown"}:
                continue
            if self.engine.runner is None:
                raise RuntimeBlocked("direction_stop_pending", "无法确认旧实验退出")
            await asyncio.to_thread(self.engine.runner.request_stop, run["id"], "direction_changed")
            latest = await asyncio.to_thread(self.engine.runner.status, run["id"])
            proof = latest.get("terminal_proof", {})
            if not proof.get("process_tree_exited") or not proof.get("start_operation_terminal"):
                raise RuntimeBlocked("direction_stop_pending", "等待旧方向实验退出，暂不创建新方向")
            self.store.put("run", run["id"], latest, research_id)

    async def apply(self, task, discussion, context):
        research_id, task_id = task["research_id"], task["id"]
        saved = self.store.get("boss_operation_plan", task_id)
        if saved:
            final_output, operations = saved["final_output"], saved["operations"]
            next_step = saved["next_step"]
        else:
            final_output, next_step = discussion["final_output"], discussion["next_step"]
            operations = normalize_operations(final_output, next_step, context["lines"])
            # The immutable whole plan precedes every individual effect.
            with self.store.connect(True) as connection:
                saved = self.store._get(connection, "boss_operation_plan", task_id)
                if not saved:
                    saved = self.store._put(connection, "boss_operation_plan", task_id, {
                        "protocol_version": 2, "final_output": final_output, "next_step": next_step,
                        "operations": operations, "status": "pending", "discussion_id": discussion.get("discussion_id"),
                        "report_snapshot": {"submission": context.get("submission"),
                            "submissions": self.store.list("submission", research_id),
                            "verifications": self.store.list("verification", research_id),
                            "cycle_history": context.get("cycle_history", [])}}, research_id)
                    handoff_id = discussion.get("discussion_id")
                    handoff = self.store._get(connection, "discussion_handoff", handoff_id) if handoff_id else None
                    if handoff:
                        self.store._put(connection, "discussion_handoff", handoff_id,
                            {**handoff, "status": "delivered", "delivered_to": task_id}, research_id)
        decision = {"protocol_version": 2, "next_step": next_step, **final_output, "operations": operations}
        confirmed = await asyncio.to_thread(self.store.commit_boss_decision, research_id, task_id, decision, self.engine.control)
        if confirmed.get("status") != "applied":
            raise RuntimeBlocked("confirmation_pending", "Boss 操作计划等待持久控制确认")
        if next_step == "finish_project":
            operations = [{"operation": "finish_project"}]
        for index, operation in enumerate(operations):
            await self.engine.guard(research_id)
            key = f"{task_id}:operation:{index}"
            record = self.store.get("boss_operation", key)
            if record and record.get("status") == "applied":
                continue
            self.store.put("boss_operation", key, {"operation": operation, "status": "applying", "plan_id": task_id}, research_id)
            try:
                effect = await self.apply_one(research_id, key, operation, saved, decision)
            except Exception as error:
                waiting = getattr(error, "code", None) in {"direction_stop_pending", "confirmation_pending"}
                with self.store.connect(True) as connection:
                    record = self.store._get(connection, "boss_operation", key)
                    self.store._put(connection, "boss_operation", key, {**record,
                        "status": "waiting" if waiting else "failed", "error": str(error)}, research_id)
                    self.store._create_message(connection, research_id, saved.get("discussion_id") or task_id,
                        {"type": "group", "id": "boss"}, ("Boss 操作等待落实：" if waiting else "Boss 操作执行失败：") +
                        operation["operation"] + (" · " + operation["target"] if operation.get("target") else "") + "\n\n" + str(error),
                        kind="operation_status", source_system="boss_operations", source_event_id=key + ":failed:" + digest(str(error))[:12],
                        operation_id=key, operation_status="waiting" if waiting else "failed")
                raise
            with self.store.connect(True) as connection:
                record = self.store._get(connection, "boss_operation", key)
                self.store._put(connection, "boss_operation", key, {**record, "status": "applied", "effect": effect, "error": None}, research_id)
                descriptions = {"research": "已安排研究任务；实验结果以后续实际执行和验证为准。",
                    "change_direction": "旧方向已停止，已安排新方向的研究任务。", "finish_line": "已结束研究线，历史成果保留。",
                    "report": "已安排报告生成；报告尚需 Paper 实际完成。", "finish_project": "已结束项目科研活动，保留已有成果。"}
                content = "已复用现有研究任务，未创建重复周期。" if effect.get("status") == "reused" else descriptions[operation["operation"]]
                self.store._create_message(connection, research_id, saved.get("discussion_id") or task_id,
                    {"type": "group", "id": "boss"}, content + ("\n\n研究线：" + operation["target"] if operation.get("target") else ""),
                    kind="operation_status", source_system="boss_operations", source_event_id=key + ":applied",
                    operation_id=key, operation_status="applied")
        # Optional knowledge carries sources; system-owned identities are bound here.
        updates = final_output.get("memory_updates", [])
        if updates:
            async with self.engine.boss_memory_locks.setdefault(research_id, asyncio.Lock()):
                project = await self.engine.guard(research_id)
                from packages.contracts.research import SharedKnowledge
                normalized = [{**SharedKnowledge.model_validate(item).model_dump(exclude_none=True),
                    "node_id": f"boss/{digest([task_id, i])[:24]}", "parent_id": "research",
                    "title": item.get("title") or "共享研究知识", "status": item.get("status") or "unverified"}
                    for i, item in enumerate(updates)]
                memory = await asyncio.to_thread(self.store.write_memory, research_id, task_id, "updates",
                    normalized, project["memory_version"], self.engine.control)
                if memory.get("status") != "applied":
                    raise RuntimeBlocked("confirmation_pending", "共享知识等待持久控制确认")
        self.store.patch("boss_operation_plan", task_id, status="applied")
        return decision

    async def apply_one(self, research_id, key, operation, saved, decision):
        kind, line_id = operation["operation"], operation.get("target")
        line = self.store.get("line", f"{research_id}:{line_id}") if line_id else None
        if kind == "research":
            active = [task for task in self.store.list("task", research_id) if task.get("kind") == "line"
                and task.get("payload", {}).get("line_id") == line_id
                and task.get("payload", {}).get("cycle_id") == line.get("cycle_id")
                and task.get("status") in {"pending", "running", "retry_wait", "waiting_recovery"}]
            if active:
                # New instructions are consumed by a later applicable design step.
                if operation.get("objective"):
                    self.store.put("line_instruction", key, {"line_id": line_id, "cycle_id": line.get("cycle_id"),
                        "content_markdown": operation["objective"], "applies_after_active_experiment": True}, research_id)
                return {"status": "reused", "task_ids": [task["id"] for task in active]}
        if kind == "change_direction":
            previous = self.store.get("boss_effects", key)
            if not previous:
                await self.stop_line(research_id, line_id, key)
                await self.engine.guard(research_id)
                # The old executors are gone; the new direction may now be admitted.
                self.store.update_line(research_id, line_id, finish_requested=False, scientific_end_requested=False)
        action = {"research": "continue", "change_direction": "change_direction", "finish_line": "finish_line",
            "report": "report", "finish_project": "finish_project"}[kind]
        legacy = {"content_markdown": decision["content_markdown"], "action": action,
            "assignments": [{"line_id": line_id, "objective": operation.get("objective") or line.get("objective") or
                self.store.get("cycle", line["cycle_id"])["objective"], "priority": 0}] if kind in {"research", "change_direction"} else [],
            "rationale": decision["content_markdown"], "request_report": kind == "report", "memory_updates": []}
        confirmed = await asyncio.to_thread(self.store.commit_boss_decision, research_id, key, legacy, self.engine.control)
        if confirmed.get("status") != "applied":
            raise RuntimeBlocked("confirmation_pending", "Boss 子操作等待持久控制确认")
        snapshot = {**saved["report_snapshot"], "decision": decision,
            "arrangements": [{"operation": item, "status": (self.store.get("boss_operation", f"{saved['id']}:operation:{i}") or {}).get("status", "pending")}
                for i, item in enumerate(saved["operations"])],
            "notice": "新安排是计划或待办；仅提交包和验证记录代表已经提交的成果。"}
        effect = await asyncio.to_thread(self.store.apply_boss_effects, research_id, key, line_id, snapshot, self.engine.control)
        if effect.get("status") != "applied":
            raise RuntimeBlocked("confirmation_pending", "Boss 子操作尚未派发")
        if kind in {"finish_line", "finish_project"}:
            for target in effect.get("finish_line_ids", []):
                await self.stop_line(research_id, target, key)
        return effect
