"""Durable generation limits shared by a logical speech, ballot or observation.

An occupied slot is never sent twice. Adapter execution receipts can recover an
answer after a crash, but an unconfirmed occupied slot still consumes the limit.
"""
from __future__ import annotations

from backend.adapters.dsh import RuntimeBlocked
from packages.contracts.research import digest


class GenerationBudget:
    def __init__(self, store, research_id, binding):
        self.store, self.research_id = store, research_id
        self.binding = binding
        self.key = digest([research_id, binding.get("kind", "generation"), binding["key"]])
        self.limit = min(3, max(1, int(binding.get("limit", 3))))

    def acquire(self, agent_id, task_id, turn_id):
        with self.store.connect(True) as connection:
            state = self.store._get(connection, "generation_budget", self.key) or {
                "binding": self.binding, "attempts": []}
            attempts = state["attempts"]
            if attempts and attempts[-1]["status"] in {"reserved", "returned", "accepted", "unconfirmed"}:
                attempt = attempts[-1]
                execution = self.store._get(connection, "execution", attempt["execution_key"])
                if not execution:
                    execution = self.store._get(connection, "execution", "demo-" + attempt["execution_key"])
                if attempt.get("output") is not None:
                    return attempt, attempt["output"]
                if execution and execution.get("status") == "completed":
                    attempt.update(status="returned", output=execution["output"])
                    self.store._put(connection, "generation_budget", self.key, state, self.research_id)
                    return attempt, attempt["output"]
                if execution and execution.get("status") in {"registered", "running", "unknown"}:
                    raise RuntimeBlocked("execution_unknown", "已占用的生成尝试仍待执行回执，继续跟踪原执行")
                attempt.update(status="unconfirmed", error="已占号但未找到完整回执；不重发该次生成")
            if len(attempts) >= self.limit:
                self.store._put(connection, "generation_budget", self.key, state, self.research_id)
            else:
                number = len(attempts) + 1
                bound_turn = turn_id + ":generation-" + str(number)
                attempt = {"attempt_id": self.key + ":" + str(number), "number": number,
                    "status": "reserved", "agent_id": agent_id, "task_id": task_id, "turn_id": bound_turn,
                    "execution_key": digest([self.research_id, agent_id, task_id, bound_turn])}
                attempts.append(attempt)
                self.store._put(connection, "generation_budget", self.key, state, self.research_id)
                return attempt, None
        # Raise after committing the last occupied slot's audit state.
        raise RuntimeBlocked("generation_budget_exhausted", "同一逻辑回合的三次生成已耗尽，已保留原回复与字段错误")

    def release_unstarted_service_failure(self, attempt, error):
        """A known service error before adapter registration is not a generation.

        A crashed/unknown reservation never uses this path: only the live caller
        with an explicit infrastructure exception may establish non-dispatch.
        """
        with self.store.connect(True) as connection:
            execution = self.store._get(connection, "execution", attempt["execution_key"])
            execution = execution or self.store._get(connection, "execution", "demo-" + attempt["execution_key"])
            if execution:
                return False
            state = self.store._get(connection, "generation_budget", self.key)
            if state["attempts"][-1]["attempt_id"] != attempt["attempt_id"]:
                return False
            released = state["attempts"].pop()
            state.setdefault("infrastructure_failures", []).append({**released,
                "status": "not_dispatched", "error": str(error)})
            self.store._put(connection, "generation_budget", self.key, state, self.research_id)
            return True

    def record(self, attempt, status, **fields):
        with self.store.connect(True) as connection:
            state = self.store._get(connection, "generation_budget", self.key)
            saved = next(item for item in state["attempts"] if item["attempt_id"] == attempt["attempt_id"])
            saved.update(status=status, **fields)
            self.store._put(connection, "generation_budget", self.key, state, self.research_id)

    def corrections(self):
        state = self.store.get("generation_budget", self.key) or {}
        return [{"attempt_id": item["attempt_id"], "error": item.get("error"), "output": item.get("output")}
            for item in state.get("attempts", []) if item.get("error")]
