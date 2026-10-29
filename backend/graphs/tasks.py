"""Finite jobs have separate durable threads; no supervisor waits on all lines."""
from typing import TypedDict
from contextvars import ContextVar

from langgraph.graph import END, START, StateGraph
from langgraph.types import Command


# The durable business task stays fixed even inside nested role/discussion graphs.
current_task = ContextVar("research_current_task", default=None)




class TaskSuspended(Exception):
    def __init__(self, root_task_id, graph_thread_id, resume_token):
        super().__init__("等待恢复子任务")
        self.root_task_id, self.graph_thread_id, self.resume_token = root_task_id, graph_thread_id, resume_token


class ScopeResolved(Exception):
    def __init__(self, arbitration_id=None, recovery_chain_id=None, diagnostic=None):
        super().__init__("投票输出修复失败，已转为讨论仲裁")
        self.arbitration_id = arbitration_id
        self.recovery_chain_id, self.diagnostic = recovery_chain_id, diagnostic


class RecoveryOutcome(dict):
    """Validated, persisted result used to resume a known waiting node."""


class TaskState(TypedDict, total=False):
    task: dict
    result: dict


class DurableTaskGraph:
    kind = "task"

    def __init__(self, operation, checkpointer):
        async def execute(state):
            return {"result": await operation(state["task"])}
        graph = StateGraph(TaskState)
        graph.add_node("execute", execute)
        graph.add_edge(START, "execute")
        graph.add_edge("execute", END)
        self.graph = graph.compile(checkpointer=checkpointer)

    async def run(self, task):
        config = {"configurable": {"thread_id": f"research/{task['research_id']}/{self.kind}/{task['id']}"}, "recursion_limit": 100000}
        snapshot = await self.graph.aget_state(config)
        interrupted = any(t.interrupts for t in snapshot.tasks)
        if snapshot.values and not snapshot.next and not interrupted and "result" in snapshot.values:
            return snapshot.values["result"]
        initial = Command(resume=task["recovery_outcome"]) if task.get("recovery_outcome") else None if snapshot.values else {"task": task}
        result = await self.graph.ainvoke(initial, config)
        if result.get("__interrupt__"):
            payload = result["__interrupt__"][0].value
            raise TaskSuspended(task["id"], config["configurable"]["thread_id"], payload["resume_token"])
        return result["result"]


class ResearchSupervisorGraph(DurableTaskGraph):
    kind = "supervisor"


class BossDecisionGraph(DurableTaskGraph):
    kind = "boss"


class OrganizerGraph(DurableTaskGraph):
    kind = "organizer"


class PaperGraph(DurableTaskGraph):
    kind = "paper"


class CommunicationGraph(DurableTaskGraph):
    kind = "communication"


class DecompositionPartGraph(DurableTaskGraph):
    kind = "decomposition_part"


class RecoveryRoleGraph(DurableTaskGraph):
    kind = "recovery_role"
