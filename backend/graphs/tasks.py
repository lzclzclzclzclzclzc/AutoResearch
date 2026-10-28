"""Finite jobs have separate durable threads; no supervisor waits on all lines."""
from typing import TypedDict

from langgraph.graph import END, START, StateGraph


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
        if snapshot.values and not snapshot.next:
            return snapshot.values["result"]
        result = await self.graph.ainvoke(None if snapshot.values else {"task": task}, config)
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
