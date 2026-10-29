from __future__ import annotations

import asyncio
import math
import random
from typing import Annotated, Any, TypedDict

from langgraph.graph import END, START, StateGraph

from backend.adapters.dsh import RuntimeBlocked
from packages.contracts.research import BossProposal, BossReview, Proposal, Review, Synthesis, Vote, Willingness, digest, majority, validate_issue_transition, validate_vote


def merge_records(left, right):
    return {**(left or {}), **(right or {})}


def select_speaker(members, eligible, scores, heat, last_speaker, seed):
    """KDS policy: softmax((score - 2 * heat) / 1.5), no consecutive speaker."""
    allowed = [m for m in members if m in eligible]
    if len(allowed) > 1:
        allowed = [m for m in allowed if m != last_speaker]
    if not allowed:
        raise ValueError("No eligible discussion member")
    logits = {m: (scores[m] - 2.0 * heat.get(m, 0.0)) / 1.5 for m in allowed}
    peak = max(logits.values())
    weights = {m: math.exp(value - peak) for m, value in logits.items()}
    total = sum(weights.values())
    probabilities = {m: weights.get(m, 0.0) / total for m in members}
    selected = random.Random(seed).choices(allowed, weights=[weights[m] for m in allowed], k=1)[0]
    return selected, probabilities


class DiscussionState(TypedDict, total=False):
    research_id: str
    line_id: str
    discussion_id: str
    role: str
    members: list[str]
    context: dict[str, Any]
    phase: str
    round: int
    member: str
    records: Annotated[dict[str, dict], merge_records]
    candidates: dict
    reviewed_candidates: dict
    winner: str | None
    result: dict
    prior_review_hash: str
    focused: bool
    history_snapshot: list[dict]
    heat: dict[str, float]
    last_speaker: str | None
    scores: dict[str, int]


class GroupDiscussionGraph:
    def __init__(self, engine, checkpointer):
        self.engine = engine
        self.locks = {}
        graph = StateGraph(DiscussionState)
        for node in ("prepare", "score_members", "choose_speaker", "member_turn", "collect", "synthesize"):
            graph.add_node(node, getattr(self, node))
        graph.add_edge(START, "prepare")
        graph.add_edge("prepare", "score_members")
        graph.add_edge("score_members", "choose_speaker")
        graph.add_edge("choose_speaker", "member_turn")
        graph.add_edge("member_turn", "collect")
        graph.add_conditional_edges("collect", lambda s: "synthesize" if s.get("winner") else "again",
                                    {"again": "prepare", "synthesize": "synthesize"})
        graph.add_conditional_edges("synthesize", lambda s: "done" if s.get("result") else "again",
                                    {"done": END, "again": "prepare"})
        self.graph = graph.compile(checkpointer=checkpointer)

    def eligible(self, state):
        return [m for m in state["members"] if f"{state['phase']}:{state['round']}:{m}" not in state.get("records", {})]

    def selection_key(self, state):
        completed = len(state["members"]) - len(self.eligible(state))
        return f"{state['discussion_id']}:select:{state['phase']}:{state['round']}:{completed}"

    async def prepare(self, state):
        await self.engine.guard(state["research_id"])
        self.engine.store.create_conversation(state["research_id"], state["discussion_id"], kind="group_discussion",
            title=state["role"] + " 组内讨论", participants=[{"type": "agent", "id": m} for m in state["members"]])
        messages = self.engine.store.messages(state["discussion_id"], limit=24)["items"]
        snapshot = [{"message_id": m["message_id"], "speaker": m["sender"]["id"], "text": m.get("content_markdown", "")[-4000:]} for m in messages]
        return {"history_snapshot": snapshot}

    async def score_members(self, state):
        # All private scorers receive exactly the same frozen transcript.
        async def score(member):
            output = await self.engine.turn(state["research_id"], member, state["role"], state["discussion_id"],
                self.selection_key(state) + ":willingness", Willingness,
                {"phase": state["phase"], "topic": state["context"].get("topic", ""),
                 "instruction": "根据是否有相关观点、证据或需要回应的问题，评估现在发言的意愿。",
                 "conversation_snapshot": [{**m, "text": m["text"][-1500:]} for m in state["history_snapshot"][-8:]]},
                conversation_id=state["discussion_id"], line_id=state.get("line_id"), publish_message=False)
            return member, output["score"]
        tasks = [asyncio.create_task(score(m)) for m in state["members"]]
        try:
            return {"scores": dict(await asyncio.gather(*tasks))}
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def choose_speaker(self, state):
        await self.engine.guard(state["research_id"])
        key = self.selection_key(state)
        store = self.engine.store
        # Commit selection before speaking. Crash recovery must not redraw the speaker.
        with store.connect(True) as c:
            selection = store._get(c, "discussion_selection", key)
            if selection is None:
                seed = digest(key)
                speaker, probabilities = select_speaker(state["members"], self.eligible(state), state["scores"],
                    state.get("heat", {}), state.get("last_speaker"), seed)
                selection = store._put(c, "discussion_selection", key, {"speaker": speaker, "scores": state["scores"],
                    "probabilities": probabilities, "heat": state.get("heat", {}), "seed": seed,
                    "phase": state["phase"], "round": state["round"], "discussion_id": state["discussion_id"]}, state["research_id"])
                store._emit(c, state["research_id"], "discussion.speaker_selected", {"selection_id": key, "speaker": speaker},
                            conversation_id=state["discussion_id"], line_id=state.get("line_id"))
        return {"member": selection["speaker"]}

    def issues(self, state):
        latest = {}
        for key, output in state.get("records", {}).items():
            owner = key.split(":", 2)[2]
            for issue in output.get("issues", []):
                identity = (owner, issue["issue_id"])
                try:
                    previous = latest.get(identity)
                    previous = {k: v for k, v in previous.items() if k != "raised_by"} if previous else None
                    checked = validate_issue_transition(previous, issue,
                        lambda source: self.engine.store.source_exists(state["research_id"], source))
                except ValueError:
                    # Old/replayed invalid reclassifications never erase the last valid blocker.
                    continue
                latest[identity] = {**checked, "raised_by": owner}
        return list(latest.values())

    def blockers(self, state):
        return [i for i in self.issues(state) if (i["category"] == "must_fix" and not i["resolved"]) or
                (i["category"] == "experimental_disagreement" and not i["experiment_mapping"])]

    async def member_turn(self, state):
        phase, member = state["phase"], state["member"]
        schema = {"proposal": Proposal, "review": Review, "vote": Vote}[phase]
        if state["role"] == "boss":
            schema = BossProposal if phase == "proposal" else BossReview if phase == "review" else Vote
        candidates = state.get("candidates", {})
        # Bind validation to this round's actual candidates, not merely all member IDs.
        vote_candidates, vote_members = set(candidates), list(state["members"])
        def validate_member_output(output):
            if phase == "vote":
                validate_vote(output, member, vote_members, vote_candidates)
        context = {**state["context"], "instruction": {
            "proposal": "提出完整候选方案与依据。直接说观点，不列出已读消息。",
            "review": "回应最新观点并解决阻断项。可通过 revised_candidate 完整修订自己的候选方案；保持 issue_id，解决项需理由和依据。证据不足可以收窄任务或请求证据，不能虚构结果。",
            "vote": "从当前候选成员 ID 选择一项，仅代表自己的票，说明理由。"}[phase],
            "proposals": {m: {"candidate": candidate} for m, candidate in candidates.items()},
            "issues": self.issues(state), "conversation_snapshot": state["history_snapshot"], "focused": state.get("focused", False)}
        output = await self.engine.turn(state["research_id"], member, state["role"], state["discussion_id"],
            f"{phase}:{state['round']}", schema, context, conversation_id=state["discussion_id"], line_id=state.get("line_id"),
            output_validator=validate_member_output)
        candidates = dict(candidates)
        if phase == "proposal":
            candidates[member] = output["candidate"]
        elif phase == "review" and output.get("revised_candidate") is not None:
            candidates[member] = output["revised_candidate"]
        reviewed = dict(state.get("reviewed_candidates", {}))
        if phase == "review":
            reviewed[member] = digest(candidates)
        heat = {m: state.get("heat", {}).get(m, 0) * 0.7 + int(m == member) for m in state["members"]}
        return {"records": {f"{phase}:{state['round']}:{member}": output}, "candidates": candidates,
                "reviewed_candidates": reviewed, "heat": heat, "last_speaker": member}

    async def collect(self, state):
        if self.eligible(state):
            return {}
        phase, round_no, members = state["phase"], state["round"], state["members"]
        current = {m: state["records"][f"{phase}:{round_no}:{m}"] for m in members}
        if phase == "proposal":
            return {"phase": "review", "round": 1}
        if phase == "review":
            issue_progress = [{k: i[k] for k in ("raised_by", "issue_id", "category", "resolved", "experiment_mapping")}
                              for i in sorted(self.issues(state), key=lambda i: (i["raised_by"], i["issue_id"]))]
            fingerprint = digest({"candidates": state["candidates"], "issues": issue_progress,
                "reviews": {m: {k: v[k] for k in ("ready_for_vote", "new_evidence")} for m, v in current.items()}})
            consistent = all(state.get("reviewed_candidates", {}).get(m) == digest(state["candidates"]) for m in members)
            if all(v["ready_for_vote"] for v in current.values()) and not self.blockers(state) and consistent:
                return {"phase": "vote", "prior_review_hash": fingerprint}
            if fingerprint == state.get("prior_review_hash"):
                problems = "；".join(i["issue_id"] + "：" + i["description"][:180] for i in self.blockers(state))
                raise RuntimeBlocked("design_deadlock", "修订讨论后仍无进展，需要补充证据或调整任务。" + problems)
            return {"phase": "review", "round": round_no + 1, "prior_review_hash": fingerprint}
        winner = majority({m: v["candidate_id"] for m, v in current.items()}, members, set(state["candidates"]))
        if winner:
            return {"winner": winner}
        if state.get("focused"):
            raise RuntimeBlocked("decision_blocked", "聚焦讨论与再次投票后仍无严格多数，需要用户处理")
        return {"phase": "review", "round": round_no + 1, "focused": True}

    async def synthesize(self, state):
        synthesis_state = {**state, "phase": "synthesis", **await self.prepare(state)}
        synthesis_state.update(await self.score_members(synthesis_state))
        host = (await self.choose_speaker(synthesis_state))["member"]
        candidate = state["candidates"][state["winner"]]
        result = await self.engine.turn(state["research_id"], host, state["role"], state["discussion_id"],
            f"synthesis:{state['round']}", Synthesis, {**state["context"],
            "instruction": "根据程序统计的多数票输出决议，保留少数意见与阻断项。Boss 的 decision 必须完整原样等于 majority_candidate。",
            "winner": state["winner"], "majority_candidate": candidate, "issues": self.issues(state),
            "conversation_snapshot": synthesis_state["history_snapshot"]},
            conversation_id=state["discussion_id"], line_id=state.get("line_id"), message_kind="decision")
        if result["selected_candidate_id"] != state["winner"] or (state["role"] == "boss" and result["decision"] != candidate):
            raise RuntimeBlocked("invalid_group_decision", "主持人决议与真实多数票选定的候选方案不符")
        records = {f"synthesis:{state['round']}:{host}": result}
        speaking = {"heat": {m: state.get("heat", {}).get(m, 0) * 0.7 + int(m == host) for m in state["members"]},
                    "last_speaker": host}
        if self.blockers({**state, "records": {**state["records"], **records}}):
            return {**speaking, "records": records, "winner": None, "phase": "review", "round": state["round"] + 1}
        result["selected_payload"] = candidate
        self.engine.store.put("discussion", state["discussion_id"], {"members": state["members"], "records": state["records"],
            "candidates": state["candidates"], "winner": state["winner"], "authored_by_agent_id": host, "result": result}, state["research_id"])
        return {**speaking, "result": result}

    async def run(self, research_id, discussion_id, role, members, context, line_id=None):
        async with self.locks.setdefault((research_id, role, line_id), asyncio.Lock()):
            discussion_id += ":serial-v2"
            config = {"configurable": {"thread_id": f"research/{research_id}/discussion/{discussion_id}"}, "recursion_limit": 100000}
            snapshot = await self.graph.aget_state(config)
            initial = None if snapshot.values and snapshot.next else {"research_id": research_id, "discussion_id": discussion_id,
                "role": role, "members": members, "context": context, "line_id": line_id, "records": {}, "phase": "proposal", "round": 1}
            if snapshot.values and not snapshot.next:
                return snapshot.values["result"]
            result = await self.graph.ainvoke(initial, config)
            return result["result"]
