from __future__ import annotations

import asyncio
import random
from typing import Annotated, Any, TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.types import Command

from backend.adapters.dsh import RuntimeBlocked
from backend.graphs.tasks import ScopeResolved, TaskSuspended, current_task, recovery_suffix
from packages.contracts.research import BossProposal, BossReview, Proposal, Review, Synthesis, Vote, design_blocker, digest, majority, validate_issue_transition, validate_vote


def merge_records(left, right):
    return {**(left or {}), **(right or {})}


def speaking_order(members, seed):
    order = list(members)
    random.Random(seed).shuffle(order)
    return order


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
    arbitration_id: str
    arbitration_reason: str | None
    arbitration_origin: str
    arbitration_diagnostic: str
    scope_revision: str
    decision_source: str
    resolution: dict
    recovery_chain_id: str
    failed_synthesis: dict
    revised_exploration: dict


class GroupDiscussionGraph:
    def __init__(self, engine, checkpointer):
        self.engine = engine
        self.locks = {}
        graph = StateGraph(DiscussionState)
        for node in ("prepare", "score_members", "choose_speaker", "member_turn", "collect", "synthesize", "arbitrate", "apply_resolution"):
            graph.add_node(node, getattr(self, node))
        graph.add_edge(START, "prepare")
        graph.add_edge("prepare", "choose_speaker")
        graph.add_edge("score_members", "choose_speaker")  # Resume legacy checkpoints.
        graph.add_edge("choose_speaker", "member_turn")
        graph.add_edge("member_turn", "collect")
        graph.add_conditional_edges("collect", lambda s: "arbitrate" if s.get("arbitration_reason") else "synthesize" if s.get("winner") else "again",
                                    {"again": "prepare", "synthesize": "synthesize", "arbitrate": "arbitrate"})
        graph.add_conditional_edges("synthesize", lambda s: "arbitrate" if s.get("arbitration_reason") else "done" if s.get("result") else "again",
                                    {"done": END, "again": "prepare", "arbitrate": "arbitrate"})
        graph.add_edge("arbitrate", "apply_resolution")
        graph.add_conditional_edges("apply_resolution", lambda s: "arbitrate" if s.get("arbitration_reason") else "done" if s.get("result") else "synthesize" if s.get("winner") else "again",
                                    {"done": END, "again": "prepare", "arbitrate": "arbitrate", "synthesize": "synthesize"})
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
        # Kept only as a resume point for checkpoints created by older versions.
        return {}

    async def choose_speaker(self, state):
        await self.engine.guard(state["research_id"])
        key = self.selection_key(state)
        store = self.engine.store
        with store.connect(True) as c:
            plan = store._get(c, "discussion_order", state["discussion_id"])
            if plan is None:
                plan = store._put(c, "discussion_order", state["discussion_id"],
                    {"members": speaking_order(state["members"], digest(state["discussion_id"]))}, state["research_id"])
            selection = store._get(c, "discussion_selection", key)
            if selection is None:
                eligible = self.eligible(state)
                speaker = next(member for member in plan["members"] if member in eligible)
                selection = store._put(c, "discussion_selection", key,
                    {"speaker": speaker, "order": plan["members"], "phase": state["phase"],
                     "round": state["round"], "discussion_id": state["discussion_id"]}, state["research_id"])
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
        return [i for i in self.issues(state) if design_blocker(i)]

    def retain_execution_requirements(self, state, result):
        # A summary may omit an unresolved runtime requirement in prose, but it
        # cannot erase the member-owned acceptance contract passed to the plan.
        required = {i["issue_id"]: {k: v for k, v in i.items() if k != "raised_by"}
            for i in self.issues(state) if i.get("acceptance_stage") == "execution"}
        result["issues"] = [i for i in result.get("issues", []) if i["issue_id"] not in required] + list(required.values())
        return result

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
        try:
            output = await self.engine.turn(state["research_id"], member, state["role"], state["discussion_id"],
                f"{phase}:{state['round']}", schema, context, conversation_id=state["discussion_id"], line_id=state.get("line_id"),
                output_validator=validate_member_output)
        except ScopeResolved as resolution:
            # An invalid member Vote is missing, never replaced by a God ballot.
            return {"arbitration_reason": "invalid_output", "arbitration_origin": "discussion.collect",
                "arbitration_diagnostic": str(resolution), "recovery_chain_id": getattr(resolution, "recovery_chain_id", None)}
        candidates = dict(candidates)
        if phase == "proposal":
            candidates[member] = output["candidate"]
        elif phase == "review" and output.get("revised_candidate") is not None:
            candidates[member] = output["revised_candidate"]
        reviewed = dict(state.get("reviewed_candidates", {}))
        if phase == "review":
            reviewed[member] = digest(candidates)
        return {"records": {f"{phase}:{state['round']}:{member}": output}, "candidates": candidates,
                "reviewed_candidates": reviewed}


    async def collect(self, state):
        if state.get("arbitration_reason"):
            return {}
        if self.eligible(state):
            return {}
        phase, round_no, members = state["phase"], state["round"], state["members"]
        current = {m: state["records"][f"{phase}:{round_no}:{m}"] for m in members}
        if phase == "proposal":
            return {"phase": "review", "round": 1}
        if phase == "review":
            issue_progress = [{k: i.get(k, "design" if k == "acceptance_stage" else None) for k in ("raised_by", "issue_id", "category", "resolved", "experiment_mapping", "acceptance_stage")}
                              for i in sorted(self.issues(state), key=lambda i: (i["raised_by"], i["issue_id"]))]
            fingerprint = digest({"candidates": state["candidates"], "issues": issue_progress,
                "reviews": {m: {k: v[k] for k in ("ready_for_vote", "new_evidence")} for m, v in current.items()}})
            consistent = all(state.get("reviewed_candidates", {}).get(m) == digest(state["candidates"]) for m in members)
            if all(v["ready_for_vote"] for v in current.values()) and not self.blockers(state) and consistent:
                return {"phase": "vote", "prior_review_hash": fingerprint}
            if fingerprint == state.get("prior_review_hash"):
                problems = "；".join(i["issue_id"] + "：" + i["description"][:180] for i in self.blockers(state))
                return {"arbitration_reason": "design_deadlock", "arbitration_origin": "discussion.collect",
                    "arbitration_diagnostic": "修订讨论后仍无进展。" + problems}
            return {"phase": "review", "round": round_no + 1, "prior_review_hash": fingerprint}
        winner = majority({m: v["candidate_id"] for m, v in current.items()}, members, set(state["candidates"]))
        if winner:
            return {"winner": winner}
        if state.get("focused"):
            return {"arbitration_reason": "decision_blocked", "arbitration_origin": "discussion.collect",
                "arbitration_diagnostic": "聚焦讨论与再次投票后仍无严格多数"}
        return {"phase": "review", "round": round_no + 1, "focused": True}

    async def synthesize(self, state):
        synthesis_state = {**state, "phase": "synthesis", **await self.prepare(state)}
        host = (await self.choose_speaker(synthesis_state))["member"]
        candidate = state["candidates"][state["winner"]]
        result = await self.engine.turn(state["research_id"], host, state["role"], state["discussion_id"],
            f"synthesis:{state['round']}", Synthesis, {**state["context"],
            "instruction": "根据程序选定的候选输出决议，decision_source=majority 表示严格多数，god 表示组外仲裁。保留真实票型、少数意见与阻断项。Boss 的 decision 必须完整原样等于 majority_candidate。",
            "decision_source": state.get("decision_source", "majority"),
            "winner": state["winner"], "majority_candidate": candidate, "issues": self.issues(state),
            "conversation_snapshot": synthesis_state["history_snapshot"]},
            conversation_id=state["discussion_id"], line_id=state.get("line_id"), message_kind="decision")
        if result["selected_candidate_id"] != state["winner"] or (state["role"] == "boss" and result["decision"] != candidate):
            return {"arbitration_reason": "invalid_group_decision", "arbitration_origin": "discussion.synthesize",
                "arbitration_diagnostic": "主持人决议与程序选定候选不符", "failed_synthesis": result}
        records = {f"synthesis:{state['round']}:{host}": result}
        speaking = {}
        if self.blockers({**state, "records": {**state["records"], **records}}):
            return {**speaking, "records": records, "winner": None, "phase": "review", "round": state["round"] + 1}
        self.retain_execution_requirements({**state, "records": {**state["records"], **records}}, result)
        result["selected_payload"] = candidate
        result["decision_source"] = state.get("decision_source", "majority")
        if state.get("revised_exploration"):
            result["revised_exploration"] = state["revised_exploration"]
        self.engine.store.put("discussion", state["discussion_id"], {"members": state["members"], "records": state["records"],
            "candidates": state["candidates"], "winner": state["winner"], "authored_by_agent_id": host, "result": result}, state["research_id"])
        if state.get("recovery_chain_id"):
            self.engine.store.close_recovery_chain(state["recovery_chain_id"])
        return {**speaking, "result": result}

    async def arbitrate(self, state):
        rid, did = state["research_id"], state["discussion_id"]
        legacy_id = (current_task.get() or {}).get("dependency_recovery_id")
        if legacy_id:
            request = self.engine.store.get("arbitration_request", legacy_id)
            revision = digest({"candidates": state.get("candidates", {}), "issues": self.issues(state),
                "round": state["round"], "records": state["records"]})
            if request and request["scope_id"] == did and request["scope_revision"] == revision:
                decision = self.engine.arbitration._decision(request)
                if self.engine.dependencies.can_route(request, decision):
                    return {"arbitration_id": legacy_id, "resolution": decision,
                        "scope_revision": revision, "recovery_chain_id": request["recovery_chain_id"]}
        # Full records and paginated original messages, rather than prepare()'s
        # deliberately short conversational excerpt, form the frozen evidence.
        messages, before = [], None
        while True:
            page = self.engine.store.messages(did, before=before, limit=200)
            messages = page["items"] + messages
            before = page.get("next_cursor")
            if not before:
                break
        issues = self.issues(state)
        context = {"task": state["context"], "topic": state["context"].get("topic", ""),
            "role": state["role"], "members": state["members"], "candidates": state.get("candidates", {}),
            "records": state["records"], "issues": issues, "blockers": self.blockers(state),
            "discussion": messages, "phase": state["phase"], "round": state["round"],
            "winner": state.get("winner"), "diagnostic": state.get("arbitration_diagnostic"),
            "failed_synthesis": state.get("failed_synthesis"), "focused": state.get("focused", False)}
        if (state["role"] == "critic" and state.get("line_id") and state["context"].get("exploration")
                and state["arbitration_reason"] in {"design_deadlock", "decision_blocked"}):
            context["revision_routes"] = {"exploration": "revise_exploration"}
        # Transient phases are not a new scientific scene on retry.
        context["dependency_catalog"] = [{k: line.get(k) for k in ("line_id", "direction_epoch", "cycle_id")}
            for line in self.engine.store.lines(rid)]
        summary_repair = state["arbitration_reason"] == "invalid_group_decision"
        def validate_summary(result):
            if result["selected_candidate_id"] != state["winner"]:
                raise ValueError("Repaired summary must retain the original selected candidate")
            if state["role"] == "boss" and result["decision"] != state["candidates"][state["winner"]]:
                raise ValueError("Boss summary must retain the complete selected payload")
            checked = {**state, "records": {**state["records"], "synthesis:god:god": result}}
            if self.blockers(checked):
                raise ValueError("A repaired summary cannot introduce or hide unresolved blockers")
        actions = ["repair_output", "request_revision", "needs_external_input"] if summary_repair else [
            "select_candidate", "request_revision", "request_evidence", "finish_scope", "needs_external_input"]
        revision = digest({"candidates": state.get("candidates", {}), "issues": issues,
            "round": state["round"], "records": state["records"]})
        decision = await self.engine.arbitration.resolve(research_id=rid, source_task_id=did,
            scope_kind="discussion", scope_id=did, reason_code=state["arbitration_reason"],
            origin_node=state.get("arbitration_origin", "discussion.collect"), context=context,
            target_schema=Synthesis if summary_repair else None, allowed_actions=actions,
            line_id=state.get("line_id"), scope_revision=revision,
            recovery_chain_id=state.get("recovery_chain_id"), output_validator=validate_summary if summary_repair else None)
        request = self.engine.store.get("arbitration_request", decision["arbitration_id"])
        return {"arbitration_id": decision["arbitration_id"], "resolution": decision,
            "scope_revision": revision, "recovery_chain_id": request["recovery_chain_id"]}

    async def apply_resolution(self, state):
        decision, rid = state["resolution"], state["research_id"]
        action = decision["action"]
        if action in {"select_candidate", "repair_output"}:
            winner = decision.get("selected_candidate_id", state.get("winner"))
            candidate = state["candidates"][winner]
            if self.blockers(state):
                raise RuntimeBlocked("arbitration_exhausted", "必改项仍未解决，仲裁不能绕过审查门禁")
            if state["role"] == "boss":
                from packages.contracts.research import BossDecision
                BossDecision.model_validate(candidate)
            result = dict(decision["output"]) if action == "repair_output" else {
                "content_markdown": decision["content_markdown"], "selected_candidate_id": winner,
                "decision": candidate if state["role"] == "boss" else {},
                "minority_views": decision.get("minority_views", []), "issues": self.issues(state)}
            if action == "repair_output" and self.blockers({**state,
                    "records": {**state["records"], "synthesis:god:god": result}}):
                raise RuntimeBlocked("invalid_group_decision", "仲裁修复总结仍有未解决必改项，禁止采用")
            self.retain_execution_requirements(state, result)
            source = state.get("decision_source", "majority") if action == "repair_output" else "god"
            result.update(selected_payload=candidate, decision_source=source, arbitration_id=state["arbitration_id"])
            if state.get("revised_exploration"):
                result["revised_exploration"] = state["revised_exploration"]
            if action == "repair_output":
                result["summary_source"] = "god"
            await self.engine.arbitration.apply(state["arbitration_id"], decision)
            self.engine.store.create_message(rid, state["discussion_id"], {"type": "system", "id": "god"},
                decision["content_markdown"] + "\n\n仲裁依据：" + decision["rationale"] +
                "\n独立仲裁会话：" + rid + ":god:" + state["arbitration_id"],
                source_system="god_arbitration", source_event_id=state["arbitration_id"] + ":summary",
                kind="decision", decision_source=source, structured=result, arbitration_id=state["arbitration_id"])
            self.engine.store.put("discussion", state["discussion_id"], {"members": state["members"],
                "records": state["records"], "candidates": state["candidates"], "winner": winner,
                "decision_source": source, "arbitration_id": state["arbitration_id"], "result": result}, rid)
            self.engine.store.close_recovery_chain(state["recovery_chain_id"])
            return {"result": result, "winner": winner, "decision_source": source, "arbitration_reason": None}
        if action == "request_revision":
            if decision["target_role"] != state["role"]:
                if not (state["role"] == "critic" and decision["target_role"] == "exploration"
                        and state.get("line_id") and state["context"].get("exploration")
                        and state["arbitration_reason"] in {"design_deadlock", "decision_blocked"}):
                    raise ValueError("No registered group revision route")
                await self.engine.arbitration.apply(state["arbitration_id"], decision, effect_kind="request_revision")
                project = await self.engine.guard(rid)
                # The original critic records stay in this checkpoint. Exploration
                # authors its own revision; only the critic can resolve its issues.
                revised = await self.run(rid, state["discussion_id"] + ":revision:" + state["arbitration_id"],
                    "exploration", self.engine.members(project, "exploration", state["line_id"]),
                    {**state["context"], "god_revision": decision, "critic_issues": self.issues(state),
                        "instruction": "根据 Critic 原始必改项和 God 修订要求完整修订本组方案；保留依据，不替 Critic 宣称问题已解决。"},
                    state["line_id"])
                return {"context": {**state["context"], "exploration": revised, "god_revision": decision},
                    "revised_exploration": revised, "winner": None, "phase": "review",
                    "round": state["round"] + 1, "prior_review_hash": "", "reviewed_candidates": {},
                    "arbitration_reason": None, "resolution": {}}
            await self.engine.arbitration.apply(state["arbitration_id"], decision, effect_kind="request_revision")
            if state["arbitration_reason"] == "invalid_group_decision":
                return {"context": {**state["context"], "god_revision": decision},
                    "round": state["round"] + 1, "arbitration_reason": None, "resolution": {}}
            return {"context": {**state["context"], "god_revision": decision}, "winner": None,
                "phase": "review", "round": state["round"] + 1, "prior_review_hash": "",
                "arbitration_reason": None, "resolution": {}}
        request = self.engine.store.get("arbitration_request", state["arbitration_id"])
        try:
            recovered = await self.engine.recover_action(request, decision, context=state["context"], schema=Synthesis,
                agent_id=state["members"][0], role=state["role"], task_id=state["discussion_id"],
                turn_id="arbitration-recovery", line_id=state.get("line_id"))
        except RuntimeBlocked as error:
            if error.code != "recovery_validation_failed":
                raise
            return {"arbitration_reason": "design_deadlock", "arbitration_origin": "discussion.collect",
                "arbitration_diagnostic": "仲裁取证未通过验收：" + str(error), "resolution": {},
                "context": {**state["context"], "recovery_failure": str(error)}}
        return {"context": {**state["context"], "recovery_evidence": recovered}, "winner": None,
            "phase": "review", "round": state["round"] + 1, "arbitration_reason": None, "resolution": {}}

    async def run(self, research_id, discussion_id, role, members, context, line_id=None):
        async with self.locks.setdefault((research_id, role, line_id), asyncio.Lock()):
            base_id = discussion_id + recovery_suffix()
            legacy_id = base_id + ":serial-v2"
            config = {"configurable": {"thread_id": f"research/{research_id}/discussion/{legacy_id}"}, "recursion_limit": 100000}
            snapshot = await self.graph.aget_state(config)
            # A saved legacy checkpoint keeps its original semantics. New instances
            # have an explicit separate identity and cannot mix old/new ballots.
            if (self.engine.store.get("discussion_state", discussion_id + ":group-v3")
                    or (not snapshot.values and context.get("discussion_protocol") != 1)):
                from backend.graphs.discussion_v2 import MemberDiscussion
                # Recovery generations are technical retries of this same
                # discussion: they must not clear failed votes or freeze state.
                new_id = discussion_id + ":group-v3"
                return await MemberDiscussion(self.engine).run(research_id, new_id, role, members,
                    context, line_id, speaking_order(members, digest(new_id)))
            discussion_id = legacy_id
            has_interrupt = any(t.interrupts for t in snapshot.tasks)
            initial = None if snapshot.values and (snapshot.next or has_interrupt) else {"research_id": research_id, "discussion_id": discussion_id,
                "role": role, "members": members, "context": context, "line_id": line_id, "records": {}, "phase": "proposal", "round": 1}
            if snapshot.values and not snapshot.next and not has_interrupt:
                return snapshot.values["result"]
            root = current_task.get()
            if root and root.get("recovery_outcome") and has_interrupt:
                initial = Command(resume=root["recovery_outcome"])
            result = await self.graph.ainvoke(initial, config)
            if result.get("__interrupt__"):
                payload = result["__interrupt__"][0].value
                if not root:
                    raise RuntimeBlocked("recovery_task_missing", "讨论等待子任务，但缺少 Worker 父任务身份")
                raise TaskSuspended(root["id"], config["configurable"]["thread_id"], payload["resume_token"])
            return result["result"]
