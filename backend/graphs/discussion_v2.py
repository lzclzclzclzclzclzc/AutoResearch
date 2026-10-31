"""Member initiated polling and unanimous, durable discussion finalization.

The legacy graph remains available for existing checkpoints. New instances use
this explicit protocol and persist each accepted speech and ballot atomically.
"""
from __future__ import annotations

import asyncio
from copy import deepcopy

from backend.adapters.dsh import ResearchStopped, RuntimeBlocked
from packages.contracts.research import (
    DiscussionFinal, DiscussionTurn, EndBallot, OrdinaryBallot, allowed_next_steps, digest,
)


class MemberDiscussion:
    def __init__(self, engine):
        self.engine, self.store = engine, engine.store

    def _history(self, discussion_id):
        messages, before = [], None
        while True:
            page = self.store.messages(discussion_id, before=before, limit=200)
            messages = page["items"] + messages
            before = page.get("next_cursor")
            if not before:
                return messages

    def _context(self, state, **fields):
        return {**state["context"], "discussion_protocol": 2, "no_arbitration": True,
            "role": state["role"], "members": state["members"],
            "allowed_next_steps": list(allowed_next_steps(state["role"])),
            "failed_end_votes": state["failed_end_votes"],
            "freeze_remaining": state["freeze_remaining"],
            "conversation_snapshot": self._history(state["discussion_id"]), **fields}

    def _message(self, state, suffix, content, *, sender=None, vote=None, **fields):
        message = {"source_event_id": state["discussion_id"] + ":" + suffix,
            "content_markdown": content, "kind": "discussion_vote" if vote else "discussion",
            "discussion_round": state["turn_index"] // len(state["members"]) + 1,
            "sender": sender or {"type": "system", "id": "discussion"}, **fields}
        if vote is not None:
            message["vote"] = deepcopy(vote)
        return message

    def _vote_view(self, state, vote=None, *, phase, **fields):
        vote = vote or state["active_vote"]
        return {**deepcopy(vote), "phase": phase, "members": state["members"],
            "ballots": list(vote.get("ballots", {}).values()),
            "failed_end_votes": state["failed_end_votes"],
            "freeze_remaining": list(state["freeze_remaining"]), **fields}

    def _validate_output(self, state, output, next_step=None):
        if not isinstance(output, dict) or not output:
            raise ValueError("结束投票需要一份可读取的完整产物")
        validator = getattr(self.engine, "validate_discussion_output", None)
        if validator:
            validator(state["role"], output, state["context"], next_step=next_step)

    async def _call(self, state, member, schema, turn_id, context, validator=None):
        # Suppress raw JSON control messages. The accepted ballot and its typed UI
        # message are committed together below; a technical failure never casts a vote.
        key = digest([state["discussion_id"], member, turn_id, schema.__name__])
        saved = self.store.get("discussion_model_call", key) or {}
        if saved.get("output") is not None:
            return saved["output"]
        attempt = saved.get("technical_attempt", 0)
        logical_turn = turn_id + (":technical-" + str(attempt) if attempt else "")
        if schema is not DiscussionFinal and hasattr(self.engine, "control"):
            line = self.store.get("line", state["research_id"] + ":" + state["line_id"]) if state.get("line_id") else None
            context["received_messages"] = await asyncio.to_thread(self.store.consume_agent_inbox,
                state["research_id"], member, "discussion-inbox-" + key,
                self.engine.control, (line or {}).get("direction_epoch"))
        try:
            output = await self.engine.turn(state["research_id"], member,
                "god" if schema is DiscussionFinal else state["role"],
                state["discussion_id"], logical_turn, schema, context,
                conversation_id=state["discussion_id"], line_id=state.get("line_id"),
                publish_message=False, output_validator=validator)
        except RuntimeBlocked as error:
            if error.code in {"invalid_output", "model_output_truncated", "model_response_invalid"}:
                # A resumed technical call must be able to correct the current
                # member's malformed response without recasting accepted ballots.
                self.store.put("discussion_model_call", key,
                    {"technical_attempt": attempt + 1, "last_error": str(error)}, state["research_id"])
                raise RuntimeBlocked("discussion_technical_failure", str(error)) from error
            raise
        self.store.put("discussion_model_call", key, {"technical_attempt": attempt,
            "output": output}, state["research_id"])
        return output

    async def run(self, research_id, discussion_id, role, members, context, line_id, order):
        if not members or len(set(members)) != len(members):
            raise ValueError("讨论必须登记非空且不重复的成员")
        self.store.create_conversation(research_id, discussion_id, kind="group_discussion",
            title=role + " 组内讨论", protocol_version=2,
            participants=[{"type": "agent", "id": m} for m in members], line_id=line_id)
        state = self.store.initialize_discussion(discussion_id, {
            "protocol_version": 2, "research_id": research_id, "discussion_id": discussion_id,
            "role": role, "members": list(members), "order": order, "context": context,
            "line_id": line_id, "mode": "discussion", "cursor": 0, "turn_index": 0,
            "poll_index": 0, "failed_end_votes": 0, "freeze_remaining": [],
            "active_vote": None, "votes": [], "result": None})
        while state["mode"] != "ended":
            await self.engine.guard(research_id)
            if state["mode"] == "discussion":
                state = await self._speech(state)
            elif state["mode"] in {"ordinary_vote", "end_vote"}:
                state = await self._ballot(state)
            elif state["mode"] == "god":
                state = await self._god(state)
            else:
                raise RuntimeBlocked("invalid_discussion_state", "未知的讨论协议状态")
        return state["result"]

    async def _speech(self, state):
        member = state["order"][state["cursor"]]
        context = self._context(state, instruction=(
            "轮到你正常发言。自由讨论观点、证据和方案；可以发起 ordinary_vote(question,options)，"
            "或在有完整产物后发起 end_vote(final_output)。没有主持人，无需全员先声明准备。"
            "结束票被冻结时仍正常发言；每名成员完成一次新发言才解除冻结。"))
        output = await self._call(state, member, DiscussionTurn,
            "discussion:" + str(state["turn_index"]), context)
        try:
            await self.engine.guard(state["research_id"])
        except ResearchStopped:
            # Preserve the actual late utterance, without opening its requested
            # vote or creating a successor after the user's stop.
            if output.get("content_markdown", "").strip():
                self.store.commit_discussion_state(state, state, [self._message(state,
                    "late-speech:" + str(state["turn_index"]), output["content_markdown"],
                    sender={"type": "agent", "id": member}, received_after_stop=True)])
            raise
        new = deepcopy(state)
        new["turn_index"] += 1
        new["cursor"] = (new["cursor"] + 1) % len(new["order"])
        text = output.get("content_markdown", "")
        messages = []
        if text.strip():
            messages.append(self._message(state, "speech:" + str(state["turn_index"]), text,
                sender={"type": "agent", "id": member}))
            if member in new["freeze_remaining"]:
                new["freeze_remaining"].remove(member)
                previous = new["votes"][-1]
                view = self._vote_view(new, previous, phase="freeze", status="frozen" if new["freeze_remaining"] else "resumed")
                remaining = "、".join(new["freeze_remaining"])
                messages.append(self._message(state, "freeze:" + str(state["turn_index"]),
                    "结束投票冻结：还需发言 " + remaining if remaining else "全员已完成新发言，可以再次发起结束投票。", vote=view))
        poll = output.get("ordinary_vote") or output.get("poll")
        end = output.get("end_vote")
        if end and new["freeze_remaining"]:
            remaining = "、".join(new["freeze_remaining"])
            messages.append(self._message(state, "frozen-request:" + str(state["turn_index"]),
                "已保留正常发言；结束投票仍冻结，还需发言：" + remaining,
                vote=self._vote_view(new, new["votes"][-1], phase="freeze", status="frozen")))
        elif end:
            final_output = end["final_output"]
            try:
                self._validate_output(state, final_output)
            except (ValueError, TypeError) as error:
                # This was a valid speech, but its routing parameters need author
                # correction. It is neither a failed ballot nor an execution failure.
                messages.append(self._message(state, "invalid-product:" + str(state["turn_index"]),
                    "已保留发言，结束投票尚未发起；请在下次发言补齐产物的交接参数：" + str(error)))
            else:
                vote = {"id": state["discussion_id"] + ":vote:" + str(state["poll_index"] + 1),
                    "type": "end", "question": "是否同意以这份产物结束当前讨论并推进？",
                    "options": [], "initiator": member, "final_output": deepcopy(final_output),
                    "output_hash": digest(final_output), "ballots": {}, "status": "voting",
                    "allowed_next_steps": list(allowed_next_steps(state["role"]))}
                new.update(mode="end_vote", active_vote=vote, poll_index=state["poll_index"] + 1)
                messages.append(self._message(state, "vote-start:" + vote["id"],
                    "发起结束投票：" + vote["question"], sender={"type": "agent", "id": member},
                    vote=self._vote_view(new, phase="started")))
        elif poll:
            vote = {"id": state["discussion_id"] + ":vote:" + str(state["poll_index"] + 1),
                "type": "ordinary", "question": poll["question"],
                "options": [{"id": "option-" + str(i + 1), "label": option} for i, option in enumerate(poll["options"])],
                "initiator": member, "ballots": {}, "status": "voting"}
            new.update(mode="ordinary_vote", active_vote=vote, poll_index=state["poll_index"] + 1)
            messages.append(self._message(state, "vote-start:" + vote["id"],
                "发起普通投票：" + vote["question"] + "\n" + "\n".join(o["label"] for o in vote["options"]),
                sender={"type": "agent", "id": member}, vote=self._vote_view(new, phase="started")))
        return self.store.commit_discussion_state(state, new, messages)

    async def _ballot(self, state):
        vote = state["active_vote"]
        missing = [m for m in state["order"] if m not in vote["ballots"]]
        if not missing:
            return self._resolve_vote(state)
        member = missing[0]
        directions = list(allowed_next_steps(state["role"]))
        is_end = vote["type"] == "end"
        def validate(output):
            if is_end:
                if len(directions) > 1 and output.get("next_step") not in directions:
                    raise ValueError("每张结束票都须选择本次允许的推进方向，包括否票")
                if len(directions) == 1 and output.get("next_step") not in (None, directions[0]):
                    raise ValueError("推进方向不属于当前讨论")
                if output["approve"]:
                    self._validate_output(state, vote["final_output"],
                        directions[0] if len(directions) == 1 else output["next_step"])
            elif output.get("option") not in {o[k] for o in vote["options"] for k in ("id", "label")}:
                raise ValueError("请选择当前普通投票中真实存在的选项")
        context = self._context(state, vote=deepcopy(vote), instruction=(
            "仅代表自己投结束票，只输出 approve 是/否；多方向小组另输出 next_step（否票也须填写）。无需理由，不修改绑定产物。"
            if is_end else "仅代表自己投普通票，输出 option（选项文字或标识）和 reason。此投票只提供讨论材料，不结束讨论。"))
        output = await self._call(state, member, EndBallot if is_end else OrdinaryBallot,
            "ballot:" + vote["id"] + ":" + member, context, validate)
        # Save a real received ballot even if stop arrived during its call. The
        # loop checks user control before resolving or starting any next action.
        new = deepcopy(state)
        if is_end:
            ballot = {"member_id": member, "approve": output["approve"],
                "next_step": directions[0] if len(directions) == 1 else output["next_step"]}
            content = "结束投票：" + ("同意" if ballot["approve"] else "不同意") + " · 推进方向：" + ballot["next_step"]
        else:
            option = next(o for o in vote["options"] if output["option"] in (o["id"], o["label"]))
            ballot = {"member_id": member, "option": option["id"], "reason": output["reason"]}
            content = "普通投票：" + option["label"] + "\n\n理由：" + ballot["reason"]
        new["active_vote"]["ballots"][member] = ballot
        message = self._message(state, "ballot:" + vote["id"] + ":" + member, content,
            sender={"type": "agent", "id": member}, vote=self._vote_view(new, phase="ballot", ballot=ballot))
        return self.store.commit_discussion_state(state, new, [message])

    def _resolve_vote(self, state):
        new = deepcopy(state)
        vote = new["active_vote"]
        ballots = list(vote["ballots"].values())
        passed = vote["type"] == "end" and all(b["approve"] for b in ballots) and len({b["next_step"] for b in ballots}) == 1
        vote["status"] = "passed" if passed else "failed" if vote["type"] == "end" else "completed"
        new["votes"].append(deepcopy(vote))
        new["active_vote"] = None
        if vote["type"] == "ordinary":
            tally = {o["id"]: sum(b["option"] == o["id"] for b in ballots) for o in vote["options"]}
            vote["tally"] = tally
            new["votes"][-1]["tally"] = tally
            new["mode"] = "discussion"
            content = "普通投票结束，继续讨论。\n" + "\n".join(o["label"] + "：" + str(tally[o["id"]]) + " 票" for o in vote["options"])
        elif passed:
            next_step = ballots[0]["next_step"]
            self._validate_output(state, vote["final_output"], next_step)
            new["mode"] = "ended"
            new["result"] = {"protocol_version": 2, "discussion_id": state["discussion_id"],
                "next_step": next_step, "final_output": deepcopy(vote["final_output"]),
                "content_markdown": vote["final_output"].get("content_markdown", ""),
                "decision_source": "unanimous"}
            content = "结束投票全票通过，推进方向：" + next_step
        else:
            new["failed_end_votes"] += 1
            new["freeze_remaining"] = list(state["order"]) if new["failed_end_votes"] < 3 else []
            new["mode"] = "god" if new["failed_end_votes"] == 3 else "discussion"
            content = "结束投票未通过，累计失败 " + str(new["failed_end_votes"]) + " 次。"
            content += "第三次失败，立即由 God 定稿。" if new["mode"] == "god" else "结束投票冻结，待每位成员再完成一次普通发言。"
        view = self._vote_view(new, vote, phase="result", next_step=(new.get("result") or {}).get("next_step"))
        message = self._message(state, "result:" + vote["id"], content, vote=view)
        return self.store.commit_discussion_state(state, new, [message])

    async def _god(self, state):
        final_id = "gfinal-" + digest(state["discussion_id"])[:32]
        cid = state["research_id"] + ":god:" + final_id
        self.store.create_conversation(state["research_id"], cid, kind="discussion_finalization",
            title="God · 讨论最终定稿", protocol_version=2,
            participants=[{"type": "agent", "id": "god/" + final_id}],
            discussion_id=state["discussion_id"], line_id=state.get("line_id"))
        self.store.put("discussion_finalization", final_id, {"discussion_id": state["discussion_id"],
            "conversation_id": cid, "status": "running", "votes": state["votes"],
            "allowed_next_steps": list(allowed_next_steps(state["role"]))}, state["research_id"])
        state = self.store.commit_discussion_state(state, state, [self._message(state,
            "god:started", "第三次结束投票失败，God 正在阅读完整讨论并定稿。",
            sender={"type": "agent", "id": "god/" + final_id},
            vote=self._vote_view(state, state["votes"][-1], phase="god", status="running",
                finalization_id=final_id))])
        def validate(output):
            if output["next_step"] not in allowed_next_steps(state["role"]):
                raise ValueError("God 必须选择本次允许的推进方向")
            self._validate_output(state, output["final_output"], output["next_step"])
        context = self._context(state, votes=state["votes"], instruction=(
            "本次讨论累计三次结束投票失败。你是最终定稿者，阅读全部讨论、普通投票与三次结束票绑定产物。"
            "直接输出 next_step 和完整 final_output。可以采用、修改、合并或提出新方案；不选候选编号，"
            "不请求原组复议，不代投，不再总结。只能使用已有真实证据，无法创造缺失的实验结果。"))
        snapshot = self.store.get("discussion_finalization_snapshot", final_id)
        if snapshot is None:
            from backend.graphs.arbitration import clean_context
            evidence = [artifact for artifact in self.store.list("artifact", state["research_id"])
                if state["role"] == "boss" or artifact.get("line_id") in {None, state.get("line_id")}]
            sections = {"context": clean_context(context), "discussion": self._history(state["discussion_id"]),
                "votes": deepcopy(state["votes"]), "evidence_catalog": evidence,
                "allowed_next_steps": list(allowed_next_steps(state["role"]))}
            # File bytes stay in the immutable artifact store and are read in
            # pages, checking the hash captured by this frozen snapshot.
            for artifact in evidence:
                sections["evidence:" + artifact["id"]] = {"artifact_id": artifact["id"],
                    "sha256": artifact["sha256"], "size_bytes": artifact["size_bytes"]}
            snapshot = self.store.put("discussion_finalization_snapshot", final_id,
                {"sections": sections, "snapshot_hash": digest(sections), "finalization_id": final_id}, state["research_id"])
        context = {**snapshot["sections"]["context"], "finalization_id": final_id,
            "snapshot_hash": snapshot["snapshot_hash"],
            "snapshot_sections": list(snapshot["sections"]),
            "evidence_catalog": snapshot["sections"]["evidence_catalog"],
            "instruction": context["instruction"] + " 可以用 read_arbitration_context(section,offset) 分页读取 snapshot_sections 中任一完整章节；evidence: 开头的章节读取完整原始证据。"}
        try:
            output = await self._call(state, "god/" + final_id, DiscussionFinal, "god:final", context, validate)
        except RuntimeBlocked as error:
            self.store.patch("discussion_finalization", final_id, status="technical_failure", diagnostic=str(error))
            self.store.commit_discussion_state(state, state, [self._message(state,
                "god:technical-failure:" + str(state["revision"]), "God 定稿遇到技术故障，已保留现场：" + str(error),
                sender={"type": "agent", "id": "god/" + final_id},
                vote=self._vote_view(state, state["votes"][-1], phase="god", status="technical_failure",
                    finalization_id=final_id, diagnostic=str(error)))])
            raise
        try:
            await self.engine.guard(state["research_id"])
        except ResearchStopped:
            content = "停止后收到 God 定稿，保留历史，未交接。\n\n" + output["final_output"].get("content_markdown", "")
            self.store.commit_discussion_state(state, state, [self._message(state,
                "god:late", content, sender={"type": "agent", "id": "god/" + final_id},
                conversation_id=cid, kind="decision", structured=output,
                decision_source="god", received_after_stop=True)])
            self.store.patch("discussion_finalization", final_id, status="completed_after_stop", output=output)
            raise
        new = deepcopy(state)
        new["mode"] = "ended"
        new["result"] = {**output, "protocol_version": 2, "discussion_id": state["discussion_id"],
            "content_markdown": output["final_output"].get("content_markdown", ""),
            "decision_source": "god", "finalization_id": final_id}
        view = self._vote_view(new, state["votes"][-1], phase="god", status="finalized",
            final_output=output["final_output"], next_step=output["next_step"], finalization_id=final_id)
        content = "God 已定稿 · 推进方向：" + output["next_step"] + "\n\n" + output["final_output"].get("content_markdown", "")
        messages = [self._message(state, "god:final", content, sender={"type": "agent", "id": "god/" + final_id},
            vote=view, decision_source="god", finalization_id=final_id),
            self._message(state, "god:original", content, sender={"type": "agent", "id": "god/" + final_id},
                conversation_id=cid, kind="decision", structured=output, decision_source="god", finalization_id=final_id)]
        committed = self.store.commit_discussion_state(state, new, messages)
        self.store.patch("discussion_finalization", final_id, status="completed", output=output)
        return committed
