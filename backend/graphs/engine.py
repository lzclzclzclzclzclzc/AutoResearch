from __future__ import annotations

import asyncio
import json
import os
import shutil
import sys
from pathlib import Path
from typing import TypedDict

from langgraph.graph import END, START, StateGraph

from backend.adapters.demo import DemoRuntimeAdapter
from backend.adapters.dsh import DshRuntimeAdapter, ResearchStopped, RuntimeBlocked
from backend.graphs.discussion import GroupDiscussionGraph
from backend.graphs.tasks import OrganizerGraph
from packages.contracts.research import (BossDecision, ExperimentSpec, Implementation, MessageReply, MonitorDecision, Proposal, ResourceAdvice,
    Role, RunAssessment, StageReport, Verification, assert_spec_unchanged, digest, validate_issue_transition)
from packages.prompts.roles import VERSION, role_prompt


class LineState(TypedDict, total=False):
    research_id: str
    line_id: str
    cycle_id: str
    task_id: str
    direction_epoch: int
    revision: int
    spec_id: str
    implementation_id: str
    allocation_id: str
    run_id: str
    submission_id: str
    route: str
    code_attempt: int
    implementation_route: str
    run_route: str


class ResearchEngine:
    def __init__(self, store, control, runner, credentials, checkpointer):
        self.store, self.control, self.runner = store, control, runner
        self.live = DshRuntimeAdapter(store, control, credentials, runner=runner)
        self.demo = DemoRuntimeAdapter(store, control)
        self.checkpointer = checkpointer
        self.discussions = GroupDiscussionGraph(self, checkpointer)
        self.boss_locks, self.paper_locks = {}, {}
        self.agent_turn_locks = {}
        self.organizer_lock = asyncio.Lock()
        self.organizer_graph = OrganizerGraph(self.organizer_advice, checkpointer)
        graph = StateGraph(LineState)
        for name in ("design", "implement", "organize", "run_experiment", "assess_run", "package", "verify", "route_result"):
            graph.add_node(name, getattr(self, name))
        graph.add_edge(START, "design")
        for source, target in (("design", "implement"), ("organize", "run_experiment"),
                               ("run_experiment", "assess_run"), ("package", "verify"), ("verify", "route_result")):
            graph.add_edge(source, target)
        graph.add_conditional_edges("implement", lambda state: state.get("implementation_route", "organize"), {"organize": "organize", "package": "package", "design": "design"})
        graph.add_conditional_edges("assess_run", lambda state: state.get("run_route", "package"), {"repair": "implement", "package": "package"})
        graph.add_conditional_edges("route_result", lambda state: "design" if state["route"] == "exploration_and_critic" else END,
                                    {"design": "design", END: END})
        self.line_graph = graph.compile(checkpointer=checkpointer)

    async def guard(self, research_id, allow_paused=False):
        p = await asyncio.to_thread(self.store.project, research_id)
        if not allow_paused and p["status"] in {"pausing", "paused", "blocked"}:
            raise RuntimeBlocked("research_paused", "研究处于暂停或阻塞状态")
        if not await asyncio.to_thread(self.control.allows_progress, research_id, p["control_generation"]):
            raise ResearchStopped("独立控制门禁已关闭或失联")
        return p

    async def line_open(self, state):
        await self.guard(state["research_id"])
        line = next(line for line in self.store.lines(state["research_id"]) if line["line_id"] == state["line_id"])
        if line.get("finish_requested"):
            raise RuntimeBlocked("line_finished", "Boss 已结束该研究方向；拒绝开始新的科研或正式实验")

    def members(self, project, role, line_id=None):
        team = project["config_snapshot"]["team"]
        count = team["boss_members"] if role == "boss" else team["m1"] if role == "exploration" else team["m2"]
        group = "boss" if role == "boss" else f"{line_id}/{role}"
        return [f"{group}/member-{i:02d}" for i in range(1, count + 1)]

    async def turn(self, research_id, agent_id, *args, **kwargs):
        async with self.agent_turn_locks.setdefault((research_id, agent_id), asyncio.Lock()):
            return await self._turn(research_id, agent_id, *args, **kwargs)

    async def _turn(self, research_id, agent_id, role, task_id, turn_id, schema, context,
                   conversation_id=None, line_id=None, message_kind="discussion", recipients=None, edge_id=None,
                   publish_message=True, reply_to=None):
        project = await self.guard(research_id)
        context = dict(context)
        if line_id and "direction_epoch" not in context:
            line = next((line for line in self.store.lines(research_id) if line["line_id"] == line_id), None)
            if line:
                context["direction_epoch"] = line["direction_epoch"]
        if line_id and role in {"exploration", "critic", "implementation"} and not turn_id.startswith("monitor-"):
            line = next(line for line in self.store.lines(research_id) if line["line_id"] == line_id)
            if line.get("finish_requested"):
                raise RuntimeBlocked("line_finished", "Boss 已结束该研究方向，停止发起新角色任务")
        conversation_id = conversation_id or f"{research_id}:{task_id}"
        inbox_consumer = digest([research_id, agent_id, task_id, turn_id, "received-messages"])
        if publish_message:
            context["received_messages"] = await asyncio.to_thread(self.store.consume_agent_inbox, research_id,
                agent_id, inbox_consumer, self.control, context.get("direction_epoch"))
        if publish_message:
            self.store.create_conversation(research_id, conversation_id, kind=message_kind, title=context.get("instruction", role),
                participants=[{"type": "agent", "id": agent_id}], line_id=line_id)
        identity = f"你的唯一成员 ID 是 {agent_id}，角色是 {role}。只代表自己，不冒充其他成员。"
        instructions = role_prompt(Role(role)) if publish_message else "你在进行私下的发言意愿评分。根据当前话题和最新聊天，返回 0 到 100 的整数 score。只有评分 JSON，不发言、不调用工具。"
        prompt = identity + "\n" + instructions + "\n\n任务输入（只包含授权上下文）:\n" + json.dumps(context, ensure_ascii=False, default=str)
        prompt += "\n\n必须返回此 JSON Schema 对应的完整单个对象:\n" + json.dumps(schema.model_json_schema(), ensure_ascii=False)
        adapter = self.demo if project["mode"] == "demo" else self.live
        delay, last_error = 1.0, None
        while True:
            try:
                result = await adapter.execute_turn(research_id=research_id, agent_id=agent_id, role=role,
                    task_id=task_id, turn_id=turn_id, prompt=prompt, schema=schema, direction_epoch=context.get("direction_epoch", 1),
                    model=project["config_snapshot"]["provider"], context=context,
                    runtime_context={"conversation_id": conversation_id, "line_id": line_id,
                        "direction_epoch": context.get("direction_epoch"), "suppress_chat": not publish_message})
                previous = {i["issue_id"]: {k: v for k, v in i.items() if k != "raised_by"} for i in context.get("issues", []) if i.get("raised_by") == agent_id}
                seen = set()
                for issue in result.get("issues", []):
                    if issue["issue_id"] in seen:
                        raise ValueError("Each issue_id may occur only once in a response")
                    seen.add(issue["issue_id"])
                    validate_issue_transition(previous.get(issue["issue_id"]), issue,
                        lambda source: self.store.source_exists(research_id, source))
                break
            except (ConnectionError, TimeoutError) as error:
                await self.guard(research_id)
                self.store.emit(research_id, "agent.retry_wait", {"delay_seconds": delay, "code": type(error).__name__}, agent_id=agent_id)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)
            except RuntimeBlocked as error:
                if error.code not in {"rate_limited", "model_transport_error", "model_timeout"}:
                    raise
                await self.guard(research_id)
                self.store.emit(research_id, "agent.retry_wait", {"delay_seconds": delay, "code": error.code}, agent_id=agent_id)
                # Short slices allow human stop to interrupt retry backoff promptly.
                until = asyncio.get_running_loop().time() + delay
                while asyncio.get_running_loop().time() < until:
                    await self.guard(research_id)
                    await asyncio.sleep(min(0.4, max(0, until - asyncio.get_running_loop().time())))
                delay = min(delay * 2, 30)
            except (ValueError, json.JSONDecodeError) as error:
                signature = str(error)[:500]
                if signature == last_error:
                    raise RuntimeBlocked("invalid_output", "相同格式错误未取得进展，任务已阻塞") from error
                last_error = signature
                prompt += "\n上次输出校验失败，请更正并完整返回 JSON：" + signature
                turn_id = turn_id + ":repair"
        await self.guard(research_id)
        if not publish_message:
            return result
        message_id = adapter.message_reference(research_id, agent_id, task_id, turn_id) if hasattr(adapter, "message_reference") else None
        message_fields = {"source_system": "demo_fixture" if project["mode"] == "demo" else "dsh",
            "source_event_id": digest([research_id, agent_id, task_id, turn_id]), "kind": message_kind,
            "line_id": line_id, "recipients": recipients or [], "edge_id": edge_id,
            "authored_by_agent_id": agent_id, "task_id": task_id, "prompt_version": VERSION,
            "demo": project["mode"] == "demo", "structured": result}
        content = result.get("content_markdown", json.dumps(result, ensure_ascii=False))
        if message_id:
            message = self.store.complete_stream_message(message_id, content, status="complete", **message_fields)
        else:
            message = self.store.create_message(research_id, conversation_id, agent_id, content, **message_fields)
        if message_kind == "communication" and recipients:
            await asyncio.to_thread(self.store.deliver_agent_message,
                {"research_id": research_id, "control_generation": project["control_generation"], "agent_id": agent_id,
                 "execution_id": inbox_consumer, "conversation_id": conversation_id, "task_id": task_id,
                 "line_id": line_id, "direction_epoch": context.get("direction_epoch"),
                 "recipient_direction_epochs": {reply_to["sender"]["id"]: reply_to["direction_epoch"]}
                     if reply_to and reply_to.get("direction_epoch") is not None else {}},
                [r["id"] for r in recipients], content, "reply", self.control,
                existing_message_id=message["message_id"], wake=False)
        return result

    async def communication(self, task):
        """Read and answer mail without executing a scientific decision or resource action."""
        rid, payload = task["research_id"], task["payload"]
        p = await self.guard(rid)
        if task.get("control_generation") != p["control_generation"]:
            raise ResearchStopped("旧代次通信任务已取消")
        topology = self.store.get("topology", rid)
        node = next(n for n in topology["nodes"] if n["id"] == payload["recipient_id"])
        role = node["data"]["role"].removesuffix("_member")
        line_id = node["data"].get("line_id")
        members = [n["id"] for n in topology["nodes"] if n.get("parentId") == node["id"]] if node["type"] == "group" else [node["id"]]
        async with self.discussions.locks.setdefault((rid, role, line_id), asyncio.Lock()):
            await self.guard(rid)
            binding = self.store.get("communication_state", task["id"])
            entries = [self.store.get("inbox", i) for i in payload["inbox_ids"]]
            line = self.store.get("line", f"{rid}:{line_id}") if line_id else None
            if binding and line and binding.get("direction_epoch") != line["direction_epoch"]:
                return {"already_consumed_or_obsolete": True}
            unread = [i for i in entries if i and i["status"] == "delivered" and
                (not line or i.get("direction_epoch") == line["direction_epoch"])]
            if not unread and not binding:
                return {"already_consumed_or_obsolete": True}
            # Reuse the group's serial selection policy and durable speaker choice.
            message = self.store.message(rid, payload["message_id"])
            context = {"topic": p["topic"], "message_to_answer": message,
                "instruction": "只回应收到的消息、说明依据或缺口。此次通信不能分派科研任务、投票、改写共享记忆或启动正式实验。"}
            state = {"research_id": rid, "discussion_id": task["id"] + ":reply", "role": role, "line_id": line_id,
                "members": members, "context": context, "phase": "reply", "round": 1,
                "records": {f"reply:1:{m}": {} for m in members if m not in {i["recipient_id"] for i in unread}}}
            if not binding:
                state.update(await self.discussions.prepare(state))
                state["history_snapshot"].append({"message_id": message["message_id"], "speaker": message["sender"]["id"],
                    "text": message["content_markdown"][-4000:]})
                state.update(await self.discussions.score_members(state))
                member = (await self.discussions.choose_speaker(state))["member"]
                binding = self.store.put("communication_state", task["id"], {"member": member,
                    "direction_epoch": line["direction_epoch"] if line else None}, rid)
            member = binding["member"]
            if line_id and self.store.get("line", f"{rid}:{line_id}")["direction_epoch"] != binding["direction_epoch"]:
                return {"already_consumed_or_obsolete": True}
            if line_id:
                context["direction_epoch"] = binding["direction_epoch"]
            # turn() consumes this exact member's inbox and freezes it for retries.
            return await self.turn(rid, member, role, task["id"], "reply", MessageReply, context,
                conversation_id=state["discussion_id"], line_id=line_id, message_kind="communication",
                recipients=[message["sender"]], reply_to=message)

    async def supervisor(self, task):
        research_id = task["research_id"]
        p = self.store.project(research_id)
        if not p["started_at"]:
            try:
                control_status = await asyncio.to_thread(self.control.status, research_id)
            except Exception as error:
                raise RuntimeBlocked("control_unavailable", "独立控制服务不可用，拒绝开始研究") from error
            if control_status.get("stop_id"):
                await asyncio.to_thread(self.store.sync_stop, research_id, control_status)
                raise ResearchStopped("研究在首次调度前已经终止")
            p = self.store.start_project(research_id)
            await asyncio.to_thread(self.control.register_project, research_id, p["deadline_at"], p["control_generation"])
        await self.guard(research_id)
        if p["mode"] == "live" and not self.live.availability()["available"]:
            raise RuntimeBlocked("dsh_sdk_missing", self.live.availability()["message"])
        return await self.boss({**task, "kind": "boss", "payload": {"initial": True}})

    async def boss(self, task):
        research_id, payload = task["research_id"], task["payload"]
        async with self.boss_locks.setdefault(research_id, asyncio.Lock()):
            p = await self.guard(research_id)
            context = {"instruction": "初始分派所有研究线" if payload.get("initial") else "审议冻结提交包与 Verify 事实，决定后续研究方向", "topic": p["topic"],
                "description": p.get("description", ""), "workspace_path": p.get("workspace_path"),
                "initial": payload.get("initial", False), "lines": self.store.lines(research_id), **payload,
                "memory_index": self.store.list("memory_node", research_id), "deadline_at": p["deadline_at"]}
            if payload.get("submission_id"):
                context["submission"] = self.store.get("submission", payload["submission_id"])
                context["cycle_history"] = self.store.list("verification", research_id, cycle_id=payload.get("cycle_id"))
            context["received_handoffs"] = await asyncio.to_thread(self.store.consume_inbox,
                research_id, "boss", task["id"] + ":inbox", payload.get("cycle_id"), self.control)
            members = self.members(p, "boss")
            discussion = await self.discussions.run(research_id, task["id"] + ":discussion", "boss", members, context)
            decision = BossDecision.model_validate(discussion["selected_payload"]).model_dump()
            if p.get("finish_requested") and decision["action"] in {"assign", "continue", "request_evidence", "change_direction"}:
                raise RuntimeBlocked("line_finished", "Boss 已结束项目，后续只能封存结果与报告")
            if payload.get("initial") and {a["line_id"] for a in decision["assignments"]} != {l["line_id"] for l in context["lines"]}:
                raise RuntimeBlocked("invalid_boss_assignment", "Boss 初始任务未完整覆盖固定研究线")
            confirmed = await asyncio.to_thread(self.store.commit_boss_decision, research_id, task["id"], decision, self.control)
            if confirmed.get("status") != "applied":
                raise RuntimeBlocked("confirmation_pending", "Boss 决议尚未取得持久控制确认")
            if decision["memory_updates"]:
                memory_result = await asyncio.to_thread(self.store.write_memory, research_id, task["id"], "updates", decision["memory_updates"], p["memory_version"], self.control)
                if memory_result.get("status") != "applied":
                    raise RuntimeBlocked("confirmation_pending", "共享记忆更新尚未取得持久控制确认")
            frozen = {"decision": decision, "submission": context.get("submission"), "cycle_history": context.get("cycle_history", [])}
            effects = await asyncio.to_thread(self.store.apply_boss_effects, research_id, task["id"], payload.get("line_id"), frozen, self.control)
            if effects.get("status") != "applied":
                raise RuntimeBlocked("confirmation_pending", "Boss 后续任务尚未取得持久控制确认")
            if decision["action"] in {"finish_line", "finish_project"}:
                ending = set(effects.get("finish_line_ids", []))
                for run in self.store.list("run", research_id):
                    if run.get("line_id") in ending and run.get("state") in {"starting", "running"}:
                        await asyncio.to_thread(self.runner.request_stop, run["id"], "early_stopped")
            return decision

    async def design(self, state):
        await self.line_open(state)
        p = await self.guard(state["research_id"])
        research_id, line_id = state["research_id"], state["line_id"]
        revision = state.get("revision", 1)
        self.store.update_line(research_id, line_id, phase="exploration")
        context = {"topic": p["topic"], "description": p.get("description", ""), "workspace_path": p.get("workspace_path"),
            "objective": self.store.get("cycle", state["cycle_id"])["objective"],
            "deadline_at": p["deadline_at"], "direction_epoch": state["direction_epoch"],
            "instruction": "设计完整实验，必须包含所有固定设置与允许调整边界", "previous_verifications": self.store.list("verification", research_id, cycle_id=state["cycle_id"])}
        context["implementation_revision_requests"] = self.store.list("spec_revision_request", research_id, line_id=line_id)
        prefix = f"{state['task_id']}:revision-{revision}"
        exploration_context = {**context, "received_handoffs": await asyncio.to_thread(self.store.consume_inbox,
            research_id, f"{line_id}/exploration", prefix + ":exploration-inbox", state["cycle_id"], self.control)}
        critic_context = {**context, "received_handoffs": await asyncio.to_thread(self.store.consume_inbox,
            research_id, f"{line_id}/critic", prefix + ":critic-inbox", state["cycle_id"], self.control)}
        exploration = await self.discussions.run(research_id, prefix + ":exploration", "exploration", self.members(p, "exploration", line_id), exploration_context, line_id)
        self.store.update_line(research_id, line_id, phase="critic")
        critic = await self.discussions.run(research_id, prefix + ":critic", "critic", self.members(p, "critic", line_id), {**critic_context, "exploration": exploration}, line_id)
        response = await self.turn(research_id, self.members(p, "exploration", line_id)[0], "exploration", prefix, "respond_to_critic", Proposal,
            {**exploration_context, "instruction": "向 Critic 逐项回应实际审查意见，记录修订及纳入实验的分歧", "exploration": exploration, "critic": critic},
            conversation_id=prefix + ":cross_group", line_id=line_id,
            recipients=[{"type": "group", "id": f"{line_id}/critic"}], edge_id=f"{line_id}/exploration->{line_id}/critic")
        spec_id = prefix + ":spec"
        spec = await self.turn(research_id, self.members(p, "critic", line_id)[0], "critic", prefix, "freeze_spec", ExperimentSpec,
            {**critic_context, "instruction": "根据两组决议和回应冻结可执行 ExperimentSpec；必须修正项均解决，分歧映射到实验", "exploration": exploration, "critic": critic, "exploration_response": response, "spec_id": spec_id, "spec_version": revision},
            conversation_id=prefix + ":cross_group", line_id=line_id, message_kind="handoff",
            recipients=[{"type": "agent", "id": f"{line_id}/implementation"}], edge_id=f"{line_id}/critic->{line_id}/implementation")
        if spec["spec_id"] != spec_id or spec["version"] != revision or spec["deadline_at"] != p["deadline_at"]:
            raise RuntimeBlocked("invalid_spec_identity", "计划标识、版本或共享截止时间不匹配")
        self.store.put("spec", spec_id, {**spec, "spec_hash": digest(spec), "line_id": line_id}, research_id)
        self.store.update_line(research_id, line_id, phase="implementation", active_spec_id=spec_id)
        return {"spec_id": spec_id, "revision": revision, "code_attempt": 1}

    def spec(self, state):
        raw = self.store.get("spec", state["spec_id"])
        return ExperimentSpec.model_validate({k: v for k, v in raw.items() if k in ExperimentSpec.model_fields})

    async def implement(self, state):
        await self.line_open(state)
        spec = self.spec(state)
        implementation_id = state["spec_id"] + f":implementation-{state.get('code_attempt', 1)}"
        result = await self.turn(state["research_id"], f"{state['line_id']}/implementation", "implementation", implementation_id, "code", Implementation,
            {"instruction": "完整实现或修复冻结计划，返回相对路径源文件与 Python 入口，禁止改动固定配置。修复必须根据真实失败证据恢复既定方案", "spec": spec.model_dump(), "spec_hash": digest(spec.model_dump()), "direction_epoch": state["direction_epoch"],
             "prior_runtime_assessments": self.store.list("run_assessment", state["research_id"], spec_id=state["spec_id"]),
             "prior_runs": [run for run in self.store.list("run", state["research_id"]) if run.get("spec_id") == state["spec_id"]]},
            conversation_id=implementation_id, line_id=state["line_id"])
        implementation = Implementation.model_validate(result)
        await self.line_open(state)
        try:
            assert_spec_unchanged(spec, implementation)
        except ValueError as error:
            signature = digest([spec.fixed, implementation.actual_config, implementation.spec_hash == digest(spec.model_dump())])
            previous_requests = self.store.list("spec_revision_request", state["research_id"], line_id=state["line_id"])
            self.store.put("spec_revision_request", implementation_id, {"line_id": state["line_id"], "spec_id": state["spec_id"],
                "signature": signature, "implementation_output": result, "reason": "Implementation 试图改变冻结的研究固定项，需要共同修订"}, state["research_id"])
            self.store.create_message(state["research_id"], state["cycle_id"] + ":spec_revision", {"type": "system", "id": "policy"},
                "实现配置与冻结方案不一致，已禁止执行并将修订请求同时交 Exploration / Critic。原始实现回复保留在对应会话。",
                source_system="spec_policy", source_event_id=implementation_id,
                recipients=[{"type": "group", "id": f"{state['line_id']}/exploration"}, {"type": "group", "id": f"{state['line_id']}/critic"}], kind="policy")
            if state.get("code_attempt", 1) > 1 and state.get("run_id"):
                return {"implementation_route": "package"}
            if any(request.get("signature") == signature for request in previous_requests):
                raise RuntimeBlocked("design_deadlock", "重复出现相同的越界修订且没有进展，需 Boss 处理") from error
            return {"implementation_route": "design", "revision": state["revision"] + 1}
        if state.get("code_attempt", 1) > 1 and state.get("implementation_id"):
            previous = self.store.get("implementation", state["implementation_id"])
            if previous["code_version"] == digest(implementation.files):
                self.store.emit(state["research_id"], "implementation.repair_no_progress", {"spec_id": state["spec_id"], "code_version": previous["code_version"]}, line_id=state["line_id"])
                return {"implementation_route": "package"}
        workspace = self.store.data_dir / "projects" / state["research_id"] / "workspaces" / state["line_id"] / f"epoch-{state['direction_epoch']}" / f"revision-{state['revision']}" / f"code-{state.get('code_attempt', 1)}"
        workspace.mkdir(parents=True, exist_ok=True)
        for relative, content in implementation.files.items():
            target = (workspace / relative).resolve()
            if not target.is_relative_to(workspace.resolve()) or Path(relative).is_absolute():
                raise RuntimeBlocked("unsafe_code_path", "Implementation 文件越出研究线工作区")
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content, encoding="utf-8")
        entrypoint = (workspace / implementation.entrypoint).resolve()
        if not entrypoint.is_relative_to(workspace.resolve()) or not entrypoint.is_file() or entrypoint.suffix != ".py":
            raise RuntimeBlocked("invalid_entrypoint", "首版正式实验入口必须是工作区内的 Python 文件")
        if implementation.dependencies:
            # No implicit installation executes unreviewed package hooks.
            import importlib.util
            absent = [name for name in implementation.dependencies if importlib.util.find_spec(name) is None]
            if absent:
                if state.get("code_attempt", 1) > 1 and state.get("run_id"):
                    self.store.put("implementation_attempt", implementation_id, {**result, "status": "dependency_blocked", "missing": absent}, state["research_id"])
                    return {"implementation_route": "package"}
                raise RuntimeBlocked("experiment_dependency_missing", "实验环境缺少依赖：" + ", ".join(absent))
        code_version = digest(implementation.files)
        self.store.put("implementation", implementation_id, {**result, "spec_id": state["spec_id"], "cwd": str(workspace), "code_version": code_version}, state["research_id"])
        return {"implementation_id": implementation_id, "implementation_route": "organize"}

    async def organize(self, state):
        await self.line_open(state)
        p = await self.guard(state["research_id"])
        self.store.update_line(state["research_id"], state["line_id"], phase="organizer")
        spec, impl = self.spec(state), self.store.get("implementation", state["implementation_id"])
        request = {"resource_request_id": state["implementation_id"] + ":resources", "research_id": state["research_id"], "line_id": state["line_id"],
            "implementation_agent_id": f"{state['line_id']}/implementation", "spec_id": spec.spec_id, "spec_version": spec.version,
            "spec_hash": digest(spec.model_dump()), "cpu_cores": impl["cpu_cores"], "memory_bytes": impl["memory_mb"] * 1024 * 1024,
            "gpu_count": impl["gpu_count"], "gpu_uuids": [], "gpu_memory_bytes": 0, "control_generation": p["control_generation"],
            "deadline_at": p["deadline_at"], "estimated_seconds": impl["estimated_seconds"]}
        async with self.organizer_lock:
            advice = await self.organizer_graph.run({"research_id": state["research_id"], "id": request["resource_request_id"],
                "payload": request})
        request["priority"] = advice["priority"]
        while True:
            await self.line_open(state)
            allocation = await asyncio.to_thread(self.runner.request_allocation, request)
            if allocation["state"] == "granted":
                self.store.emit(state["research_id"], "allocation.granted", allocation, line_id=state["line_id"])
                return {"allocation_id": allocation["allocation_id"]}
            self.store.update_line(state["research_id"], state["line_id"], phase="waiting_resources")
            await asyncio.sleep(1)

    async def organizer_advice(self, task):
        request = task["payload"]
        return await self.turn(task["research_id"], "organizer", "organizer", task["id"], "schedule", ResourceAdvice,
            {"instruction": "评估当前申请并给出有依据的优先级，不能自己声称资源已批准", "request": request},
            conversation_id=f"{task['research_id']}:resources", line_id=request["line_id"],
            recipients=[{"type": "agent", "id": request["implementation_agent_id"]}], edge_id=f"{request['line_id']}/implementation->organizer")

    async def collect_run_logs(self, run_id, offset=0):
        """Drain to a fixed observed EOF; expose a bounded tail and explicit truncation."""
        page = await asyncio.to_thread(self.runner.collect, run_id, offset, 262144)
        start, end = page.get("offset", offset), page.get("log_size", page["next_offset"])
        text = page["text"]
        truncated = len(text) > 65536
        text = text[-65536:]
        position = page["next_offset"]
        while position < end:
            await self.guard(page["run"]["research_id"], allow_paused=True)
            chunk = await asyncio.to_thread(self.runner.collect, run_id, position, min(262144, end-position))
            if chunk["next_offset"] <= position:
                break
            position = chunk["next_offset"]
            truncated = truncated or len(text + chunk["text"]) > 65536
            text = (text + chunk["text"])[-65536:]
        return {**page, "text": text, "offset": start, "next_offset": position,
            "log_size": end, "has_more": position < end, "text_truncated": truncated,
            "window_bytes": max(0, position-start)}

    async def monitor_run(self, state, run):
        run_id, rid = run["experiment_run_id"], state["research_id"]
        cursor = self.store.get("run_monitor_cursor", run_id) or {"round_no": 0, "next_offset": 0}
        round_no = cursor["round_no"] + 1
        window_id = f"{run_id}:monitor-window:{round_no}"
        window = self.store.get("run_monitor_window", window_id)
        if not window:
            logs = await self.collect_run_logs(run_id, cursor["next_offset"])
            window = self.store.put("run_monitor_window", window_id, {"logs": logs, "log_hash": digest(logs),
                "round_no": round_no, "from_offset": cursor["next_offset"], "next_offset": logs["next_offset"]}, rid)
        spec = self.spec(state)
        monitor = await self.turn(rid, f"{state['line_id']}/implementation", "implementation", run_id,
            f"monitor-{round_no}", MonitorDecision, {"instruction": "根据冻结监控规则与真实增量日志判断是否继续，不擅改科学设置。text_truncated=true 时正文仅保留本窗口末尾，不代表完整日志。若请求停止必须引用规则编号和观察证据；未预见但确实不可继续的执行问题可使用 execution_unusable",
            "spec": spec.model_dump(), "run": run, "logs": window["logs"], "evidence_log_hash": window["log_hash"],
            "rule_ids": [str(rule.get("rule_id", i)) for i, rule in enumerate(spec.early_stop_rules)]},
            conversation_id=run_id + ":monitor", line_id=state["line_id"])
        if monitor["request_early_stop"]:
            allowed = {str(rule.get("rule_id", i)) for i, rule in enumerate(spec.early_stop_rules)} | {"execution_unusable"}
            if monitor["rule_id"] not in allowed or monitor["evidence_log_hash"] != window["log_hash"]:
                raise RuntimeBlocked("invalid_early_stop", "提前终止意见未绑定冻结规则与真实运行证据")
            await asyncio.to_thread(self.runner.request_stop, run_id, "early_stopped")
        await self.guard(rid)
        # The cursor advances only with the accepted monitoring decision. A lost response
        # reuses this stored window and logical model turn, even if the log keeps growing.
        with self.store.connect(True) as c:
            self.store._put(c, "run_monitor", f"{run_id}:{round_no}", {**monitor, "window_id": window_id}, rid)
            self.store._put(c, "run_monitor_cursor", run_id, {"round_no": round_no, "next_offset": window["next_offset"]}, rid)
        return monitor

    async def run_experiment(self, state):
        await self.line_open(state)
        self.store.update_line(state["research_id"], state["line_id"], phase="running")
        allocation = self.store.get("allocation", state["allocation_id"])
        impl = self.store.get("implementation", state["implementation_id"])
        run_id = state["implementation_id"] + ":run-1"
        payload = {**allocation, "experiment_run_id": run_id, "start_operation_id": run_id + ":start", "command": [sys.executable, "-u", impl["entrypoint"], *impl["arguments"]],
                   "cwd": impl["cwd"], "code_version": impl["code_version"]}
        try:
            run = await asyncio.to_thread(self.runner.start, payload)
        except Exception:
            await self.guard(state["research_id"])
            # A transport/start response may be lost after an admitted failure commits.
            # Only the matching, proven terminal execution can enter the failure pipeline.
            try:
                recovered = await asyncio.to_thread(self.runner.status, run_id)
            except Exception:
                raise RuntimeBlocked("runner_unknown", "启动应答失败且无法确认运行状态，保留资源等待对账")
            proof = recovered.get("terminal_proof", {})
            bound = ("research_id", "allocation_id", "start_operation_id", "experiment_run_id", "control_generation",
                     "grant_revision", "spec_id", "spec_version", "spec_hash", "code_version")
            if (recovered.get("state") not in {"execution_failed", "interrupted"}
                    or any(recovered.get(k) != payload.get(k) for k in bound)
                    or not proof.get("process_tree_exited") or not proof.get("start_operation_terminal")
                    or any(proof.get(k) != recovered.get(k) for k in ("allocation_id", "grant_revision", "experiment_run_id", "runner_generation", "start_operation_id"))):
                raise
            run = recovered
        self.store.put("run", run_id, run, state["research_id"])
        last_monitor = asyncio.get_running_loop().time()
        while run.get("state", run.get("status")) not in {"completed", "succeeded", "failed", "execution_failed", "early_stopped", "cancelled", "terminated", "interrupted", "unknown"}:
            await self.guard(state["research_id"], allow_paused=True)
            await asyncio.sleep(0.5)
            run = await asyncio.to_thread(self.runner.status, run_id)
            self.store.put("run", run_id, run, state["research_id"])
            current_project = self.store.project(state["research_id"])
            line = next(line for line in self.store.lines(state["research_id"]) if line["line_id"] == state["line_id"])
            if line.get("finish_requested") and run.get("state") == "running":
                await asyncio.to_thread(self.runner.request_stop, run_id, "early_stopped")
            if not line.get("finish_requested") and current_project["status"] == "running" and run.get("state") == "running" and asyncio.get_running_loop().time() - last_monitor >= 5:
                await self.monitor_run(state, run)
                last_monitor = asyncio.get_running_loop().time()
        if run.get("state") == "unknown":
            raise RuntimeBlocked("runner_unknown", "正式实验状态未知，保留资源并等待对账，禁止重复启动")
        return {"run_id": run_id}

    async def assess_run(self, state):
        await self.guard(state["research_id"])
        run = self.store.get("run", state["run_id"])
        line = next(line for line in self.store.lines(state["research_id"]) if line["line_id"] == state["line_id"])
        if run.get("state") in {"completed", "succeeded"} or line.get("finish_requested"):
            return {"run_route": "package"}
        logs = await self.collect_run_logs(state["run_id"])
        assessment = await self.turn(state["research_id"], f"{state['line_id']}/implementation", "implementation", state["run_id"], "assess_failure", RunAssessment,
            {"instruction": "保留全部失败与提前停止证据。仅实现错误可在固定科学设置内修复并重新申请资源；科学方案需修订则封包送验，不能偷改计划。无有用修复也封包送验。",
             "spec": self.spec(state).model_dump(), "run": run, "logs": logs},
            conversation_id=state["run_id"] + ":assessment", line_id=state["line_id"])
        self.store.put("run_assessment", state["run_id"], {**assessment, "spec_id": state["spec_id"]}, state["research_id"])
        return {"run_route": "repair" if assessment["action"] == "repair_implementation" else "package", "code_attempt": state.get("code_attempt", 1) + 1}

    async def package(self, state):
        await self.guard(state["research_id"])
        submission_id = state["spec_id"] + ":submission"
        if self.store.get("submission", submission_id):
            return {"submission_id": submission_id}
        impl, run = self.store.get("implementation", state["implementation_id"]), self.store.get("run", state["run_id"])
        artifacts = []
        implementations = self.store.list("implementation", state["research_id"], spec_id=state["spec_id"])
        runs = [run for run in self.store.list("run", state["research_id"]) if run.get("spec_id") == state["spec_id"]]
        for implementation in implementations:
            for path in Path(implementation["cwd"]).rglob("*"):
                if path.is_file():
                    artifacts.append(self.store.register_artifact(state["research_id"], path, "application/json" if path.suffix == ".json" else "text/plain", line_id=state["line_id"], code_version=implementation["code_version"]))
        run_logs = {}
        for attempt_run in runs:
            attempt_id = attempt_run.get("experiment_run_id", attempt_run["id"])
            log = await self.collect_run_logs(attempt_id)
            run_logs[attempt_id] = log
            for reference in log.get("artifacts", []):
                if reference.get("kind") == "stdout_stderr" and reference.get("path"):
                    log_path = Path(reference["path"])
                    if log_path.is_file():
                        artifacts.append(self.store.register_artifact(state["research_id"], log_path, "text/plain",
                            line_id=state["line_id"], run_id=attempt_id, kind="experiment_log"))
        collected = run_logs[state["run_id"]]
        spec = self.spec(state)
        submission = {"cycle_id": state["cycle_id"], "line_id": state["line_id"], "spec_id": spec.spec_id, "spec_version": spec.version,
            "code_version": impl["code_version"], "code_versions": [item["code_version"] for item in implementations], "run_ids": [item.get("experiment_run_id", item["id"]) for item in runs], "runs": runs, "spec": spec.model_dump(), "run": run,
            "actual_config": impl["actual_config"], "artifacts": artifacts, "logs": collected, "run_logs": run_logs, "status": "awaiting_verification",
            "runtime_assessments": self.store.list("run_assessment", state["research_id"], spec_id=state["spec_id"])}
        final_metrics = Path(impl["cwd"]) / "metrics.json"
        if final_metrics.is_file():
            try:
                from backend.runtime.scientific_json import loads as scientific_loads
                submission["metrics"] = scientific_loads(final_metrics.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                pass
        self.store.put("submission", submission_id, submission, state["research_id"])
        return {"submission_id": submission_id}

    async def verify(self, state):
        p = await self.guard(state["research_id"])
        self.store.update_line(state["research_id"], state["line_id"], phase="verify")
        submission = self.store.get("submission", state["submission_id"])
        report = await self.turn(state["research_id"], f"{state['line_id']}/verify", "verify", state["submission_id"], "verify", Verification,
            {"instruction": "核查冻结计划和已有结果，不复跑正式实验。有效负结果可以通过，边界与缺口必须明确", "submission": submission},
            conversation_id=state["submission_id"] + ":verify", line_id=state["line_id"], message_kind="verification",
            edge_id=f"{state['line_id']}/implementation->{state['line_id']}/verify")
        report = {**report, "checks": [report["content_markdown"]], "supported_conclusions": [e["claim"] for e in report["evidence"]],
                  "unsupported_conclusions": report["limitations"], **{k: submission[k] for k in ("spec_id", "spec_version", "code_version", "run_ids")}}
        transition = await asyncio.to_thread(self.store.commit_verification, state["cycle_id"], state["submission_id"], report, p["control_generation"], self.control)
        if transition["route"] in {"halted", "awaiting_confirmation_sync"}:
            raise ResearchStopped("验证候选未获得有效控制确认，不增加轮次")
        self.store.update_line(state["research_id"], state["line_id"], completed_verify_rounds=transition["round_no"])
        return {"route": transition["route"]}

    async def route_result(self, state):
        await self.guard(state["research_id"])
        report = next(v for v in self.store.list("verification", state["research_id"]) if v["submission_version_id"] == state["submission_id"])
        # Verify committed this durable outbox in the same transaction as its
        # count. Only its dispatcher materializes messages, inboxes and Boss work.
        # A transient delivery failure keeps the serial line here, while the
        # independent Worker drain can recover the same immutable handoff.
        while True:
            await self.guard(state["research_id"])
            outbox = self.store.get("outbox", report["operation_id"])
            if outbox and outbox.get("status") == "delivered":
                break
            try:
                await asyncio.to_thread(self.store.dispatch_outbox, self.control, 100, state["research_id"])
            except Exception as error:
                self.store.emit(state["research_id"], "verification.delivery_retry",
                    {"submission_id": state["submission_id"], "code": type(error).__name__}, line_id=state["line_id"])
            outbox = self.store.get("outbox", report["operation_id"])
            if outbox and outbox.get("status") == "delivered":
                break
            if outbox and outbox.get("status") == "cancelled":
                raise ResearchStopped("验证交接已取消，不再推进科研任务")
            await asyncio.sleep(0.4)
        return {} if state["route"] == "boss" else {"revision": state["revision"] + 1}

    async def line(self, task):
        state = {"research_id": task["research_id"], "task_id": task["id"], "revision": 1, **task["payload"]}
        config = {"configurable": {"thread_id": f"research/{state['research_id']}/line/{state['line_id']}/{state['direction_epoch']}/{state['cycle_id']}"}, "recursion_limit": 100000}
        snapshot = await self.line_graph.aget_state(config)
        if snapshot.values and not snapshot.next:
            return snapshot.values
        return await self.line_graph.ainvoke(None if snapshot.values else state, config)

    async def paper(self, task):
        research_id = task["research_id"]
        async with self.paper_locks.setdefault(research_id, asyncio.Lock()):
            frozen = self.store.get("report_snapshot", task["payload"]["snapshot_id"])
            report = await self.turn(research_id, "paper", "paper", task["id"], "write_report", StageReport,
                {"instruction": "撰写冻结资料对应的阶段报告，保留所有验证状态和证据边界", **frozen}, conversation_id=task["id"] + ":paper", message_kind="report")
            folder = self.store.data_dir / "projects" / research_id / "reports"
            folder.mkdir(parents=True, exist_ok=True)
            path = folder / (digest(task["id"])[:20] + ".md")
            path.write_text("# " + report["title"] + "\n\n" + report["content_markdown"], encoding="utf-8")
            artifact = self.store.register_artifact(research_id, path, "text/markdown")
            committed = await asyncio.to_thread(self.store.commit_report, research_id, task["id"],
                {**report, "artifact_id": artifact["id"], "snapshot_id": frozen["id"], "status": "completed"}, self.control)
            if committed.get("status") != "completed":
                raise RuntimeBlocked("confirmation_pending", "报告尚未取得持久控制确认")
            return report

    async def close(self):
        await self.live.close()
        await self.demo.close()
