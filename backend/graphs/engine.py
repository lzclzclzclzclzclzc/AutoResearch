from __future__ import annotations

import asyncio
import json
import inspect
import hashlib
import os
import shutil
import sys
from pathlib import Path
from typing import TypedDict

from langgraph.graph import END, START, StateGraph

from backend.adapters.demo import DemoRuntimeAdapter
from backend.adapters.model_errors import MAX_RESPONSE_ATTEMPTS
from backend.adapters.dsh import DshRuntimeAdapter, ResearchStopped, RuntimeBlocked
from backend.adapters.output_contract import bind_metadata, model_contract
from backend.adapters.token_limits import MAX_OUTPUT_TOKENS, output_tokens
from backend.graphs.discussion import GroupDiscussionGraph
from backend.graphs.tasks import OrganizerGraph, current_task, TaskSuspended, ScopeResolved, recovery_suffix
from backend.runtime.control import ControlUnavailable
from backend.runtime.evidence import validate_execution_checks
from backend.persistence.store import StoreError
from packages.contracts.research import (BossDecision, ExperimentSpec, Implementation, MessageReply, MonitorDecision, Proposal, ResourceAdvice,
    Role, RunAssessment, StageReport, Verification, assert_spec_unchanged, digest, validate_issue_transition)
from packages.prompts.roles import VERSION, role_prompt


def god_prompt_context(sections):
    """A bounded prompt projection; the immutable snapshot remains complete."""
    manifest, references = [], set()
    def collect(value):
        if isinstance(value, dict):
            for key, child in value.items():
                if key in {"message_id", "artifact_id", "source_id", "id", "submission_id", "run_id"} and isinstance(child, str):
                    references.add(child)
                collect(child)
        elif isinstance(value, list):
            for child in value:
                collect(child)
    collect(sections)
    for section, value in sections.items():
        encoded = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, sort_keys=True)
        manifest.append({"section": section, "total_length": len(encoded), "sha256": hashlib.sha256(encoded.encode()).hexdigest(),
            "read": {"tool": "read_arbitration_context", "section": section, "offset": 0}, "page_size": 16384})
    def excerpt(value, key=""):
        if key in {"candidates", "candidate", "majority_candidate", "proposals", "target_schema", "issues", "blockers"}:
            return value
        if isinstance(value, str):
            return value if len(value) <= 12000 else {"excerpt": value[:6000] + "\n…\n" + value[-6000:],
                "complete": False, "total_length": len(value), "notice": "仅摘录；完整原文请读取该顶层 section 的分页快照"}
        if isinstance(value, list):
            recent = value[-24:]
            projected = [excerpt(child) for child in recent]
            return projected if len(recent) == len(value) else {"recent_items": projected,
                "complete": False, "total_items": len(value), "notice": "更早内容保存在完整分页快照"}
        if isinstance(value, dict):
            if key == "records":
                votes = {k: v for k, v in value.items() if k.startswith("vote:")}
                recent = [(k, v) for k, v in value.items() if not k.startswith("vote:")][-12:]
                return {"votes": votes, "recent_records": {k: excerpt(v) for k, v in recent},
                    "complete": len(votes) + len(recent) == len(value), "total_records": len(value)}
            if key == "discussion":
                return value
            return {k: excerpt(v, k) for k, v in value.items()}
        return value
    projected = {}
    for key, value in sections.items():
        if key == "discussion" and isinstance(value, list):
            projected[key] = {"total_messages": len(value), "complete": len(value) <= 24, "recent_messages": [
                {k: excerpt(v, k) for k, v in message.items() if k in {"message_id", "sender", "content_markdown", "kind", "round", "source_event_id"}}
                for message in value[-24:]], "notice": "讨论正文为最近24条摘录；完整历史可通过discussion分页读取"}
        else:
            projected[key] = excerpt(value, key)
    return {"snapshot_excerpt": projected, "snapshot_manifest": manifest, "available_evidence_refs": sorted(references),
        "reference_rule": "evidence_refs 只能使用 available_evidence_refs 中的值；集合为空时返回 []。候选ID、JSON路径和快照哈希不是证据ID。",
        "read_instruction": "需要完整原文时调用 read_arbitration_context(section, offset)，按 next_offset 继续到 total_length；摘录不等于完整上下文。"}


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
    allocation_route: str
    run_id: str
    submission_id: str
    route: str
    code_attempt: int
    implementation_route: str
    run_route: str
    implementation_recovery_id: str


class ResearchEngine:
    def __init__(self, store, control, runner, credentials, checkpointer):
        self.store, self.control, self.runner = store, control, runner
        self.live = DshRuntimeAdapter(store, control, credentials, runner=runner)
        self.demo = DemoRuntimeAdapter(store, control)
        from backend.graphs.arbitration import ArbitrationService
        from backend.graphs.decomposition import DecompositionService
        from backend.graphs.dependencies import DependencyService
        self.arbitration = ArbitrationService(self)
        self.dependencies = DependencyService(self)
        self.decompositions = DecompositionService(self)
        self.checkpointer = checkpointer
        self.discussions = GroupDiscussionGraph(self, checkpointer)
        self.boss_locks, self.paper_locks = {}, {}
        self.boss_memory_locks = {}
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
        graph.add_conditional_edges("implement", lambda state: state.get("implementation_route", "organize"), {"organize": "organize", "package": "package", "design": "design", "implement": "implement"})
        graph.add_conditional_edges("organize", lambda state: state.get("allocation_route", "run_experiment"), {"run_experiment": "run_experiment", "implement": "implement"})
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
        if line["direction_epoch"] != state.get("direction_epoch", line["direction_epoch"]) or line.get("cycle_id") != state.get("cycle_id", line.get("cycle_id")):
            raise RuntimeBlocked("task_superseded", "Boss 已提交新周期，旧任务仅保留历史")
        if line.get("finish_requested"):
            raise RuntimeBlocked("line_finished", "Boss 已结束该研究方向；拒绝开始新的科研或正式实验")

    def members(self, project, role, line_id=None):
        team = project["config_snapshot"]["team"]
        count = team["boss_members"] if role == "boss" else team["m1"] if role == "exploration" else team["m2"]
        group = "boss" if role == "boss" else f"{line_id}/{role}"
        return [f"{group}/member-{i:02d}" for i in range(1, count + 1)]

    async def turn(self, research_id, agent_id, *args, **kwargs):
        chain_id = kwargs.pop("recovery_chain_id", None)
        original_bound = inspect.signature(self._turn).bind(research_id, agent_id, *args, **kwargs)
        original_bound.apply_defaults()
        if recovery_suffix():
            original_bound.arguments["turn_id"] += recovery_suffix()
            original_bound.arguments["context"] = {**original_bound.arguments["context"],
                "recovery_generation": (current_task.get() or {})["recovery_generation"]}
        original_values = dict(original_bound.arguments)
        failure_id = "turn-failure-" + digest([research_id, agent_id, original_values["task_id"], original_values["turn_id"],
            original_values["schema"].model_json_schema(), original_values["context"]])
        saved_failure = self.store.get("turn_failure", failure_id)
        root = current_task.get() or {}
        marker = root.get("recovery_marker") or {}
        if marker.get("resume_point") == "engine.validate_output":
            values = dict(original_values)
            if (marker.get("agent_id") == agent_id and marker.get("logical_task_id") == values["task_id"] and
                    marker.get("turn_id") == values["turn_id"]):
                request = self.store.get("arbitration_request", marker["arbitration_id"])
                decision = marker["decision"]
                if decision["action"] == "repair_output":
                    await self.arbitration.apply(request["id"], decision)
                    values.update(agent_id="god/" + request["id"], result=decision["output"],
                        decision_source="god", arbitration_id=request["id"], repaired_for_agent_id=agent_id)
                    return await self.accept_turn_output(**values)
                values.pop("research_id")
                return await self.recover_action(request, decision, **values)
        if saved_failure:
            values = dict(original_values)
            schema, context = values.pop("schema"), values.pop("context")
            if context.get("recovery_part"):
                raise RuntimeBlocked("recovery_child_output_invalid", saved_failure["diagnostic"]["message"])
            return await self.arbitration.repair_turn(schema=schema, context=context,
                reason_code=saved_failure["reason_code"], diagnostic=saved_failure["diagnostic"],
                recovery_chain_id=chain_id, **values)
        try:
            async with self.agent_turn_locks.setdefault((research_id, agent_id), asyncio.Lock()):
                return await self._turn(**original_values)
        except RuntimeBlocked as error:
            if error.code not in {"invalid_output", "model_output_truncated"}:
                raise
            # Exit both the member lock and DSH capacity slot before asking God.
            values = dict(original_values)
            schema, context = values.pop("schema"), values.pop("context")
            if context.get("recovery_part"):
                # Child failure returns to the parent's one cumulative recovery
                # chain; it cannot recursively judge or fulfill that chain.
                raise RuntimeBlocked("recovery_child_output_invalid", f"{error.code}: {str(error)}") from error
            if schema.__name__ == "Vote" and context.get("god_revision_of"):
                raise ScopeResolved(context["god_revision_of"], chain_id, str(error)) from error
            diagnostic = {"code": error.code, "message": str(error), **getattr(error, "recovery", {})}
            self.store.put("turn_failure", failure_id, {"reason_code": error.code, "diagnostic": diagnostic,
                "agent_id": agent_id, "task_id": values["task_id"], "turn_id": values["turn_id"],
                "target_schema_hash": digest(schema.model_json_schema())}, research_id)
            return await self.arbitration.repair_turn(schema=schema, context=context,
                reason_code=error.code, diagnostic=diagnostic, recovery_chain_id=chain_id, **values)

    async def recover_required_task(self, task, error):
        """Worker fallback accepts a named recovery position, never a callback."""
        from packages.contracts import research
        recovery = dict(error.recovery)
        point = recovery.get("resume_point")
        allowed = {"discussion.collect", "discussion.synthesize", "boss.validate_assignment", "line.implement", "engine.validate_output"}
        if point not in allowed or not isinstance(recovery.get("context"), dict):
            return False
        schema_name = recovery.get("target_schema_name")
        schema = getattr(research, schema_name, None) if schema_name else None
        if schema_name and (schema is None or not hasattr(schema, "model_json_schema")):
            return False
        self.store.patch("task", task["id"], execution_phase="arbitrating")
        decision = await self.arbitration.resolve(research_id=task["research_id"], source_task_id=task["id"],
            scope_kind=recovery.get("scope_kind", "turn_output"), scope_id=recovery.get("scope_id", task["id"]),
            reason_code=error.code, origin_node=point, context={**recovery["context"], "original_error": str(error)},
            target_schema=schema, allowed_actions=recovery.get("allowed_actions"),
            line_id=task.get("payload", {}).get("line_id"), recovery_chain_id=recovery.get("recovery_chain_id"))
        marker = {"resume_point": point, "arbitration_id": decision["arbitration_id"], "decision": decision,
            "scope_id": recovery.get("scope_id", task["id"]), "agent_id": recovery.get("agent_id"),
            "logical_task_id": recovery.get("logical_task_id"), "turn_id": recovery.get("turn_id")}
        self.store.patch("task", task["id"], status="pending", owner=None, recovery_marker=marker)
        return True

    def validate_output(self, research_id, agent_id, schema, result, context, output_validator=None):
        """Accept structured output only after schema and contextual business checks."""
        result = schema.model_validate(bind_metadata(schema, result, context)).model_dump()
        previous = {i["issue_id"]: {k: v for k, v in i.items() if k != "raised_by"}
                    for i in context.get("issues", []) if i.get("raised_by") == agent_id}
        seen = set()
        for issue in result.get("issues", []):
            if issue["issue_id"] in seen:
                raise ValueError("Each issue_id may occur only once in a response")
            seen.add(issue["issue_id"])
            validate_issue_transition(previous.get(issue["issue_id"]), issue,
                lambda source: self.store.source_exists(research_id, source))
        if output_validator is not None:
            output_validator(result)
        if schema is Verification:
            validate_execution_checks(self.store, research_id, context.get("submission", {}), result,
                context.get("line_id") or agent_id.split("/")[0])
        return result

    def validate_recovery_output(self, research_id, agent_id, schema, result, context, output_validator=None):
        """God repairs and assembled output retain the originating business gates."""
        original = context.get("original_context", context)
        result = self.validate_output(research_id, agent_id, schema, result, original, output_validator)
        if schema is Implementation:
            if not original.get("spec"):
                raise ValueError("Implementation recovery needs the original frozen ExperimentSpec")
            spec = ExperimentSpec.model_validate(original["spec"])
            implementation = Implementation.model_validate(bind_metadata(Implementation, result, {"spec": spec.model_dump()}))
            assert_spec_unchanged(spec, implementation)
            names = []
            for relative in implementation.files:
                normalized = os.path.normpath(relative.replace("\\", "/")).replace("\\", "/")
                if (not normalized or normalized.startswith("/") or ":" in normalized or "\0" in normalized or
                        any(part in {"", ".", ".."} for part in normalized.split("/"))):
                    raise ValueError("Repaired implementation file must stay in the research workspace")
                names.append(normalized.casefold())
            if len(set(names)) != len(names):
                raise ValueError("Repaired implementation contains conflicting Windows file paths")
            entry = os.path.normpath(implementation.entrypoint.replace("\\", "/")).replace("\\", "/")
            if entry not in {os.path.normpath(path.replace("\\", "/")).replace("\\", "/") for path in implementation.files}:
                raise ValueError("Repaired implementation entrypoint must be one of its owned source files")
        if schema is BossDecision and original.get("lines") is not None:
            legal = {line["line_id"] for line in original["lines"]}
            assigned = [a["line_id"] for a in result["assignments"]]
            if len(set(assigned)) != len(assigned) or set(assigned) - legal:
                raise ValueError("Repaired Boss assignment has unknown or duplicate research lines")
            if original.get("initial") and (set(assigned) != legal or result["action"] != "assign"):
                raise ValueError("Repaired initial Boss assignment must cover every research line")
        return result

    async def _turn(self, research_id, agent_id, role, task_id, turn_id, schema, context,
                   conversation_id=None, line_id=None, message_kind="discussion", recipients=None, edge_id=None,
                   publish_message=True, reply_to=None, output_validator=None):
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
        wire_contract = model_contract(schema, context)
        prompt += "\n\n必须返回此 JSON Schema 对应的完整单个对象（系统编号、版本和哈希由程序填写）:\n" + json.dumps(wire_contract.model_json_schema(), ensure_ascii=False)
        adapter = self.demo if project["mode"] == "demo" else self.live
        delay, last_error = 1.0, None
        token_limit = output_tokens(not publish_message)
        while True:
            response_retry_id = digest([research_id, agent_id, task_id, turn_id, project["control_generation"]])
            response_retry = self.store.get("model_response_retry", response_retry_id) or {}
            if response_retry.get("failures", 0) >= MAX_RESPONSE_ATTEMPTS:
                raise RuntimeBlocked("model_response_invalid", "模型响应连续三次异常，已保留现场，请检查模型服务的响应协议后重试。",
                    provider_error_code=response_retry.get("provider_error_code"))
            try:
                result = await adapter.execute_turn(research_id=research_id, agent_id=agent_id, role=role,
                    task_id=task_id, turn_id=turn_id, prompt=prompt, schema=wire_contract, direction_epoch=context.get("direction_epoch", 1),
                    model=project["config_snapshot"]["provider"], context=context,
                    runtime_context={"conversation_id": conversation_id, "line_id": line_id,
                        "direction_epoch": context.get("direction_epoch"), "suppress_chat": not publish_message,
                        "max_output_tokens": token_limit})
                result = self.validate_output(research_id, agent_id, schema, result, context, output_validator)
                break
            except (ConnectionError, TimeoutError) as error:
                await self.guard(research_id)
                self.store.emit(research_id, "agent.retry_wait", {"delay_seconds": delay, "code": type(error).__name__}, agent_id=agent_id)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)
            except RuntimeBlocked as error:
                if error.code == "model_response_invalid":
                    await self.guard(research_id)
                    failures = response_retry.get("failures", 0) + 1
                    self.store.put("model_response_retry", response_retry_id, {"failures": failures,
                        "agent_id": agent_id, "task_id": task_id, "turn_id": turn_id,
                        "provider_error_code": error.provider_error_code}, research_id)
                    if failures >= MAX_RESPONSE_ATTEMPTS:
                        raise RuntimeBlocked("model_response_invalid", "模型响应连续三次异常，已保留现场，请检查模型服务的响应协议后重试。",
                            http_status=error.http_status, provider_error_code=error.provider_error_code) from error
                    # Adapter admission requires exit proof before a replacement.
                    # Keep the logical turn so completed tools and session history can be reused.
                    delay = min(2 ** (failures - 1), 4)
                    self.store.emit(research_id, "agent.retry_wait", {"delay_seconds": delay,
                        "code": error.code, "provider_error_code": error.provider_error_code,
                        "attempt": failures, "max_attempts": MAX_RESPONSE_ATTEMPTS}, agent_id=agent_id)
                    prompt += "\n上次响应的结束协议异常，未被接受。沿用已保存的工具结果，不重复已完成的操作，重新返回完整 JSON。"
                    until = asyncio.get_running_loop().time() + delay
                    while asyncio.get_running_loop().time() < until:
                        await self.guard(research_id)
                        await asyncio.sleep(min(0.4, max(0, until - asyncio.get_running_loop().time())))
                    continue
                if error.code == "model_output_truncated":
                    await self.guard(research_id)
                    if token_limit >= MAX_OUTPUT_TOKENS:
                        from backend.graphs.arbitration import ArbitrationRequired
                        raise ArbitrationRequired("model_output_truncated", f"模型输出在 {token_limit} token 上限下仍被截断", max_output_tokens=token_limit, original_error=str(error)) from error
                    token_limit = min(MAX_OUTPUT_TOKENS, token_limit * 2)
                    self.store.emit(research_id, "agent.retry_wait", {"code": error.code, "max_output_tokens": token_limit,
                        "message": "输出被截断，已提高额度重新请求完整结果"}, agent_id=agent_id)
                    prompt += "\n上次输出达到 token 上限，现已增加额度。沿用已完成的工具结果，不重复执行已完成的操作；直接返回完整 JSON 对象。"
                    turn_id += f":more-output:{token_limit}"
                    continue
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
                    from backend.graphs.arbitration import ArbitrationRequired
                    raise ArbitrationRequired("invalid_output", "相同输出校验错误未取得进展：" + signature,
                        validation_error=signature, source_output=result if "result" in locals() else None) from error
                last_error = signature
                prompt += "\n上次输出校验失败，请更正并完整返回 JSON：" + signature
                turn_id = turn_id + ":repair"
        return await self.accept_turn_output(research_id, agent_id, role, task_id, turn_id, schema, result, context,
            conversation_id=conversation_id, line_id=line_id, message_kind=message_kind, recipients=recipients,
            edge_id=edge_id, publish_message=publish_message, reply_to=reply_to, output_validator=output_validator)

    async def accept_turn_output(self, research_id, agent_id, role, task_id, turn_id, schema, result, context,
                                 conversation_id=None, line_id=None, message_kind="discussion", recipients=None,
                                 edge_id=None, publish_message=True, reply_to=None, output_validator=None,
                                 decision_source=None, arbitration_id=None, repaired_for_agent_id=None):
        validator = self.validate_recovery_output if decision_source == "god" else self.validate_output
        result = validator(research_id, repaired_for_agent_id or agent_id, schema, result, context, output_validator)
        project = await self.guard(research_id)
        if not publish_message:
            return result
        conversation_id = conversation_id or f"{research_id}:{task_id}"
        adapter = self.demo if project["mode"] == "demo" else self.live
        inbox_consumer = digest([research_id, agent_id, task_id, turn_id, "received-messages"])
        message_id = adapter.message_reference(research_id, agent_id, task_id, turn_id) if hasattr(adapter, "message_reference") else None
        message_fields = {"source_system": "demo_fixture" if project["mode"] == "demo" else "dsh",
            "source_event_id": digest([research_id, agent_id, task_id, turn_id]), "kind": message_kind,
            "line_id": line_id, "recipients": recipients or [], "edge_id": edge_id,
            "authored_by_agent_id": agent_id, "task_id": task_id, "prompt_version": VERSION,
            "demo": project["mode"] == "demo", "structured": result}
        if decision_source:
            message_fields.update(decision_source=decision_source, arbitration_id=arbitration_id,
                repaired_for_agent_id=repaired_for_agent_id)
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

    async def execute_god_turn(self, request, sections, attempt, feedback=None, max_output_tokens=65536):
        """One independent supervised generation; arbitration owns its retry budget."""
        from packages.contracts.research import GodDecision
        project = await self.guard(request["research_id"])
        if (project["provider_config_id"] != request["provider_config_id"] or
                project.get("credential_version", 0) != request["credential_version"] or
                project["config_snapshot"]["provider"].get("model") != request["model"]):
            raise RuntimeBlocked("arbitration_config_changed", "模型或凭据版本已改变，旧仲裁配置不能静默切换")
        aid = request["arbitration_id"]
        cid = f"{request['research_id']}:god:{aid}"
        agent_id = "god/" + aid
        self.store.create_conversation(request["research_id"], cid, kind="arbitration", title="God 自动仲裁",
            participants=[{"type": "agent", "id": agent_id}], line_id=request.get("line_id"))
        context = {"arbitration_request": request, "snapshot": sections, "repair_feedback": feedback}
        prompt_context = {"arbitration_request": {k: v for k, v in request.items()
            if k not in {"history", "attempts", "recovered_response"}}, "repair_feedback": feedback, **god_prompt_context(sections)}
        prompt = f"你的独立执行身份是 {agent_id}。\n" + role_prompt(Role.GOD) + "\n本次冻结仲裁请求：\n" + json.dumps(prompt_context, ensure_ascii=False)
        wire_contract = model_contract(GodDecision, context)
        prompt += "\n返回允许动作之一的完整 JSON（系统身份和哈希由程序填写）：\n" + json.dumps(wire_contract.model_json_schema(), ensure_ascii=False)
        adapter = self.demo if project["mode"] == "demo" else self.live
        output = await adapter.execute_turn(research_id=request["research_id"], agent_id=agent_id, role="god",
            task_id=aid + ":judge", turn_id=f"judge:{attempt}", prompt=prompt, schema=wire_contract,
            direction_epoch=request.get("direction_epoch", 1), model=project["config_snapshot"]["provider"], context=context,
            runtime_context={"conversation_id": cid, "line_id": request.get("line_id"),
                "direction_epoch": request.get("direction_epoch"), "arbitration_id": aid,
                "snapshot_hash": request["snapshot_hash"], "max_output_tokens": max_output_tokens})
        output = bind_metadata(GodDecision, output, context)
        # The service validates the action and only then publishes an applied summary.
        self.store.create_message(request["research_id"], cid, agent_id,
            output.get("content_markdown", json.dumps(output, ensure_ascii=False)), source_system="god_generation",
            source_event_id=f"{aid}:judge:{attempt}", kind="arbitration", structured=output,
            line_id=request.get("line_id"), arbitration_id=aid, decision_source="god", demo=project["mode"] == "demo")
        return output

    async def recover_action(self, request, decision, *, schema=None, context=None, agent_id=None, role=None,
                             task_id=None, turn_id=None, conversation_id=None, line_id=None,
                             output_validator=None, **options):
        action, context = decision["action"], context or {}
        if action in {"request_evidence", "needs_external_input"} and self.dependencies.can_route(request, decision):
            return await self.dependencies.wait(request, decision)
        if action == "decompose_task":
            if context.get("recovery_part"):
                raise RuntimeBlocked("recovery_depth_exceeded", "拆分子任务不得递归拆分，请在同一恢复链替换有限计划")
            try:
                result = await self.decompositions.suspend(request, decision, schema=schema, context=context,
                    agent_id=agent_id, role=role, task_id=task_id, turn_id=turn_id, line_id=line_id,
                    output_validator=output_validator, **options)
            except RuntimeBlocked as error:
                if error.code != "recovery_validation_failed":
                    raise
                return await self.arbitration.repair_turn(request["research_id"], agent_id, role, task_id, turn_id,
                    schema, context, reason_code="invalid_output", diagnostic={"original_error": request["reason_code"],
                        "plan_id": request["id"] + ":plan", "child_validation_error": str(error)},
                    conversation_id=conversation_id, line_id=line_id, output_validator=output_validator,
                    recovery_chain_id=request["recovery_chain_id"], **options)
            return await self.accept_turn_output(request["research_id"], "god/" + request["id"], role,
                task_id, turn_id + ":reassembled", schema, result, context, conversation_id=conversation_id,
                line_id=line_id, output_validator=output_validator, decision_source="god",
                arbitration_id=request["id"], repaired_for_agent_id=agent_id, **options)
        if action == "request_revision":
            if decision["target_role"] != role:
                raise ValueError("An output revision must remain with its original role")
            await self.arbitration.apply(request["id"], decision, effect_kind="request_revision")
            self.store.put("recovery_revision", request["id"], {"agent_id": agent_id, "role": role,
                "source_task_id": task_id, "source_turn_id": turn_id, "instructions": decision["instructions"],
                "allowed_changes": decision["allowed_changes"], "acceptance_checks": decision["acceptance_checks"],
                "recovery_chain_id": request["recovery_chain_id"]}, request["research_id"])
            result = await self.turn(request["research_id"], agent_id, role, task_id,
                turn_id + ":god-revision:" + request["id"], schema,
                {**context, "instruction": decision["instructions"], "god_revision_of": request["id"],
                    "allowed_changes": decision["allowed_changes"], "acceptance_checks": decision["acceptance_checks"]},
                conversation_id=conversation_id, line_id=line_id, output_validator=output_validator,
                recovery_chain_id=request["recovery_chain_id"], **options)
            self.store.patch("recovery_revision", request["id"], status="fulfilled", result_hash=digest(result))
            self.store.close_recovery_chain(request["recovery_chain_id"])
            return result
        if action == "request_evidence":
            return await self.decompositions.request_evidence(request, decision, context=context, schema=schema,
                agent_id=agent_id, role=role, task_id=task_id, turn_id=turn_id, line_id=line_id,
                conversation_id=conversation_id, output_validator=output_validator, **options)
        if action == "finish_scope":
            target = decision.get("line_id") or line_id
            if line_id and target != line_id:
                raise ValueError("Arbitration cannot finish another research line")
            if not target and request["scope_kind"] not in {"boss_assignment", "discussion"}:
                raise ValueError("Only project Boss arbitration may finish the project")
            boss_decision = BossDecision(content_markdown=decision["content_markdown"],
                action="finish_line" if target else "finish_project", assignments=[], memory_updates=[],
                request_report=True, rationale=decision["rationale"]).model_dump()
            operation_id = request["id"] + ":finish"
            commit = await asyncio.to_thread(self.store.commit_boss_decision, request["research_id"], operation_id, boss_decision, self.control)
            if commit.get("status") != "applied":
                raise RuntimeBlocked("confirmation_pending", "God 收束决定尚待持久控制确认")
            effects = await asyncio.to_thread(self.store.apply_boss_effects, request["research_id"], operation_id,
                target, {"decision": boss_decision, "submission": context.get("submission"), "cycle_history": []}, self.control)
            if effects.get("status") != "applied":
                raise RuntimeBlocked("confirmation_pending", "God 收束效果尚待控制确认")
            applied = self.store.get("arbitration_request", request["id"])
            if applied["status"] != "applied":
                raise RuntimeBlocked("confirmation_pending", "God 收束效果尚未完成原子业务确认")
            ending = set(effects.get("finish_line_ids", []))
            for run in self.store.list("run", request["research_id"]):
                if run.get("line_id") in ending and run.get("state") in {"starting", "running"}:
                    await asyncio.to_thread(self.runner.request_stop, run.get("experiment_run_id", run["id"]), "early_stopped")
            chain = self.store.get("recovery_chain", request["recovery_chain_id"])
            if chain and chain["status"] != "cancelled":
                self.store.close_recovery_chain(request["recovery_chain_id"])
            raise RuntimeBlocked("line_finished", decision["rationale"])
        raise RuntimeBlocked("arbitration_action_unhandled", "仲裁动作缺少合法恢复位置：" + action)

    async def supervisor(self, task):
        research_id = task["research_id"]
        p = self.store.project(research_id)
        try:
            control_status = await asyncio.to_thread(self.control.status, research_id)
        except Exception as error:
            raise RuntimeBlocked("control_unavailable", "独立控制服务不可用，拒绝开始研究") from error
        if control_status.get("stop_id"):
            await asyncio.to_thread(self.store.sync_stop, research_id, control_status)
            raise ResearchStopped("研究在调度前已经终止")
        self.store.assert_progress(p)
        if not p["started_at"]:
            p = self.store.start_project(research_id)
        # The business clock may have committed before registration reached
        # Control. Reconcile idempotently with the original frozen deadline.
        await asyncio.to_thread(self.control.register_project, research_id, p["deadline_at"], p["control_generation"])
        await self.guard(research_id)
        if p["mode"] == "live" and not self.live.availability()["available"]:
            raise RuntimeBlocked("dsh_sdk_missing", self.live.availability()["message"])
        return await self.boss({**task, "kind": "boss", "payload": {"initial": True}})

    async def boss(self, task):
        # Each conflict gets a new durable deliberation identity. Old decisions,
        # memory baselines and unapplied effects remain historical facts.
        while True:
            revision = self.store.get("boss_reconsideration", task["id"])
            if revision:
                task = revision["replacement_task"]
                continue
            try:
                return await self._boss(task)
            except (RuntimeBlocked, StoreError) as error:
                if error.code != "memory_version_conflict":
                    raise
                project = await self.guard(task["research_id"])
                with self.store.connect(True) as c:
                    revision = self.store._get(c, "boss_reconsideration", task["id"])
                    if not revision:
                        replacement = {**task, "id": task["id"] + f":memory-review:{project['memory_version']}",
                            "payload": {**task["payload"], "memory_conflict_of": task["id"],
                                "memory_conflict": str(error)}}
                        self.store._put(c, "boss_reconsideration", task["id"],
                            {"replacement_task": replacement}, task["research_id"])
                        self.store._emit(c, task["research_id"], "boss.reconsideration_requested",
                            {"task_id": task["id"], "replacement_id": replacement["id"], "reason": error.code})

    async def _boss(self, task):
        research_id, payload = task["research_id"], task["payload"]
        async with self.boss_locks.setdefault((research_id, task["id"]), asyncio.Lock()):
            p = await self.guard(research_id)
            context = {"instruction": "初始分派所有研究线" if payload.get("initial") else "审议冻结提交包与 Verify 事实，决定后续研究方向", "topic": p["topic"],
                "description": p.get("description", ""), "workspace_path": p.get("workspace_path"),
                "initial": payload.get("initial", False), "lines": self.store.lines(research_id), **payload,
                "memory_index": self.store.list("memory_node", research_id), "deadline_at": p["deadline_at"]}
            # A checkpointed discussion may return its old decision on retry.
            # Keep its memory baseline too, including checkpoints predating this record.
            memory_context_id = task["id"] + recovery_suffix()
            memory_context = self.store.get("boss_memory_context", memory_context_id)
            if memory_context is None:
                snapshot = await self.discussions.graph.aget_state({"configurable": {
                    "thread_id": f"research/{research_id}/discussion/{task['id']}:discussion{recovery_suffix()}:serial-v2"}})
                memory_index = snapshot.values.get("context", {}).get("memory_index", context["memory_index"])
                memory_context = self.store.put("boss_memory_context", memory_context_id, {"memory_index": memory_index}, research_id)
            context["memory_index"] = memory_context["memory_index"]
            if payload.get("dependency_wait_id"):
                dependency = self.store.get("dependency_wait", payload["dependency_wait_id"])
                if not dependency or dependency["status"] != "waiting_children":
                    raise RuntimeBlocked("task_superseded", "内部依赖协调请求已结束")
                context["instruction"] = "协调本项目内部研究线依赖。复用正在运行或待重试的上游任务，不重复分派同一周期。由设计阶段定义并冻结折划分/清洗/交接接口；后续 Runner 产出的基线数字是执行阶段前置条件，不能要求 Exploration 先运行实验才能冻结设计。可通过带来源的共享记忆发布接口、阶段安排或实际已有结果；不得把协议建议或未验证数字写成实测。"
                context["dependency_submissions"] = [s for identity in payload.get("upstream_submission_ids", [])
                    if (s := self.store.get("submission", identity)) and s["research_id"] == research_id]
                context["dependency_verifications"] = [v for identity in payload.get("upstream_verification_ids", [])
                    if (v := self.store.get("verification", identity)) and v["research_id"] == research_id]
            if payload.get("submission_id"):
                context["submission"] = self.store.get("submission", payload["submission_id"])
                context["cycle_history"] = self.store.list("verification", research_id, cycle_id=payload.get("cycle_id"))
            context["received_handoffs"] = await asyncio.to_thread(self.store.consume_inbox,
                research_id, "boss", task["id"] + ":inbox", payload.get("cycle_id"), self.control)
            members = self.members(p, "boss")
            discussion = await self.discussions.run(research_id, task["id"] + ":discussion", "boss", members, context)
            decision = BossDecision.model_validate(discussion["selected_payload"]).model_dump()
            marker = (current_task.get() or {}).get("recovery_marker") or {}
            if marker.get("resume_point") == "boss.validate_assignment":
                recovery_request = self.store.get("arbitration_request", marker["arbitration_id"])
                if marker["decision"]["action"] == "repair_output":
                    decision = BossDecision.model_validate(marker["decision"]["output"]).model_dump()
                    await self.arbitration.apply(recovery_request["id"], marker["decision"])
                else:
                    decision = await self.recover_action(recovery_request, marker["decision"], schema=BossDecision,
                        context=context, agent_id=members[0], role="boss", task_id=task["id"], turn_id="repair_assignment")
            if p.get("finish_requested") and decision["action"] in {"assign", "continue", "request_evidence", "change_direction"}:
                raise RuntimeBlocked("line_finished", "Boss 已结束项目，后续只能封存结果与报告")
            def validate_assignment(value):
                identities = [a["line_id"] for a in value["assignments"]]
                legal = {line["line_id"] for line in context["lines"]}
                if len(set(identities)) != len(identities) or set(identities) - legal:
                    raise ValueError("Boss 分派包含未知或重复研究线")
                if payload.get("initial") and (set(identities) != legal or value["action"] != "assign"):
                    raise ValueError("Boss 初始分派必须完整覆盖固定研究线")
                if payload.get("dependency_wait_id") and value["action"] != "change_direction":
                    live_lines = {t.get("payload", {}).get("line_id") for t in self.store.list("task", research_id)
                        if t["kind"] == "line" and t["status"] in {"pending", "running", "retry_wait"}}
                    if set(identities) & live_lines:
                        raise ValueError("上游任务正在运行或恢复；协调必须复用当前任务，禁止重复分派周期")
            try:
                validate_assignment(decision)
            except ValueError as error:
                resolution = await self.arbitration.resolve(research_id=research_id, source_task_id=task["id"],
                    scope_kind="boss_assignment", scope_id=task["id"], reason_code="invalid_boss_assignment",
                    origin_node="boss.validate_assignment", context={**context, "invalid_assignment": decision,
                        "validation_error": str(error)}, target_schema=BossDecision,
                    allowed_actions=["repair_output", "request_revision", "needs_external_input"], output_validator=validate_assignment)
                request = self.store.get("arbitration_request", resolution["arbitration_id"])
                if resolution["action"] == "repair_output":
                    decision = BossDecision.model_validate(resolution["output"]).model_dump()
                    validate_assignment(decision)
                    await self.arbitration.apply(request["id"], resolution, effect_kind="boss_assignment")
                else:
                    decision = await self.recover_action(request, resolution, schema=BossDecision, context=context,
                        agent_id=members[0], role="boss", task_id=task["id"], turn_id="repair_assignment", output_validator=validate_assignment)
                self.store.close_recovery_chain(request["recovery_chain_id"])
            if payload.get("dependency_wait_id"):
                dependency = self.store.get("dependency_wait", payload["dependency_wait_id"])
                source = self.store.get("line", f"{research_id}:{dependency['line_id']}")
                if (dependency["status"] != "waiting_children" or not source or source.get("finish_requested") or
                        source.get("cycle_id") != dependency.get("source_cycle_id") or
                        source["direction_epoch"] != dependency["direction_epoch"] or
                        p["control_generation"] != dependency["control_generation"]):
                    raise RuntimeBlocked("task_superseded", "原依赖已结束，旧 Boss 回复仅保留历史")
            confirmed = await asyncio.to_thread(self.store.commit_boss_decision, research_id, task["id"], decision, self.control)
            if confirmed.get("status") != "applied":
                raise RuntimeBlocked("confirmation_pending", "Boss 决议尚未取得持久控制确认")
            if decision["memory_updates"]:
                # Deliberation stays concurrent. Only the read/check/write of
                # shared memory is serialized, with unchanged targets rebased
                # onto the latest version rather than the pre-discussion one.
                async with self.boss_memory_locks.setdefault(research_id, asyncio.Lock()):
                    latest = await self.guard(research_id)
                    original_nodes = {node["node_id"]: node for node in context["memory_index"]}
                    current_nodes = {node["node_id"]: node for node in self.store.list("memory_node", research_id)}
                    conflicts = [update["node_id"] for update in decision["memory_updates"]
                        if update.get("node_id") and current_nodes.get(update["node_id"]) != original_nodes.get(update["node_id"])
                        and (current_nodes.get(update["node_id"]) or {}).get("boss_decision_id") != task["id"]]
                    if conflicts:
                        raise RuntimeBlocked("memory_version_conflict", "共享记忆目标已被其他 Boss 决议更新，需重新审议：" + ", ".join(conflicts))
                    memory_result = await asyncio.to_thread(self.store.write_memory, research_id, task["id"], "updates",
                        decision["memory_updates"], latest["memory_version"], self.control)
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
        recovery = self.store.get("implementation_recovery", state.get("implementation_recovery_id", ""))
        marker = (current_task.get() or {}).get("recovery_marker") or {}
        if marker.get("resume_point") == "line.implement":
            request = self.store.get("arbitration_request", marker["arbitration_id"])
            if marker["decision"]["action"] == "repair_output":
                recovery = {"output": marker["decision"]["output"]}
                assert_spec_unchanged(spec, Implementation.model_validate(recovery["output"]))
                await self.arbitration.apply(request["id"], marker["decision"])
            elif marker["decision"]["action"] == "request_revision":
                await self.arbitration.apply(request["id"], marker["decision"])
                self.store.put("spec_revision_request", request["id"], {"line_id": state["line_id"],
                    "spec_id": state["spec_id"], "god_revision": marker["decision"]}, state["research_id"])
                self.store.patch("task", current_task.get()["id"], recovery_marker=None)
                current_task.get()["recovery_marker"] = None
                return {"implementation_route": "design", "revision": state["revision"] + 1}
            else:
                await self.recover_action(request, marker["decision"], context={"spec": spec.model_dump()}, line_id=state["line_id"])
        result = recovery["output"] if recovery else await self.turn(state["research_id"], f"{state['line_id']}/implementation", "implementation", implementation_id, "code", Implementation,
            {"instruction": "完整实现或修复冻结计划，返回相对路径源文件与 Python 入口，禁止改动固定配置。修复必须根据真实失败证据恢复既定方案", "spec": spec.model_dump(), "spec_hash": digest(spec.model_dump()), "direction_epoch": state["direction_epoch"],

             "resource_feedback": self.store.get("resource_feedback", state["spec_id"]),
             "prior_runtime_assessments": self.store.list("run_assessment", state["research_id"], spec_id=state["spec_id"]),
             "prior_runs": [run for run in self.store.list("run", state["research_id"]) if run.get("spec_id") == state["spec_id"]]},
            conversation_id=implementation_id, line_id=state["line_id"])
        implementation = Implementation.model_validate(bind_metadata(Implementation, result, {"spec": spec.model_dump()}))
        await self.line_open(state)
        try:
            assert_spec_unchanged(spec, implementation)
        except ValueError as error:
            # A malformed configuration echo should be corrected by its author
            # before reopening both scientific design groups. Never overwrite it.
            repair_id = implementation_id + ":config-repair" + recovery_suffix()
            if not self.store.get("implementation_config_repair", repair_id):
                self.store.put("implementation_config_repair", repair_id, {"spec_id": state["spec_id"],
                    "original_output": result, "reason": str(error), "status": "requested"}, state["research_id"])
                self.store.emit(state["research_id"], "implementation.config_repair_requested",
                    {"implementation_id": implementation_id, "spec_id": state["spec_id"]}, line_id=state["line_id"])
            result = await self.turn(state["research_id"], f"{state['line_id']}/implementation", "implementation",
                implementation_id, "code:config-repair", Implementation,
                {"instruction": "纠正上次实现与冻结配置的不一致。actual_config 如提供应与 spec.fixed 一致，不得混入其他字段；系统哈希由程序填写。同步核查并修正源代码，禁止只改配置声明掩盖代码偏离。若认为冻结方案无法实现，保留真实不一致，交原设计组处理。返回完整实现。",
                    "spec": spec.model_dump(), "spec_hash": digest(spec.model_dump()),

                    "configuration_error": str(error), "prior_implementation": result,
                    "direction_epoch": state["direction_epoch"]},
                conversation_id=implementation_id, line_id=state["line_id"])
            implementation = Implementation.model_validate(bind_metadata(Implementation, result, {"spec": spec.model_dump()}))
            self.store.patch("implementation_config_repair", repair_id,
                status="corrected" if implementation.spec_hash == digest(spec.model_dump()) and implementation.actual_config == spec.fixed else "requires_design_revision")
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
                resolution = await self.arbitration.resolve(research_id=state["research_id"], source_task_id=state["task_id"],
                    scope_kind="implementation", scope_id=state["cycle_id"], reason_code="design_deadlock", origin_node="line.implement",
                    context={"spec": spec.model_dump(), "implementation_output": result, "signature": signature,
                        "prior_revision_requests": previous_requests, "validation_error": str(error)}, target_schema=Implementation,
                    allowed_actions=["repair_output", "request_revision", "finish_scope", "needs_external_input"],
                    line_id=state["line_id"], output_validator=lambda output: assert_spec_unchanged(spec, Implementation.model_validate(output)))
                request = self.store.get("arbitration_request", resolution["arbitration_id"])
                if resolution["action"] == "repair_output":
                    repaired = Implementation.model_validate(resolution["output"])
                    assert_spec_unchanged(spec, repaired)
                    self.store.put("implementation_recovery", implementation_id, {"output": repaired.model_dump(),
                        "arbitration_id": request["id"]}, state["research_id"])
                    await self.arbitration.apply(request["id"], resolution)
                    # The next code attempt consumes the validated repair; no old file/plan is rewritten.
                    self.store.close_recovery_chain(request["recovery_chain_id"])
                    return {"implementation_route": "implement", "code_attempt": state.get("code_attempt", 1) + 1,
                        "implementation_recovery_id": implementation_id}
                if resolution["action"] == "request_revision":
                    self.store.put("spec_revision_request", request["id"], {"line_id": state["line_id"], "spec_id": state["spec_id"],
                        "reason": resolution["instructions"], "god_revision": resolution, "recovery_chain_id": request["recovery_chain_id"]}, state["research_id"])
                    await self.arbitration.apply(request["id"], resolution, effect_kind="request_revision")
                    return {"implementation_route": "design", "revision": state["revision"] + 1}
                await self.recover_action(request, resolution, context={"spec": spec.model_dump()}, line_id=state["line_id"])
            return {"implementation_route": "design", "revision": state["revision"] + 1}
        workspace = self.store.data_dir / "projects" / state["research_id"] / "workspaces" / state["line_id"] / f"epoch-{state['direction_epoch']}" / f"revision-{state['revision']}" / f"code-{state.get('code_attempt', 1)}"
        workspace.mkdir(parents=True, exist_ok=True)
        for relative, content in implementation.files.items():
            target = (workspace / relative).resolve()
            if not target.is_relative_to(workspace.resolve()) or Path(relative).is_absolute():
                raise RuntimeBlocked("unsafe_code_path", "Implementation 文件越出研究线工作区")
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content, encoding="utf-8")
        entrypoint = (workspace / implementation.entrypoint).resolve()
        if not entrypoint.is_relative_to(workspace.resolve()) or not entrypoint.is_file():
            raise RuntimeBlocked("invalid_entrypoint", "实验入口必须是工作区内的文件")
        result = implementation.model_dump()
        code_version = digest(implementation.files)
        self.store.put("implementation", implementation_id, {**result, "spec_id": state["spec_id"], "cwd": str(workspace), "code_version": code_version}, state["research_id"])
        for chain in self.store.list("recovery_chain", state["research_id"]):
            if (chain.get("source_task_id") == state["task_id"] and chain.get("scope_id") == state["cycle_id"] and
                    chain.get("resume_point") == "line.implement" and chain["status"] not in {"fulfilled", "cancelled"}):
                self.store.close_recovery_chain(chain["id"])
        return {"implementation_id": implementation_id, "implementation_route": "organize", "implementation_recovery_id": ""}

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
        allocation = await self.wait_for_allocation(state, request)
        if allocation["state"] == "unsatisfiable":
            self.store.put("resource_feedback", state["spec_id"], allocation, state["research_id"])
            return {"allocation_route": "implement", "code_attempt": state.get("code_attempt", 1) + 1}
        return {"allocation_id": allocation["allocation_id"], "allocation_route": "run_experiment"}

    async def wait_for_allocation(self, state, request):
        while True:
            await self.line_open(state)
            allocation = await asyncio.to_thread(self.runner.request_allocation, request)
            if allocation["state"] == "unsatisfiable":
                self.store.emit(state["research_id"], "allocation.unsatisfiable", allocation, line_id=state["line_id"])
                return allocation
            if allocation["state"] == "granted":
                self.store.emit(state["research_id"], "allocation.granted", allocation, line_id=state["line_id"])
                return allocation
            if allocation.get("bound_run_id"):
                return allocation  # Replay the original start, never renew an admitted run.
            if allocation["state"] != "queued":
                raise RuntimeBlocked("allocation_unavailable", "资源申请已关闭，不能重新启动实验")
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
        monitor["evidence_log_hash"] = window["log_hash"]
        if monitor["request_early_stop"]:
            allowed = {str(rule.get("rule_id", i)) for i, rule in enumerate(spec.early_stop_rules)} | {"execution_unusable"}
            if monitor["rule_id"] not in allowed:
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
        allocation = self.store.get("allocation", state["allocation_id"])
        if not allocation.get("bound_run_id"):
            # A checkpoint may outlive the unbound grant's short lease. Recheck
            # capacity and obtain its current fence before attempting a start.
            allocation = await self.wait_for_allocation(state, allocation)
        self.store.update_line(state["research_id"], state["line_id"], phase="running")
        impl = self.store.get("implementation", state["implementation_id"])
        run_id = state["implementation_id"] + ":run-1"
        payload = {**allocation, "experiment_run_id": run_id, "start_operation_id": run_id + ":start", "command": [sys.executable, "-u", impl["entrypoint"], *impl["arguments"]],
                   "cwd": impl["cwd"], "code_version": impl["code_version"]}
        try:
            run = await asyncio.to_thread(self.runner.start, payload)
        except Exception:
            await self.guard(state["research_id"])
            # Lost acknowledgements may hide a live or already finished run.
            # Rejoin only the exact admitted operation; never submit a new one.
            try:
                recovered = await asyncio.to_thread(self.runner.status, run_id)
            except ControlUnavailable:
                # A second lost response remains a durable infrastructure retry.
                raise
            except Exception:
                raise RuntimeBlocked("runner_unknown", "启动应答失败且无法确认运行状态，保留资源等待对账")
            proof = recovered.get("terminal_proof", {})
            bound = ("research_id", "allocation_id", "start_operation_id", "experiment_run_id", "control_generation",
                     "grant_revision", "spec_id", "spec_version", "spec_hash", "code_version")
            if any(recovered.get(k) != payload.get(k) for k in bound):
                raise
            if recovered.get("state") not in {"starting", "running", "reconciling"} and (
                    recovered.get("state") not in {"execution_failed", "interrupted", "completed", "succeeded", "early_stopped"}
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
        if transition["route"] == "awaiting_confirmation_sync":
            raise RuntimeBlocked("confirmation_pending", "验证等待控制确认，稍后自动重试")
        if transition["route"] == "halted":
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
        from langgraph.types import Command
        state = {"research_id": task["research_id"], "task_id": task["id"], "revision": 1, **task["payload"]}
        config = {"configurable": {"thread_id": f"research/{state['research_id']}/line/{state['line_id']}/{state['direction_epoch']}/{state['cycle_id']}"}, "recursion_limit": 100000}
        snapshot = await self.line_graph.aget_state(config)
        if snapshot.values and not snapshot.next and not any(t.interrupts for t in snapshot.tasks):
            return snapshot.values
        initial = Command(resume=task["recovery_outcome"]) if task.get("recovery_outcome") else None if snapshot.values else state
        result = await self.line_graph.ainvoke(initial, config)
        if result.get("__interrupt__"):
            raise TaskSuspended(task["id"], config["configurable"]["thread_id"], result["__interrupt__"][0].value["resume_token"])
        return result

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
