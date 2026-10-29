"""Bounded, durable arbitration. A judgment is data, never executable authority."""
from __future__ import annotations

import asyncio
import json
import os
import uuid
from datetime import datetime, timedelta, timezone

from backend.adapters.dsh import ResearchStopped, RuntimeBlocked
from backend.adapters.token_limits import MAX_OUTPUT_TOKENS
from backend.persistence.store import TERMINAL, now
from backend.graphs.tasks import ScopeResolved, current_task, recovery_suffix
from packages.contracts.research import DecompositionPlan, GodDecision, Role, digest


ARBITRABLE = {"decision_blocked", "design_deadlock", "invalid_group_decision",
              "invalid_boss_assignment", "invalid_output", "model_output_truncated"}


class ArbitrationRequired(RuntimeBlocked):
    def __init__(self, code, message, **recovery):
        super().__init__(code, message)
        self.recovery = recovery


def clean_context(value):
    """Exclude capabilities and credentials before freezing model input."""
    if isinstance(value, dict):
        return {k: clean_context(v) for k, v in value.items()
                if not any(s in k.lower() for s in ("api_key", "secret", "capability", "authorization", "credential_ref"))}
    if isinstance(value, (list, tuple)):
        return [clean_context(v) for v in value]
    return value


def parse_time(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class ArbitrationService:
    def __init__(self, engine):
        self.engine, self.store = engine, engine.store
        self.owner = f"god-{os.getpid()}-{uuid.uuid4().hex[:8]}"
        self.locks = {}  # Per request, never one lock for all God calls.

    def _end(self, arbitration_id, status, **changes):
        """Late cancellation must preserve an already committed or stale result."""
        current = self.store.get("arbitration_request", arbitration_id)
        if current["status"] in {"applied", "cancelled", "stale", "needs_external_input", "exhausted"}:
            return current
        return self.store.transition_arbitration(arbitration_id, status, **changes)

    def _scope_current(self, request):
        p = self.store.project(request["research_id"])
        if p["status"] in TERMINAL or p["control_generation"] != request["control_generation"]:
            return "cancelled"
        if p.get("deadline_at") and parse_time(p["deadline_at"]) <= datetime.now(timezone.utc):
            return "cancelled"
        scope = self.store.get("arbitration_scope", digest([request["research_id"], request["scope_kind"], request["scope_id"]]))
        if scope and scope.get("scope_revision") != request["scope_revision"]:
            return "stale"
        line_id = request.get("line_id")
        if line_id:
            line = self.store.get("line", f"{p['id']}:{line_id}")
            if not line or line.get("finish_requested"):
                return "cancelled"
            if line["direction_epoch"] != request["direction_epoch"]:
                return "stale"
        return None

    async def resolve(self, *, research_id, source_task_id, scope_kind, scope_id, reason_code,
                      origin_node, context, target_schema=None, allowed_actions=None, line_id=None,
                      scope_revision=None, recovery_chain_id=None, output_validator=None):
        if reason_code not in ARBITRABLE:
            raise RuntimeBlocked(reason_code, "该失败不属于自动仲裁范围")
        p = await self.engine.guard(research_id)
        root_task = current_task.get()
        if root_task:
            source_task_id = root_task["id"]
            task = self.store.get("task", source_task_id)
            if task and task.get("status") == "running":
                self.store.patch("task", source_task_id, execution_phase="arbitrating")
        sections = clean_context(context)
        if recovery_suffix():
            sections["recovery_generation"] = root_task["recovery_generation"]
        schema = target_schema.model_json_schema() if target_schema else {}
        sections = {**sections, "target_schema": schema}
        snapshot_hash = digest(sections)
        revision = str(scope_revision or digest({k: sections.get(k) for k in
            ("candidates", "issues", "spec", "direction_epoch", "target_schema", "original_task")}))
        fingerprint = digest({"reason": reason_code, "node": origin_node, "context": sections})
        key = digest([research_id, source_task_id, scope_kind, scope_id, revision,
                      reason_code, fingerprint, snapshot_hash, 1])
        arbitration_id = "arb-" + key[:40]
        snapshot_id = arbitration_id + ":snapshot"
        line = self.store.get("line", f"{research_id}:{line_id}") if line_id else None
        created = datetime.now(timezone.utc)
        deadline = min(created + timedelta(seconds=600), parse_time(p["deadline_at"]))
        actions = allowed_actions or (["decompose_task", "request_revision", "needs_external_input"]
            if reason_code == "model_output_truncated" else ["repair_output", "request_revision", "needs_external_input"])
        chain_id = recovery_chain_id or "chain-" + digest([research_id, source_task_id, scope_id, origin_node])[:40]
        if recovery_suffix() and not chain_id.endswith(recovery_suffix()):
            chain_id += recovery_suffix()
        request = {"arbitration_id": arbitration_id, "schema_version": 1, "policy_version": 1,
            "research_id": research_id, "source_task_id": source_task_id, "scope_kind": scope_kind,
            "scope_id": scope_id, "origin_node": origin_node, "origin_checkpoint": context.get("origin_checkpoint"),
            "reason_code": reason_code, "failure_fingerprint": fingerprint, "scope_revision": revision,
            "direction_epoch": line["direction_epoch"] if line else 1, "control_generation": p["control_generation"],
            "snapshot_id": snapshot_id, "snapshot_hash": snapshot_hash,
            "source_event_seq": self._latest_seq(research_id), "target_schema_name": target_schema.__name__ if target_schema else "",
            "target_schema_hash": digest(schema), "allowed_actions": actions, "resume_point": origin_node,
            "provider_config_id": p["provider_config_id"], "credential_version": p.get("credential_version", 0),
            "model": p["config_snapshot"]["provider"].get("model", "deepseek-flash"),
            "recovery_chain_id": chain_id, "created_at": created.isoformat(), "judge_deadline_at": deadline.isoformat(),
            "expires_at": p["deadline_at"]}
        if line_id:
            request["line_id"] = line_id
        if not self.store.get("arbitration_request", arbitration_id):
            self.store.put("arbitration_scope", digest([research_id, scope_kind, scope_id]),
                {"scope_revision": revision, "scope_id": scope_id}, research_id)
        request = self.store.create_arbitration(request, {"sections": sections, "snapshot_hash": snapshot_hash})
        arbitration_id = request.get("arbitration_id", request["id"])
        async with self.locks.setdefault(arbitration_id, asyncio.Lock()):
            request = self.store.get("arbitration_request", arbitration_id)
            invalid = self._scope_current(request)
            if invalid:
                self._end(arbitration_id, invalid, reason="scope_control_changed")
                raise ResearchStopped("仲裁范围已停止、过期或改变")
            if request["status"] in {"decided", "applying", "applied"}:
                decision = self._decision(request)
                self.validate_decision(request, sections, decision, target_schema, output_validator)
                return decision
            if request["status"] in {"exhausted", "needs_external_input", "stale", "cancelled"}:
                if request["status"] == "needs_external_input" and hasattr(self.engine, "dependencies"):
                    decision = self._decision(request)
                    if self.engine.dependencies.can_route(request, decision):
                        return decision
                raise RuntimeBlocked("arbitration_" + request["status"], request.get("diagnostic", "仲裁已结束；请查看仲裁详情"))
            request = await self._claim(request)
            return await self._judge(request, sections, target_schema, output_validator)

    def _latest_seq(self, research_id):
        with self.store.connect() as c:
            return c.execute("SELECT COALESCE(MAX(seq),0) FROM events WHERE research_id=?", (research_id,)).fetchone()[0]

    def _decision(self, request):
        decision = self.store.get("arbitration_decision", request.get("decision_id", request["id"]))
        if decision:
            return decision.get("decision", decision.get("output", decision))
        return request["decision"]

    async def _claim(self, request):
        while True:
            await self.engine.guard(request["research_id"])
            # An expired lease is not proof of process exit.
            executions = [x for x in self.store.list("execution", request["research_id"])
                if x.get("agent_id") == "god/" + request["id"] and x.get("status") in {"registered", "running", "unknown"}]
            if executions and request.get("lease_expires_at") and parse_time(request["lease_expires_at"]) <= datetime.now(timezone.utc):
                raise RuntimeBlocked("execution_unknown", "旧 God 执行尚未完成退出对账，拒绝替代调用")
            claimed = self.store.claim_arbitration(request["id"], self.owner, processes_reconciled=not executions)
            if claimed:
                return claimed
            await asyncio.sleep(0.1)
            request = self.store.get("arbitration_request", request["id"])
            if request["status"] in {"decided", "applying", "applied"}:
                return request
            if request["status"] in {"exhausted", "cancelled", "stale", "needs_external_input"}:
                raise RuntimeBlocked("arbitration_" + request["status"], request.get("diagnostic", "仲裁结束"))

    async def _judge(self, request, sections, schema, validator):
        if request["status"] in {"decided", "applying", "applied"}:
            return self._decision(request)
        rid, aid = request["research_id"], request["id"]
        feedback = request.get("diagnostic")
        token_limit = request.get("max_output_tokens", 65536)
        while request.get("generation_attempts", 0) <= 3:
            recovered = request.get("recovered_response")
            invalid = self._scope_current(request)
            if invalid:
                self._end(aid, invalid, reason="scope_control_changed")
                raise ResearchStopped("God 仲裁范围已经失效")
            remaining = (parse_time(request["judge_deadline_at"]) - datetime.now(timezone.utc)).total_seconds()
            if remaining <= 0 and not recovered:
                break
            try:
                await self.engine.guard(rid)
            except RuntimeBlocked as error:
                if error.code == "research_paused":
                    self.store.transition_arbitration(aid, "retry_wait", owner=self.owner,
                        lease_epoch=request["lease_epoch"], generation_attempts=max(0, request.get("attempt", 1) - 1),
                        attempt=max(0, request.get("attempt", 1) - 1), retry_not_before=now(),
                        diagnostic="研究暂停，尚未启动的 God 回合等待恢复")
                raise
            retry_at = request.get("retry_not_before")
            if retry_at:
                while parse_time(retry_at) > datetime.now(timezone.utc):
                    await self.engine.guard(rid)
                    if datetime.now(timezone.utc) >= parse_time(request["judge_deadline_at"]):
                        break
                    await asyncio.sleep(0.2)
                remaining = (parse_time(request["judge_deadline_at"]) - datetime.now(timezone.utc)).total_seconds()
                if remaining <= 0:
                    break
                claimed = self.store.claim_arbitration(aid, self.owner, processes_reconciled=True)
                if claimed is None:
                    current = self.store.get("arbitration_request", aid)
                    if current["status"] in {"exhausted", "cancelled", "stale"}:
                        raise RuntimeBlocked("arbitration_" + current["status"], current.get("diagnostic", current.get("reason", "仲裁已结束")))
                    # A competing Worker owns the new generation; reuse its result.
                    request = await self._claim(current)
                else:
                    request = claimed
                if request["status"] in {"decided", "applying", "applied"}:
                    return self._decision(request)
            attempt = request.get("generation_attempts", 1)
            request = self.store.transition_arbitration(aid, "running", owner=self.owner,
                lease_epoch=request["lease_epoch"], generation_attempts=attempt, max_output_tokens=token_limit)
            try:
                if recovered:
                    result = recovered["output"]
                else:
                    result = await asyncio.wait_for(self.engine.execute_god_turn(request, sections, attempt,
                        feedback=feedback, max_output_tokens=token_limit), timeout=max(0.01, remaining))
                decision = GodDecision.model_validate(result).model_dump()
                self.validate_decision(request, sections, decision, schema, validator)
                invalid = self._scope_current(request)
                if not await asyncio.to_thread(self.engine.control.allows_progress, rid, request["control_generation"]):
                    invalid = "cancelled"
                if invalid:
                    self._end(aid, invalid, reason="late_response")
                    raise ResearchStopped("停止或方向变化后的 God 回复仅归档")
                execution = next((e for e in self.store.list("execution", rid)
                    if e.get("agent_id") == "god/" + aid and e.get("turn_id") == f"judge:{attempt}"), {})
                accepted = self.store.decide_arbitration(aid, decision, owner=self.owner, lease_epoch=request["lease_epoch"],
                    raw_response=result, source_execution_id=execution.get("execution_attempt_id"))
                if accepted["status"] != "decided":
                    raise RuntimeBlocked("arbitration_" + accepted["status"], "God 回复到达时判决时限或控制状态已失效")
                if decision["action"] == "needs_external_input":
                    if hasattr(self.engine, "dependencies") and self.engine.dependencies.can_route(request, decision):
                        return decision
                    diagnostic = decision["content_markdown"] + "\n缺失：" + "；".join(decision["missing_items"]) + \
                        "\n原因：" + decision["why_unavailable"] + "\n操作：" + decision["operation"]
                    self.store.transition_arbitration(aid, "needs_external_input", diagnostic=diagnostic)
                    raise RuntimeBlocked("arbitration_needs_external_input", diagnostic)
                return decision
            except ResearchStopped:
                self._end(aid, "cancelled", reason="research_stopped")
                raise
            except asyncio.CancelledError:
                # Worker shutdown is recoverable; only the independent stop
                # gate can turn a still-needed judgment into cancellation.
                if await asyncio.to_thread(self.engine.control.allows_progress, rid, request["control_generation"]):
                    self.store.transition_arbitration(aid, "retry_wait", owner=self.owner,
                        lease_epoch=request["lease_epoch"], retry_not_before=now(), diagnostic="Worker 中断，等待执行退出对账后恢复")
                else:
                    self._end(aid, "cancelled", reason="judge_cancelled")
                raise
            except RuntimeBlocked as error:
                if error.code == "research_paused":
                    self.store.transition_arbitration(aid, "retry_wait", owner=self.owner,
                        lease_epoch=request["lease_epoch"], generation_attempts=max(0, attempt - 1),
                        attempt=max(0, attempt - 1), retry_not_before=now(),
                        diagnostic="模型容量等待期间研究暂停，God 回合尚未发起")
                    raise
                if error.code not in {"model_output_truncated", "rate_limited", "model_transport_error", "model_timeout"}:
                    raise
                if error.code == "model_output_truncated":
                    token_limit = min(MAX_OUTPUT_TOKENS, token_limit * 2)
                feedback = f"{error.code}: {str(error)[:1000]}"
            except (ValueError, ConnectionError, TimeoutError) as error:
                feedback = str(error)[:2000]
            if attempt >= 3:
                break
            request = self.store.transition_arbitration(aid, "retry_wait", owner=self.owner,
                lease_epoch=request["lease_epoch"], diagnostic=feedback, recovered_response=None,
                retry_not_before=(datetime.now(timezone.utc) + timedelta(seconds=min(2 ** attempt, 8))).isoformat())
        self._end(aid, "exhausted", diagnostic=feedback or "God 判决阶段超过 600 秒或项目截止时间")
        raise RuntimeBlocked("arbitration_exhausted", f"自动仲裁未通过原验收；原始错误 {request['reason_code']}。{feedback or '判决时限已耗尽'}；现场及尝试已保存，可在仲裁详情查看缺失条件。")

    def validate_decision(self, request, context, decision, schema=None, validator=None):
        if decision["arbitration_id"] != request["id"] or decision["snapshot_hash"] != request["snapshot_hash"]:
            raise ValueError("God response identity/hash does not match frozen request")
        if decision["action"] not in request["allowed_actions"]:
            raise ValueError("God action is not allowed at this recovery point")
        allowed_refs = set()
        def collect(value):
            if isinstance(value, dict):
                for k, v in value.items():
                    if k in {"message_id", "artifact_id", "source_id", "id", "submission_id", "run_id"} and isinstance(v, str):
                        allowed_refs.add(v)
                    collect(v)
            elif isinstance(value, list):
                for v in value:
                    collect(v)
        collect(context)
        if any(ref not in allowed_refs for ref in decision.get("evidence_refs", [])):
            raise ValueError("God evidence reference is outside the frozen authorized snapshot")
        action = decision["action"]
        if action in {"request_evidence", "needs_external_input"} and hasattr(self.engine, "dependencies"):
            self.engine.dependencies.line_ids(request, decision)
        role = context.get("role") or context.get("original_context", {}).get("role")
        if action in {"request_revision", "request_evidence"}:
            if decision["target_role"] not in {r.value for r in Role if r != Role.GOD}:
                raise ValueError("Recovery must target an existing research role")
            if role and decision["target_role"] != role:
                raise ValueError("Recovery cannot expand the original role permissions")
        if action == "request_evidence" and any(ref not in allowed_refs for ref in decision.get("source_refs", [])):
            raise ValueError("Evidence tasks cannot read outside the frozen authorized snapshot")
        if action == "decompose_task":
            plan = DecompositionPlan.model_validate(decision["plan"])
            if plan.target_schema_hash != request["target_schema_hash"]:
                raise ValueError("Decomposition must preserve the original schema")
            if role and any(part.target_role != role for part in plan.parts):
                raise ValueError("Decomposition cannot expand the original role permissions")
            if hasattr(self.engine, "decompositions"):
                self.engine.decompositions.validate_plan(request, decision, schema, context, role)
        if action == "select_candidate":
            if decision["selected_candidate_id"] not in context.get("candidates", {}):
                raise ValueError("God must select an existing frozen candidate")
            if context.get("blockers"):
                raise ValueError("God selection cannot remove unresolved blockers")
        if action == "repair_output":
            if not schema or decision["target_schema_hash"] != request["target_schema_hash"]:
                raise ValueError("God repair must match the original target schema")
            if schema.__name__ == "Vote":
                raise ValueError("God cannot generate a member Vote")
            if schema.__name__ == "Verification":
                source = context.get("diagnostic", {}).get("source_output")
                if isinstance(source, str):
                    try:
                        source = json.loads(source)
                    except ValueError:
                        source = None
                factual_fields = ("passed", "scientific_observation", "evidence", "limitations", "required_changes")
                if not isinstance(source, dict) or any(field not in source for field in factual_fields):
                    raise ValueError("Verification repair requires the original complete verification facts; request Verify revision")
                if any(decision["output"].get(field) != source.get(field) for field in
                       (*factual_fields, "independent_replication") if field in source):
                    raise ValueError("God may repair Verification formatting but cannot change verification facts")
            validate = getattr(self.engine, "validate_recovery_output", self.engine.validate_output)
            validate(request["research_id"], context.get("agent_id", "god"), schema,
                decision["output"], context.get("original_context", context), validator)
        if action == "finish_scope":
            if request["scope_kind"] not in {"discussion", "implementation", "boss_assignment"}:
                raise ValueError("This output repair cannot finish scientific scope")
            line_id = request.get("line_id")
            target = decision.get("line_id")
            if line_id and target not in {None, line_id}:
                raise ValueError("Arbitration cannot finish another research line")
            if not line_id:
                if request["scope_kind"] != "boss_assignment" and context.get("role") != "boss":
                    raise ValueError("Only project Boss arbitration may finish a project scope")
                if target and target not in {line["line_id"] for line in self.store.lines(request["research_id"])}:
                    raise ValueError("Unknown research line in finish_scope")
            if any(ref not in allowed_refs for ref in decision.get("existing_result_refs", [])):
                raise ValueError("Scope closure cannot invent existing results")
            target = decision.get("line_id")
            if request.get("line_id") and target != request["line_id"]:
                raise ValueError("Arbitration cannot finish another research line or its whole project")
            if not request.get("line_id") and role != "boss":
                raise ValueError("Only project Boss scope can finish the project")

    async def apply(self, request_id, decision, *, effect_kind="resume", payload=None):
        request = self.store.get("arbitration_request", request_id)
        invalid = self._scope_current(request)
        if invalid:
            self._end(request_id, invalid, reason="scope_changed_before_apply")
            raise ResearchStopped("仲裁应用前范围已失效")
        await self.engine.guard(request["research_id"])
        return self.store.apply_arbitration_effect(request_id, effect_kind,
            payload or {"decision": decision, "resume_point": request["resume_point"]},
            expected_scope_revision=request["scope_revision"])

    async def repair_turn(self, research_id, agent_id, role, task_id, turn_id, schema, context, *,
                          reason_code, diagnostic, conversation_id=None, line_id=None,
                          output_validator=None, **message_options):
        frozen = {"agent_id": agent_id, "role": role, "original_task": {"task_id": task_id, "turn_id": turn_id},
            "original_context": context, "diagnostic": diagnostic}
        actions = ["request_revision", "decompose_task", "needs_external_input"]
        if reason_code != "model_output_truncated" and schema.__name__ != "Vote":
            actions.insert(0, "repair_output")
        decision = await self.resolve(research_id=research_id, source_task_id=task_id, scope_kind="turn_output",
            scope_id=f"{agent_id}:{task_id}:{turn_id}", reason_code=reason_code, origin_node="engine.validate_output",
            context=frozen, target_schema=schema, allowed_actions=actions, line_id=line_id,
            recovery_chain_id=message_options.pop("recovery_chain_id", None), output_validator=output_validator)
        request = self.store.get("arbitration_request", decision["arbitration_id"])
        if decision["action"] == "repair_output":
            result = self.engine.validate_output(research_id, agent_id, schema, decision["output"], context, output_validator)
            await self.apply(request["id"], decision)
            accepted = await self.engine.accept_turn_output(research_id, "god/" + request["id"], role,
                task_id, turn_id + ":god:" + request["id"], schema, result, context,
                conversation_id=conversation_id, line_id=line_id, output_validator=output_validator,
                decision_source="god", arbitration_id=request["id"], repaired_for_agent_id=agent_id, **message_options)
            self.store.close_recovery_chain(request["recovery_chain_id"], "fulfilled", result_hash=digest(accepted))
            return accepted
        return await self.engine.recover_action(request, decision, schema=schema, context=context,
            agent_id=agent_id, role=role, task_id=task_id, turn_id=turn_id,
            conversation_id=conversation_id, line_id=line_id, output_validator=output_validator, **message_options)
