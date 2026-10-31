"""Transactional recovery records. No model, process, or scientific work runs here."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from packages.contracts.research import ArbitrationRequest, GodDecision, digest


def _now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _time(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _error(code, message):
    from backend.persistence.store import StoreError
    return StoreError(code, message)


FINAL = {"applied", "cancelled", "stale", "needs_external_input", "exhausted"}
TRANSITIONS = {
    "requested": {"running", "cancelled", "stale", "needs_external_input", "exhausted"},
    "running": {"running", "decided", "retry_wait", "cancelled", "stale", "needs_external_input", "exhausted"},
    "retry_wait": {"running", "cancelled", "stale", "needs_external_input", "exhausted"},
    "decided": {"applying", "applied", "cancelled", "stale", "needs_external_input", "exhausted"},
    "applying": {"applied", "cancelled", "stale", "needs_external_input", "exhausted"},
}


class ArbitrationStoreMixin:
    def _recovered_arbitration_response(self, request):
        """Only a hash-bound, on-time response from the current generation is reusable."""
        marker = request.get("recovered_response")
        if not isinstance(marker, dict) or not isinstance(marker.get("output"), dict):
            return None
        try:
            if (marker.get("output_hash") != digest(marker["output"])
                    or not marker.get("source_execution_id")
                    or marker.get("attempt") != request.get("generation_attempts", request.get("attempt"))
                    or _time(marker["completed_at"]) > _time(request["judge_deadline_at"])):
                return None
            if (marker["output"].get("arbitration_id") != request["arbitration_id"]
                    or marker["output"].get("snapshot_hash") != request["snapshot_hash"]):
                return None
        except (KeyError, TypeError, ValueError):
            return None
        return marker

    def _arbitration(self, c, arbitration_id):
        request = self._get(c, "arbitration_request", arbitration_id)
        if not request:
            raise _error("arbitration_not_found", "仲裁请求不存在")
        return request

    def _arbitration_event(self, c, request, status, **changes):
        value = {**request, **changes, "status": status, "updated_at": _now()}
        event_name = "started" if status == "running" else status
        payload = {k: value.get(k) for k in ("arbitration_id", "source_task_id", "scope_kind", "scope_id",
            "reason_code", "snapshot_id", "snapshot_hash", "source_event_seq", "decision_id", "decision_hash",
            "effect_id", "attempt", "lease_epoch", "retry_not_before", "reason") if value.get(k) is not None}
        diagnostic = changes.get("diagnostic", changes.get("reason"))
        if diagnostic:
            payload["reason"] = diagnostic
        event = self._emit(c, value["research_id"], "arbitration." + event_name, payload,
            line_id=value.get("line_id"), conversation_id=f"{value['research_id']}:god:{value['arbitration_id']}")
        value[status + "_event_seq"] = event["seq"]
        value["history"] = [*request.get("history", []), {"status": status,
            "occurred_at": event["occurred_at"], "event_seq": event["seq"], "reason": diagnostic}]
        if value.get("attempts") and status in {"retry_wait", "decided", "cancelled", "stale", "exhausted"}:
            value["attempts"] = [dict(attempt) for attempt in value["attempts"]]
            value["attempts"][-1].update(finished_at=event["occurred_at"], status=status,
                diagnostic=changes.get("diagnostic", changes.get("reason")), retry_not_before=value.get("retry_not_before"))
        validation = {"decided": "accepted", "applied": "applied", "retry_wait": "rejected", "exhausted": "rejected"}.get(status)
        if validation:
            self._annotate_god_messages(c, value, validation, diagnostic)
        return self._put(c, "arbitration_request", value["arbitration_id"], value, value["research_id"])

    def _annotate_god_messages(self, c, request, validation, diagnostic):
        """Generation completion is not decision acceptance; keep cursor history."""
        import json
        from backend.persistence.store import canonical
        aid, rid, attempt = request["arbitration_id"], request["research_id"], request.get("attempt", 1)
        execution = self._get(c, "execution", digest([rid, "god/" + aid, aid + ":judge", f"judge:{attempt}"])) or {}
        execution_id = execution.get("execution_attempt_id", "")
        rows = c.execute("SELECT data FROM messages WHERE research_id=? AND conversation_id=? "
            "AND (source_event_id=? OR json_extract(data,'$.execution_id')=?)",
            (rid, f"{rid}:god:{aid}", f"{aid}:judge:{attempt}", execution_id)).fetchall()
        for row in rows:
            message = json.loads(row[0])
            message.update(validation_status=validation, validation_diagnostic=diagnostic,
                arbitration_id=aid, arbitration_attempt=attempt)
            event = self._stream_event(c, "message.completed", message,
                {"message_id": message["message_id"], "message": message, "replace": True})
            message["latest_event_seq"] = event["seq"]
            c.execute("UPDATE messages SET data=? WHERE message_id=?", (canonical(message), message["message_id"]))
            event["payload"]["message"] = message
            c.execute("UPDATE events SET data=? WHERE research_id=? AND seq=?", (canonical(event), rid, event["seq"]))

    def _assert_arbitration_scope(self, c, request):
        """Scope checks share the effect transaction, closing check/apply races."""
        scope = self._get(c, "arbitration_scope", digest([request["research_id"], request["scope_kind"], request["scope_id"]]))
        if scope and scope.get("scope_revision") != request["scope_revision"]:
            raise _error("arbitration_stale", "仲裁对应的业务范围已经修订")
        if request.get("line_id"):
            line = self._get(c, "line", f"{request['research_id']}:{request['line_id']}")
            if not line or line.get("finish_requested"):
                raise _error("arbitration_scope_closed", "仲裁对应的研究方向已经结束")
            if line.get("direction_epoch") != request.get("direction_epoch"):
                raise _error("arbitration_stale", "仲裁对应的研究方向已经改变")

    def create_arbitration(self, request, snapshot):
        """Freeze one scope and atomically deduplicate independently captured failures."""
        supplied = dict(request)
        contract = ArbitrationRequest.model_validate({k: supplied[k] for k in ArbitrationRequest.model_fields if k in supplied}).model_dump()
        sections = snapshot.get("sections", snapshot)
        snapshot_hash = digest(sections)
        if contract["snapshot_hash"] != snapshot_hash or snapshot.get("snapshot_hash", snapshot_hash) != snapshot_hash:
            raise _error("arbitration_snapshot_mismatch", "仲裁快照与内容哈希不符")
        semantic_key = digest({k: contract[k] for k in ("research_id", "source_task_id", "scope_kind", "scope_id",
            "scope_revision", "reason_code", "failure_fingerprint", "snapshot_hash", "policy_version")})
        with self.connect(True) as c:
            existing = c.execute("SELECT id FROM documents WHERE kind='arbitration_request' AND json_extract(data,'$.request_key')=?",
                                 (semantic_key,)).fetchone()
            if existing:
                return self._arbitration(c, existing["id"])
            project = self._project(c, contract["research_id"])
            self.assert_progress(project, contract["control_generation"])
            if project["status"] in {"paused", "pausing", "blocked"}:
                raise _error("research_paused", "研究已暂停，暂不发起新仲裁")
            identity = contract["arbitration_id"]
            if self._get(c, "arbitration_request", identity):
                raise _error("idempotency_conflict", "仲裁身份已用于另一现场")
            frozen = {"sections": sections, "snapshot_hash": snapshot_hash,
                      "arbitration_id": identity, "created_at": contract["created_at"]}
            self._put(c, "arbitration_snapshot", contract["snapshot_id"], frozen, contract["research_id"])
            chain_id = contract["recovery_chain_id"]
            chain = self._get(c, "recovery_chain", chain_id)
            if chain is None:
                chain = self._put(c, "recovery_chain", chain_id, {"source_task_id": contract["source_task_id"],
                    "scope_id": contract["scope_id"], "resume_point": contract["resume_point"],
                    "rounds": 0, "max_rounds": 3, "status": "recovering", "request_ids": [], "created_at": _now()}, contract["research_id"])
            if chain["research_id"] != contract["research_id"] or chain.get("status") in {"fulfilled", "cancelled"}:
                raise _error("recovery_chain_closed", "不能向已结束或其他研究的恢复链添加请求")
            if chain["rounds"] >= chain.get("max_rounds", 3):
                raise _error("arbitration_exhausted", "同一恢复链的三轮自动仲裁已耗尽")
            self._put(c, "recovery_chain", chain_id, {**chain, "rounds": chain["rounds"] + 1,
                "request_ids": [*chain.get("request_ids", []), identity], "status": "recovering"}, contract["research_id"])
            value = {**supplied, **contract, "request_key": semantic_key, "status": "requested", "owner": None,
                     "lease_epoch": 0, "lease_expires_at": None, "attempt": 0, "generation_attempts": 0,
                     "attempts": [], "history": []}
            return self._arbitration_event(c, value, "requested")

    def claim_arbitration(self, arbitration_id, owner, lease_seconds=600, processes_reconciled=False):
        with self.connect(True) as c:
            request = self._arbitration(c, arbitration_id)
            if request["status"] in FINAL | {"decided", "applying"}:
                return None
            project = self._project(c, request["research_id"])
            try:
                self.assert_progress(project, request["control_generation"])
            except Exception as exc:
                self._arbitration_event(c, request, "cancelled", reason=str(exc))
                return None
            if project["status"] in {"paused", "pausing", "blocked"}:
                return None
            current = datetime.now(timezone.utc)
            recovered = self._recovered_arbitration_response(request)
            if current >= _time(request["judge_deadline_at"]) and not recovered:
                self._arbitration_event(c, request, "exhausted", reason="God 判决时限已到期")
                return None
            if request.get("retry_not_before") and current < _time(request["retry_not_before"]):
                return None
            if request["status"] == "running":
                # A timed-out lease is not proof that the previous model process exited.
                if not processes_reconciled or (request.get("lease_expires_at") and current < _time(request["lease_expires_at"])):
                    return None
            if request.get("generation_attempts", request.get("attempt", 0)) >= 3 and not recovered:
                self._arbitration_event(c, request, "exhausted", reason="God 输出修复次数已耗尽")
                return None
            epoch = request.get("lease_epoch", 0) + 1
            attempt = request.get("attempt", 0) if recovered else request.get("attempt", 0) + 1
            lease_boundary = request["expires_at"] if recovered else request["judge_deadline_at"]
            return self._arbitration_event(c, request, "running", owner=owner, lease_epoch=epoch,
                retry_not_before=None,
                recovered_response=recovered,
                lease_expires_at=min(current + timedelta(seconds=lease_seconds), _time(lease_boundary)).isoformat().replace("+00:00", "Z"),
                attempt=attempt, generation_attempts=request.get("generation_attempts", attempt) if recovered else attempt,
                attempts=[*request.get("attempts", []), {"attempt": attempt, "lease_epoch": epoch, "owner": owner,
                    "started_at": _now(), "agent_id": f"god/{arbitration_id}", "recovered_response": bool(recovered)}])

    def _check_arbitration_owner(self, request, owner, lease_epoch):
        if owner is not None and request.get("owner") != owner:
            raise _error("arbitration_lease_lost", "仲裁请求已由其他 Worker 认领")
        if lease_epoch is not None and request.get("lease_epoch") != lease_epoch:
            raise _error("arbitration_lease_lost", "旧执行代次不能提交仲裁响应")

    def transition_arbitration(self, arbitration_id, status, *, owner=None, lease_epoch=None, **changes):
        with self.connect(True) as c:
            request = self._arbitration(c, arbitration_id)
            self._check_arbitration_owner(request, owner, lease_epoch)
            forbidden = set(changes).intersection(ArbitrationRequest.model_fields)
            if forbidden:
                raise _error("immutable_arbitration_request", "仲裁冻结身份和合同不可修改")
            if status == request["status"]:
                if status in FINAL:
                    return request
                return self._put(c, "arbitration_request", arbitration_id, {**request, **changes}, request["research_id"])
            if status not in TRANSITIONS.get(request["status"], set()):
                raise _error("invalid_arbitration_transition", f"非法仲裁状态变化：{request['status']} → {status}")
            return self._arbitration_event(c, request, status, **changes)

    def decide_arbitration(self, arbitration_id, decision, *, owner=None, lease_epoch=None, **metadata):
        checked = GodDecision.model_validate(decision).model_dump()
        decision_hash = digest(checked)
        with self.connect(True) as c:
            request = self._arbitration(c, arbitration_id)
            self._check_arbitration_owner(request, owner, lease_epoch)
            if request.get("decision_hash"):
                if request["decision_hash"] != decision_hash:
                    raise _error("idempotency_conflict", "同一仲裁不能接受不同的有效决策")
                return request
            if request["status"] != "running":
                raise _error("invalid_arbitration_transition", "仅运行中的请求可以保存判决")
            if checked["arbitration_id"] != arbitration_id or checked["snapshot_hash"] != request["snapshot_hash"]:
                raise _error("arbitration_snapshot_mismatch", "God 响应不属于本次冻结现场")
            if checked["action"] not in request["allowed_actions"]:
                raise _error("arbitration_action_forbidden", "本次仲裁不允许该动作")
            project = self._project(c, request["research_id"])
            try:
                self.assert_progress(project, request["control_generation"])
                self._assert_arbitration_scope(c, request)
            except Exception as exc:
                status = "stale" if getattr(exc, "code", None) == "arbitration_stale" else "cancelled"
                return self._arbitration_event(c, request, status, reason=str(exc), late_response=checked)
            recovered = self._recovered_arbitration_response(request)
            try:
                recovered_matches = recovered and digest(GodDecision.model_validate(recovered["output"]).model_dump()) == decision_hash
            except ValueError:
                recovered_matches = False
            if datetime.now(timezone.utc) >= _time(request["judge_deadline_at"]) and not recovered_matches:
                return self._arbitration_event(c, request, "exhausted", reason="God 判决到达时已过期", late_response=checked)
            decision_id = arbitration_id + ":decision"
            self._put(c, "arbitration_decision", decision_id, {"arbitration_id": arbitration_id,
                "decision": checked, "decision_hash": decision_hash, "created_at": _now(),
                "lease_epoch": request["lease_epoch"], "validation": {"valid": True}, **metadata}, request["research_id"])
            return self._arbitration_event(c, request, "decided", decision_id=decision_id, decision_hash=decision_hash)

    def apply_arbitration_effect(self, arbitration_id, effect_kind, payload, *, expected_scope_revision=None):
        """Commit only a resume marker; existing domain commits own all scientific effects."""
        with self.connect(True) as c:
            request = self._arbitration(c, arbitration_id)
            key = digest([arbitration_id, request.get("decision_hash"), effect_kind])
            existing = self._get(c, "arbitration_effect", key)
            if existing:
                if digest(existing["payload"]) != digest(payload):
                    raise _error("idempotency_conflict", "同一仲裁恢复标记不能应用不同载荷")
                return existing
            if request["status"] not in {"decided", "applying"}:
                raise _error("arbitration_not_decided", "仲裁尚未取得有效判决")
            project = self._project(c, request["research_id"])
            self.assert_progress(project, request["control_generation"])
            if project["status"] in {"paused", "pausing", "blocked"}:
                raise _error("research_paused", "研究已暂停，判决等待恢复后应用")
            self._assert_arbitration_scope(c, request)
            if datetime.now(timezone.utc) >= _time(request["expires_at"]):
                raise _error("arbitration_expired", "仲裁业务有效期已过")
            if expected_scope_revision is not None and request["scope_revision"] != expected_scope_revision:
                raise _error("arbitration_stale", "仲裁对应的业务范围已经修订")
            self._arbitration_event(c, request, "applying")
            effect = self._put(c, "arbitration_effect", key, {"arbitration_id": arbitration_id,
                "decision_hash": request["decision_hash"], "effect_kind": effect_kind,
                "payload": payload, "applied_at": _now(), "resume_point": request["resume_point"],
                "scope_revision": request["scope_revision"]}, request["research_id"])
            self._arbitration_event(c, self._arbitration(c, arbitration_id), "applied", effect_id=key)
            return effect

    def create_recovery_chain(self, research_id, chain_id, source_task_id, scope_id, resume_point):
        with self.connect(True) as c:
            existing = self._get(c, "recovery_chain", chain_id)
            if existing:
                if existing["research_id"] != research_id:
                    raise _error("cross_project_reference", "恢复链不属于当前研究")
                return existing
            return self._put(c, "recovery_chain", chain_id, {"source_task_id": source_task_id,
                "scope_id": scope_id, "resume_point": resume_point, "rounds": 0, "max_rounds": 3,
                "status": "recovering", "request_ids": [], "created_at": _now()}, research_id)

    def close_recovery_chain(self, chain_id, status="fulfilled", **changes):
        if status not in {"fulfilled", "failed", "cancelled", "waiting_children", "validating", "recovering"}:
            raise _error("invalid_recovery_status", "恢复链状态不合法")
        return self.patch("recovery_chain", chain_id, status=status, **changes)

    def suspend_recovery_task(self, task_id, resume_token, *, arbitration_id, graph_thread_id,
                              waiting_node, recovery_chain_id):
        with self.connect(True) as c:
            task = self._get(c, "task", task_id)
            if not task:
                raise _error("task_not_found", "父任务不存在")
            self.assert_progress(self._project(c, task["research_id"]), task.get("control_generation"))
            if task["status"] == "waiting_recovery":
                if task.get("resume_token") != resume_token:
                    raise _error("recovery_token_conflict", "父任务已经在另一个恢复点等待")
                return task
            if task["status"] != "running":
                raise _error("invalid_task_transition", "仅正在执行的父任务可以挂起")
            result = self._put(c, "task", task_id, {**task, "status": "waiting_recovery",
                "execution_phase": "waiting_subtasks", "resume_token": resume_token,
                "arbitration_id": arbitration_id, "graph_thread_id": graph_thread_id,
                "waiting_node": waiting_node, "recovery_chain_id": recovery_chain_id}, task["research_id"])
            self._emit(c, task["research_id"], "task.waiting_recovery", {"task_id": task_id,
                "arbitration_id": arbitration_id, "resume_token": resume_token})
            return result

    def enqueue_recovery_resume(self, root_task_id, resume_token, outcome):
        """Domain validators call this only after all required children pass acceptance."""
        with self.connect(True) as c:
            task = self._get(c, "task", root_task_id)
            if not task:
                raise _error("task_not_found", "父任务不存在")
            self.assert_progress(self._project(c, task["research_id"]), task.get("control_generation"))
            if task.get("resume_token") != resume_token or task["status"] != "waiting_recovery":
                raise _error("recovery_token_conflict", "等待节点或恢复令牌已失效")
            key = digest([root_task_id, resume_token])
            existing = self._get(c, "recovery_outbox", key)
            if existing:
                if digest(existing["outcome"]) != digest(outcome):
                    raise _error("idempotency_conflict", "同一恢复令牌不能提交不同结果")
                return existing
            return self._put(c, "recovery_outbox", key, {"root_task_id": root_task_id,
                "resume_token": resume_token, "outcome": outcome, "status": "pending", "created_at": _now()}, task["research_id"])

    def dispatch_recovery_outbox(self, research_id=None):
        result = []
        for item in self.list("recovery_outbox", research_id, status="pending"):
            with self.connect(True) as c:
                outbox = self._get(c, "recovery_outbox", item["id"])
                if outbox["status"] != "pending":
                    continue
                task = self._get(c, "task", outbox["root_task_id"])
                project = self._project(c, item["research_id"])
                try:
                    self.assert_progress(project, task.get("control_generation"))
                except Exception:
                    self._put(c, "recovery_outbox", item["id"], {**outbox, "status": "cancelled"}, item["research_id"])
                    continue
                if project["status"] in {"paused", "pausing"}:
                    continue
                if task["status"] != "waiting_recovery" or task.get("resume_token") != outbox["resume_token"]:
                    self._put(c, "recovery_outbox", item["id"], {**outbox, "status": "stale"}, item["research_id"])
                    continue
                self._put(c, "task", task["id"], {**task, "status": "pending", "recovery_outcome": outbox["outcome"],
                    "execution_phase": "resuming"}, item["research_id"])
                self._put(c, "recovery_outbox", item["id"], {**outbox, "status": "delivered", "delivered_at": _now()}, item["research_id"])
                self._emit(c, item["research_id"], "task.recovery_ready", {"task_id": task["id"], "resume_token": outbox["resume_token"]})
                result.append(task["id"])
        return result

    def _cancel_recovery(self, c, research_id, line_id=None, reason="scope_closed"):
        """Cancel waits and pending recovery work in the same transaction as scope closure."""
        import json
        kinds = ("arbitration_request", "recovery_chain", "recovery_child", "recovery_wait", "dependency_wait", "decomposition_plan",
                 "decomposition_part", "recovery_evidence", "recovery_resume_outbox", "recovery_outbox", "task")
        rows = [dict(row) for row in c.execute("SELECT kind,id,data FROM documents WHERE research_id=?", (research_id,))
                if row["kind"] in kinds]
        for row in rows:
            row["value"] = json.loads(row["data"])
        matching = lambda value: line_id is None or value.get("line_id", value.get("payload", {}).get("line_id")) == line_id
        parents = {row["id"] for row in rows if row["kind"] == "task" and matching(row["value"])}
        requests = {row["id"] for row in rows if row["kind"] == "arbitration_request" and matching(row["value"])}
        chains = {row["value"].get("recovery_chain_id") for row in rows if row["id"] in requests}
        chains.discard(None)
        plans = {row["id"] for row in rows if row["kind"] in {"decomposition_plan", "recovery_evidence", "dependency_wait"}
                 and (matching(row["value"]) or row["value"].get("arbitration_id") in requests)}
        parents.update(row["value"].get("root_task_id") for row in rows if row["id"] in plans)
        parents.discard(None)
        active = {"pending", "running", "requested", "retry_wait", "decided", "applying", "waiting_recovery",
                  "waiting_children", "validating", "recovering", "resuming"}
        for row in rows:
            value, kind = row["value"], row["kind"]
            related = (line_id is None or matching(value) or row["id"] in requests | plans | parents | chains
                       or value.get("root_task_id") in parents or value.get("plan_id") in plans
                       or value.get("arbitration_id") in requests or value.get("recovery_chain_id") in chains)
            if not related or value.get("status") not in active:
                continue
            # Existing experiments still need monitoring; only recovery tasks/waits are cancelled on direction finish.
            if kind == "task" and value.get("status") != "waiting_recovery" and value.get("kind") not in {"decomposition_part", "recovery_role"} and value.get("payload", {}).get("dependency_wait_id") not in plans:
                continue
            if kind == "arbitration_request":
                self._arbitration_event(c, value, "cancelled", reason=reason)
            else:
                self._put(c, kind, row["id"], {**value, "status": "cancelled", "cancelled_at": _now(), "reason": reason}, research_id)
                if kind == "dependency_wait":
                    self._emit(c, research_id, "dependency.cancelled", {"arbitration_id": value["arbitration_id"],
                        "task_id": value["root_task_id"], "reason": reason}, line_id=value["line_id"])

    def _apply_confirmed_arbitration_finish(self, c, research_id, decision_task_id, candidate, receipt,
                                          operation_id, dispatch_allowed):
        """Join the existing confirmed Boss transaction; never grant new authority."""
        if not decision_task_id.endswith(":finish"):
            return None
        arbitration_id = decision_task_id.removesuffix(":finish")
        request = self._get(c, "arbitration_request", arbitration_id)
        if request is None:
            return None
        if request["research_id"] != research_id:
            raise _error("cross_project_reference", "收束仲裁不属于当前研究")
        record = self._get(c, "arbitration_decision", request.get("decision_id", ""))
        if not record or record.get("decision_hash") != request.get("decision_hash"):
            raise _error("arbitration_not_decided", "收束需要已验证且持久保存的 God 判决")
        judgment = GodDecision.model_validate(record["decision"]).model_dump()
        if (judgment["action"] != "finish_scope" or digest(judgment) != request["decision_hash"]
                or judgment["arbitration_id"] != arbitration_id or judgment["snapshot_hash"] != request["snapshot_hash"]
                or "finish_scope" not in request["allowed_actions"]):
            raise _error("arbitration_action_forbidden", "Boss 收束操作与冻结的 God 判决不匹配")
        target = judgment.get("line_id")
        if candidate["action"] != ("finish_line" if target else "finish_project"):
            raise _error("arbitration_action_forbidden", "收束操作不能扩大 God 判决的范围")
        if target and candidate["ending"] != [target]:
            raise _error("arbitration_action_forbidden", "收束操作包含未经 God 判决授权的研究线")
        if request.get("line_id") and request["line_id"] != target:
            raise _error("arbitration_action_forbidden", "单线仲裁不能结束其他方向或项目")
        if not target:
            snapshot = self._get(c, "arbitration_snapshot", request["snapshot_id"])
            if request["scope_kind"] not in {"discussion", "boss_assignment"} or (snapshot or {}).get("sections", {}).get("role") != "boss":
                raise _error("arbitration_action_forbidden", "只有项目级 Boss 仲裁可以结束项目")
        key = digest([arbitration_id, request["decision_hash"], "finish_scope"])
        existing = self._get(c, "arbitration_effect", key)
        if existing:
            return existing
        if not dispatch_allowed:
            if request["status"] not in FINAL:
                self._arbitration_event(c, request, "cancelled", reason="confirmed_finish_archived_after_stop")
            return None
        if request["status"] not in {"decided", "applying"}:
            raise _error("arbitration_not_decided", "收束仲裁尚未取得可应用判决")
        self._assert_arbitration_scope(c, request)
        if request["control_generation"] != candidate["generation"]:
            raise _error("stale_generation", "God 收束与 Boss 控制确认的代次不匹配")
        self._arbitration_event(c, request, "applying")
        effect = self._put(c, "arbitration_effect", key, {"arbitration_id": arbitration_id,
            "decision_hash": request["decision_hash"], "effect_kind": "finish_scope",
            "payload": {"decision_task_id": decision_task_id, "boss_effects_operation_id": operation_id,
                        "finish_line_ids": candidate["ending"]}, "control_receipt": receipt,
            "applied_at": _now(), "resume_point": request["resume_point"],
            "scope_revision": request["scope_revision"]}, research_id)
        self._arbitration_event(c, self._arbitration(c, arbitration_id), "applied", effect_id=key)
        chain = self._get(c, "recovery_chain", request["recovery_chain_id"])
        if chain and chain["status"] != "cancelled":
            self._put(c, "recovery_chain", chain["id"], {**chain, "status": "fulfilled",
                "fulfilled_at": _now(), "result_hash": request["decision_hash"]}, research_id)
        return effect
