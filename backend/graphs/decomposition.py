"""Finite recovery jobs. Parent graphs interrupt; Workers execute their children."""
from __future__ import annotations

import asyncio
import copy
import json

from langgraph.types import interrupt

from backend.adapters.dsh import ResearchStopped, RuntimeBlocked
from backend.graphs.tasks import current_task, TaskSuspended, RecoveryOutcome
from backend.persistence.store import now
from packages.contracts.research import Contract, DecompositionPlan, Evidence, digest
from pydantic import Field


class EvidenceRecoveryOutput(Contract):
    content_markdown: str
    evidence: list[Evidence] = Field(min_length=1)
    limitations: list[str] = Field(default_factory=list)


def validate_fragment(value, schema, root=None):
    """Validate the supported, deliberately bounded JSON Schema subset."""
    root = root or schema
    if "$ref" in schema:
        ref = schema["$ref"]
        if not ref.startswith("#/$defs/"):
            raise ValueError("Only local part-schema references are supported")
        return validate_fragment(value, root["$defs"][ref.split("/")[-1]], root)
    for keyword in ("anyOf", "oneOf"):
        if keyword in schema:
            matches = 0
            for branch in schema[keyword]:
                try:
                    validate_fragment(value, branch, root)
                    matches += 1
                except ValueError:
                    pass
            if matches == 0 or (keyword == "oneOf" and matches != 1):
                raise ValueError("Part output does not match its union contract")
            return value
    kind = schema.get("type")
    checks = {"object": isinstance(value, dict), "array": isinstance(value, list),
        "string": isinstance(value, str), "integer": type(value) is int,
        "number": type(value) in (int, float), "boolean": type(value) is bool, "null": value is None}
    if kind and not checks.get(kind, False):
        raise ValueError(f"Part output must be {kind}")
    if "const" in schema and value != schema["const"] or "enum" in schema and value not in schema["enum"]:
        raise ValueError("Part value is outside allowed choices")
    if isinstance(value, dict):
        if set(schema.get("required", [])) - set(value):
            raise ValueError("Part output is missing required fields")
        for key, child in value.items():
            contract = schema.get("properties", {}).get(key, schema.get("additionalProperties", True))
            if contract is False:
                raise ValueError("Unexpected part output field: " + key)
            if isinstance(contract, dict):
                validate_fragment(child, contract, root)
    if isinstance(value, list):
        if len(value) < schema.get("minItems", 0) or len(value) > schema.get("maxItems", 1000000):
            raise ValueError("Part array length outside contract")
        for child in value:
            validate_fragment(child, schema.get("items", {}), root)
    if isinstance(value, str) and (len(value) < schema.get("minLength", 0) or len(value) > schema.get("maxLength", 100000000)):
        raise ValueError("Part text length outside contract")
    if type(value) in (int, float):
        import math
        if not math.isfinite(value) or value < schema.get("minimum", float("-inf")) or value > schema.get("maximum", float("inf")):
            raise ValueError("Part number outside contract")
    return value


def validate_fragment_schema(schema):
    supported = {"$schema", "$defs", "$ref", "title", "description", "default", "type", "anyOf", "oneOf",
        "const", "enum", "properties", "additionalProperties", "required", "items", "minItems", "maxItems",
        "minLength", "maxLength", "minimum", "maximum"}
    if not isinstance(schema, dict) or set(schema) - supported:
        raise ValueError("Unsupported recovery schema rule: " + ", ".join(sorted(set(schema) - supported)))
    if "$ref" in schema and not schema["$ref"].startswith("#/$defs/"):
        raise ValueError("Recovery schema cannot use external references")
    for group in (schema.get("$defs", {}), schema.get("properties", {})):
        for child in group.values():
            validate_fragment_schema(child)
    for child in schema.get("anyOf", []) + schema.get("oneOf", []):
        validate_fragment_schema(child)
    for key in ("items", "additionalProperties"):
        if isinstance(schema.get(key), dict):
            validate_fragment_schema(schema[key])


def fragment_contract(document):
    validate_fragment_schema(document)
    class Fragment:
        __name__ = "RecoveryFragment"
        @classmethod
        def model_json_schema(cls):
            return document
        @classmethod
        def model_validate(cls, value):
            validate_fragment(value, document)
            return cls(value)
        @classmethod
        def model_validate_json(cls, text):
            return cls.model_validate(json.loads(text))
        def __init__(self, value):
            self.value = value
        def model_dump(self):
            return self.value
    Fragment.__name__ = "RecoveryFragment"
    return Fragment


def pointer_tokens(pointer):
    return [s.replace("~1", "/").replace("~0", "~") for s in pointer[1:].split("/")]


def read_pointer(value, pointer):
    for token in pointer_tokens(pointer):
        if not isinstance(value, dict) or token not in value:
            raise ValueError("Part omitted its owned position: " + pointer)
        value = value[token]
    return value


def write_pointer(value, pointer, content):
    tokens = pointer_tokens(pointer)
    for token in tokens[:-1]:
        if token in value and not isinstance(value[token], dict):
            raise ValueError("Merge path conflicts with existing scalar")
        value = value.setdefault(token, {})
    if tokens[-1] in value and value[tokens[-1]] != content:
        raise ValueError("Merge would overwrite an existing field")
    value[tokens[-1]] = copy.deepcopy(content)


def merge_parts(plan, results):
    plan = DecompositionPlan.model_validate(plan).model_dump()
    output = copy.deepcopy(plan["merge_plan"]["base_output"])
    parts = {p["part_id"]: p for p in plan["parts"]}
    strategy = plan["merge_plan"]["strategy"]
    sections = []
    for identity in plan["merge_plan"]["order"]:
        part, result = parts[identity], results[identity]
        validate_fragment(result, part["output_schema"])
        for pointer in part["allowed_write_paths"]:
            content = read_pointer(result, pointer)
            if strategy in {"markdown_sections", "file_fragments"}:
                if not isinstance(content, str):
                    raise ValueError("Text assembly requires text fragments")
                sections.append(content)
            else:
                write_pointer(output, pointer, content)
    if strategy == "markdown_sections":
        if "content_markdown" in output:
            raise ValueError("Markdown output must be assembled exclusively from verified parts")
        output["content_markdown"] = "\n\n".join(sections)
    elif strategy == "file_fragments":
        path = plan["invariants"].get("file_path")
        if not path or path.startswith(("/", "\\")) or ":" in path or ".." in path.replace("\\", "/").split("/"):
            raise ValueError("File fragments need a safe invariant file_path")
        files = output.setdefault("files", {})
        if path in files:
            raise ValueError("Duplicate assembled file path")
        files[path] = "".join(sections)
    return output


class DecompositionService:
    def __init__(self, engine):
        self.engine, self.store = engine, engine.store

    def validate_plan(self, request, decision, schema, context=None, role=None):
        if schema is None:
            raise ValueError("Decomposition needs an original output contract")
        plan = DecompositionPlan.model_validate(decision["plan"]).model_dump()
        context = context or {}
        role = role or context.get("role")
        original = context.get("original_context", context)
        if plan["target_schema_hash"] != digest(schema.model_json_schema()):
            raise ValueError("Decomposition changed the original output contract")
        if any(p["target_role"] != role for p in plan["parts"]):
            raise ValueError("Recovery children cannot change the original role permissions")
        for part in plan["parts"]:
            validate_fragment_schema(part["output_schema"])
            if len(json.dumps(part["output_schema"])) > 65536:
                raise ValueError("Recovery part schema exceeds the bounded contract size")
            if plan["merge_plan"]["strategy"] == "json_fields":
                allowed = schema.model_json_schema().get("properties", {})
                if any(pointer_tokens(p)[0] not in allowed for p in part["allowed_write_paths"]):
                    raise ValueError("Part owns a field outside the original target contract")
        base = plan["merge_plan"]["base_output"]
        frozen = {k: original[k] for k in ("spec_id", "spec_version", "deadline_at", "spec_hash") if k in original}
        if "spec" in original:
            frozen.update(spec_hash=digest(original["spec"]), actual_config=original["spec"].get("fixed"))
        for key, value in frozen.items():
            target_key = "version" if key == "spec_version" else key
            if target_key in base and base[target_key] != value:
                raise ValueError("Decomposition base changed a program-frozen field: " + target_key)
        if plan["merge_plan"]["strategy"] == "files":
            identities = []
            for part in plan["parts"]:
                for pointer in part["allowed_write_paths"]:
                    tokens = pointer_tokens(pointer)
                    if len(tokens) != 2 or tokens[0] != "files":
                        raise ValueError("File assembly requires individual owned /files/path positions")
                    path = tokens[1].replace("\\", "/")
                    if path.startswith("/") or ":" in path or any(t in {"", ".", ".."} for t in path.split("/")):
                        raise ValueError("File part path must be a safe relative path")
                    identities.append(path.casefold())
            if len(set(identities)) != len(identities):
                raise ValueError("File paths conflict on this Windows workspace")
        return plan

    async def request_evidence(self, request, decision, *, context, schema=None, agent_id=None, role=None,
                               task_id=None, turn_id=None, line_id=None, **options):
        if decision["target_role"] != role:
            raise ValueError("Evidence recovery must preserve its originating role permissions")
        root = current_task.get()
        if not root:
            raise RuntimeBlocked("recovery_task_missing", "取证需要可恢复的 Worker 父任务")
        identity = request["id"] + ":evidence"
        token = digest([root["id"], identity])
        job = self.store.get("recovery_evidence", identity)
        if not job:
            await self.engine.guard(request["research_id"])
            with self.store.connect(True) as c:
                job = self.store._put(c, "recovery_evidence", identity, {"root_task_id": root["id"],
                    "resume_token": token, "arbitration_id": request["id"], "recovery_chain_id": request["recovery_chain_id"],
                    "status": "waiting_children", "decision": decision, "context": context,
                    "agent_id": agent_id, "role": role, "line_id": line_id}, request["research_id"])
                self.store._put(c, "recovery_wait", token, {"root_task_id": root["id"], "resume_token": token,
                    "evidence_job_id": identity, "status": "waiting_children"}, request["research_id"])
                self.store._put(c, "task", identity, {"kind": "recovery_role", "payload": {"evidence_job_id": identity,
                    "line_id": line_id}, "status": "pending", "created_at": now(), "attempt": 0}, request["research_id"])
        # Replay the same effect if creation committed just before a crash.
        if request["status"] != "applied":
            await self.engine.arbitration.apply(request["id"], decision, effect_kind="request_evidence", payload={"job_id": identity, "resume_token": token})
            self.store.patch("arbitration_request", request["id"], resolution_status="waiting_children", applied_action="request_evidence")
            self.store.close_recovery_chain(request["recovery_chain_id"], status="waiting_children")
        # Replays must consume this wait's resume value even after completion;
        # later waits in the same node use the next interrupt index.
        outcome = interrupt({"root_task_id": root["id"], "resume_token": token, "evidence_job_id": identity})
        job = self.store.get("recovery_evidence", identity)
        if outcome.get("resume_token") != token:
            raise RuntimeBlocked("recovery_validation_failed", "取证恢复令牌与等待节点不符")
        if job["status"] == "failed":
            raise RuntimeBlocked("recovery_validation_failed", json.dumps(job.get("errors", []), ensure_ascii=False))
        if job["status"] != "fulfilled":
            raise RuntimeBlocked("recovery_validation_failed", "取证任务尚未通过实际来源验收：" + str(job.get("errors", [])))
        return job["result"]

    async def execute_evidence(self, task):
        job = self.store.get("recovery_evidence", task["payload"]["evidence_job_id"])
        if job["status"] == "fulfilled":
            return job["result"]
        request = self.store.get("arbitration_request", job["arbitration_id"])
        if not request or request["status"] != "applied":
            raise ResearchStopped("取证仲裁动作尚未生效或已经取消")
        decision = job["decision"]
        context = {**job["context"], "instruction": "执行具体取证步骤并引用真实已登记来源。无法取得时明确失败；不得捏造实验。正式实验需要返回正常设计流程。",
            "question": decision["question"], "steps": decision["steps"], "source_refs": decision["source_refs"],
            "result_requirements": decision["result_requirements"], "recovery_part": True}
        def validate(output):
            for evidence in output["evidence"]:
                if evidence["status"] == "unverified" or not self.store.source_exists(task["research_id"], evidence["source_id"]):
                    raise ValueError("Evidence recovery requires actual, registered sources and observed verification status")
        result = await self.engine.turn(task["research_id"], job["agent_id"], job["role"], task["id"], "gather_evidence",
            EvidenceRecoveryOutput, context, line_id=job.get("line_id"), output_validator=validate,
            recovery_chain_id=job["recovery_chain_id"])
        validate(result)
        await self.engine.guard(task["research_id"])
        if self.store.get("recovery_evidence", job["id"])["status"] != "waiting_children":
            raise ResearchStopped("取证范围已取消，晚到结果不恢复父任务")
        self.store.patch("recovery_evidence", job["id"], status="fulfilled", result=result, completed_at=now())
        self.store.patch("arbitration_request", job["arbitration_id"], resolution_status="validating")
        self._wake(job, {"status": "fulfilled", "result_hash": digest(result)})
        return result

    async def suspend(self, request, decision, *, schema, context, agent_id, role, task_id, turn_id, **options):
        plan = self.validate_plan(request, decision, schema, context, role)
        root = current_task.get()
        if not root:
            raise RuntimeBlocked("recovery_task_missing", "拆分需要可恢复的 Worker 父任务，原始输出现场已保留")
        token = digest([root["id"], request["id"], plan])
        plan_id = request["id"] + ":plan"
        stored = self.store.get("decomposition_plan", plan_id)
        if not stored:
            await self.engine.guard(request["research_id"])
            reusable = {}
            previous_plans = self.store.list("decomposition_plan", request["research_id"], recovery_chain_id=request["recovery_chain_id"])
            for previous in previous_plans:
                if (previous["id"] == plan_id or digest(previous.get("context")) != digest(context) or
                        previous["plan"]["invariants"] != plan["invariants"] or
                        previous["plan"]["target_schema_hash"] != plan["target_schema_hash"]):
                    continue
                for child in self.store.list("decomposition_part", request["research_id"], plan_id=previous["id"]):
                    # Dependencies can change the semantic input; only independent,
                    # exactly unchanged parts can be reused without another proof.
                    if child["status"] == "completed" and not child["part"]["depends_on"]:
                        reusable[digest(child["part"])] = child
            with self.store.connect(True) as c:
                stored = self.store._get(c, "decomposition_plan", plan_id)
                if not stored:
                    stored = self.store._put(c, "decomposition_plan", plan_id, {"plan": plan,
                        "arbitration_id": request["id"], "recovery_chain_id": request["recovery_chain_id"],
                        "root_task_id": root["id"], "resume_token": token, "status": "waiting_children",
                        "context": context, "agent_id": agent_id, "role": role, "logical_task_id": task_id,
                        "turn_id": turn_id, "line_id": options.get("line_id"), "target_schema_name": schema.__name__,
                        "created_at": now()}, request["research_id"])
                    self.store._put(c, "recovery_wait", token, {"root_task_id": root["id"], "plan_id": plan_id,
                        "resume_token": token, "status": "waiting_children"}, request["research_id"])
                    for part in plan["parts"]:
                        child_id = plan_id + ":part:" + part["part_id"]
                        reused = reusable.get(digest(part))
                        extra = {"status": "completed", "result": reused["result"], "result_hash": reused["result_hash"],
                            "reused_from": reused["id"], "completed_at": now()} if reused else {}
                        self.store._put(c, "decomposition_part", child_id, {"plan_id": plan_id, "part": part,
                            "status": "pending", "root_task_id": root["id"], "resume_token": token, **extra}, request["research_id"])
                    self.store._emit(c, request["research_id"], "task.decomposed", {"task_id": root["id"],
                        "arbitration_id": request["id"], "recovery_chain_id": request["recovery_chain_id"],
                        "plan_id": plan_id, "part_count": len(plan["parts"])}, line_id=options.get("line_id"))
        if request["status"] != "applied":
            await self.engine.arbitration.apply(request["id"], decision, effect_kind="decompose_task", payload={"plan_id": plan_id, "resume_token": token})
            self.store.patch("arbitration_request", request["id"], resolution_status="waiting_children", applied_action="decompose_task")
            self.store.close_recovery_chain(request["recovery_chain_id"], status="waiting_children")
        await self.dispatch(request["research_id"])
        # Always consume this interrupt on replay, including fulfilled/failed
        # plans, so a subsequent wait cannot consume this plan's resume value.
        # A new wait unwinds all parent/member locks and releases Worker capacity.
        outcome = interrupt({"root_task_id": root["id"], "resume_token": token, "plan_id": plan_id})
        stored = self.store.get("decomposition_plan", plan_id)
        if outcome.get("resume_token") != token:
            raise RuntimeBlocked("recovery_incomplete", "拆分恢复令牌与等待节点不符")
        if stored["status"] == "failed":
            raise RuntimeBlocked("recovery_validation_failed", json.dumps(stored.get("errors", []), ensure_ascii=False))
        if stored["status"] != "fulfilled":
            raise RuntimeBlocked("recovery_incomplete", "子任务尚未通过完整合同验收")
        result = self.engine.validate_recovery_output(request["research_id"], agent_id, schema, stored["result"], context, options.get("output_validator"))
        self.store.close_recovery_chain(request["recovery_chain_id"])
        return result

    async def execute_part(self, task):
        child = self.store.get("decomposition_part", task["payload"]["part_id"])
        plan = self.store.get("decomposition_plan", child["plan_id"])
        if child["status"] == "completed":
            return child["result"]
        if plan["status"] != "waiting_children":
            raise ResearchStopped("旧拆分计划已失效")
        dependencies = {}
        for identity in child["part"]["depends_on"]:
            dependency = self.store.get("decomposition_part", child["plan_id"] + ":part:" + identity)
            if not dependency or dependency["status"] != "completed":
                raise RuntimeBlocked("recovery_dependency_pending", "拆分任务依赖尚未完成")
            dependencies[identity] = dependency["result"]
        part = child["part"]
        contract = fragment_contract(part["output_schema"])
        # Child tasks only generate their owned data; they never launch experiments.
        context = {**plan["context"], "instruction": part["instructions"], "recovery_part": True,
            "owned_output_paths": part["allowed_write_paths"], "dependency_results": dependencies,
            "invariants": plan["plan"]["invariants"]}
        result = await self.engine.turn(task["research_id"], plan["agent_id"], plan["role"], task["id"],
            "part", contract, context, line_id=plan.get("line_id"), recovery_chain_id=plan["recovery_chain_id"])
        validate_fragment(result, part["output_schema"])
        for pointer in part["allowed_write_paths"]:
            read_pointer(result, pointer)
        await self.engine.guard(task["research_id"])
        if self.store.get("decomposition_plan", plan["id"])["status"] != "waiting_children":
            raise ResearchStopped("拆分范围已取消，晚到结果不恢复父任务")
        existing = self.store.get("decomposition_part", child["id"])
        if existing["status"] == "completed" and existing["result_hash"] != digest(result):
            raise ValueError("A recovery child cannot complete with two different results")
        self.store.patch("decomposition_part", child["id"], status="completed", result=result,
            result_hash=digest(result), completed_at=now())
        return result

    async def dispatch(self, research_id=None):
        """Short scans; capacity belongs to the ordinary Worker task scheduler."""
        from packages.contracts import research
        for job in self.store.list("recovery_evidence", research_id):
            if job["status"] == "fulfilled":
                self._wake(job, {"status": "fulfilled", "result_hash": digest(job["result"])})
                continue
            if job["status"] == "failed":
                self._wake(job, {"status": "failed", "errors": job.get("errors", [])})
                continue
            if job["status"] != "waiting_children":
                continue
            project = self.store.project(job["research_id"])
            if project["status"] in {"terminated", "terminating", "completed", "quarantined"}:
                self.store.patch("recovery_evidence", job["id"], status="cancelled")
                continue
            child = self.store.get("task", job["id"])
            if child and child["status"] == "failed":
                self.store.patch("recovery_evidence", job["id"], status="failed", errors=[child.get("error")])
                self._wake(job, {"status": "failed", "errors": [child.get("error")]})
        for plan in self.store.list("decomposition_plan", research_id):
            if plan["status"] == "fulfilled":
                self._wake(plan, {"status": "fulfilled", "result_hash": digest(plan["result"])})
                continue
            if plan["status"] == "failed":
                self._wake(plan, {"status": "failed", "errors": plan.get("errors", [])})
                continue
            if plan["status"] != "waiting_children":
                continue
            request = self.store.get("arbitration_request", plan["arbitration_id"])
            if not request or request["status"] != "applied":
                continue
            rid = plan["research_id"]
            project = self.store.project(rid)
            if project["status"] in {"terminated", "terminating", "completed", "quarantined"}:
                self.store.patch("decomposition_plan", plan["id"], status="cancelled")
                for child in self.store.list("decomposition_part", rid, plan_id=plan["id"]):
                    if child["status"] != "completed":
                        self.store.patch("decomposition_part", child["id"], status="cancelled")
                continue
            if project["status"] in {"paused", "pausing", "blocked"}:
                continue
            if not await asyncio.to_thread(self.engine.control.allows_progress, rid, project["control_generation"]):
                continue
            children = self.store.list("decomposition_part", rid, plan_id=plan["id"])
            by_id = {c["part"]["part_id"]: c for c in children}
            failed = []
            for child in children:
                task = self.store.get("task", child["id"])
                if task and task["status"] == "failed":
                    failed.append({"part_id": child["part"]["part_id"], "error": task.get("error")})
                if child["status"] == "pending" and all(by_id[d]["status"] == "completed" for d in child["part"]["depends_on"]):
                    self.store.add_task(rid, "decomposition_part", {"part_id": child["id"], "line_id": plan.get("line_id")}, child["id"])
            if failed:
                self.store.patch("decomposition_plan", plan["id"], status="failed", errors=failed)
                self._wake(plan, {"status": "failed", "errors": failed})
                continue
            if not all(c["status"] == "completed" for c in children):
                continue
            try:
                result = merge_parts(plan["plan"], {identity: c["result"] for identity, c in by_id.items()})
                schema = getattr(research, plan["target_schema_name"])
                result = self.engine.validate_recovery_output(rid, plan["agent_id"], schema, result, plan["context"])
            except (ValueError, AttributeError) as error:
                self.store.patch("decomposition_plan", plan["id"], status="failed", errors=[str(error)])
                self._wake(plan, {"status": "failed", "errors": [str(error)]})
                continue
            with self.store.connect(True) as c:
                self.store._put(c, "decomposition_plan", plan["id"], {**plan, "status": "fulfilled", "result": result,
                    "result_hash": digest(result), "completed_at": now()}, rid)
                self.store._emit(c, rid, "task.reassembled", {"task_id": plan["root_task_id"], "plan_id": plan["id"],
                    "arbitration_id": plan["arbitration_id"], "recovery_chain_id": plan["recovery_chain_id"],
                    "result_hash": digest(result)}, line_id=plan.get("line_id"))
            self._wake(plan, {"status": "fulfilled", "result_hash": digest(result)})
        self.drain_wakeups()

    def _wake(self, plan, outcome):
        identity = plan["root_task_id"] + ":" + plan["resume_token"]
        if not self.store.get("recovery_resume_outbox", identity):
            self.store.put("recovery_resume_outbox", identity, {"root_task_id": plan["root_task_id"],
                "resume_token": plan["resume_token"], "outcome": outcome, "status": "pending", "line_id": plan.get("line_id"),
                "arbitration_id": plan.get("arbitration_id"), "recovery_chain_id": plan.get("recovery_chain_id")}, plan["research_id"])

    def drain_wakeups(self):
        for outbox in self.store.list("recovery_resume_outbox", status="pending"):
            with self.store.connect(True) as c:
                task = self.store._get(c, "task", outbox["root_task_id"])
                project = self.store._project(c, outbox["research_id"])
                if project["status"] in {"paused", "pausing", "blocked"}:
                    continue
                line = self.store._get(c, "line", f"{outbox['research_id']}:{outbox['line_id']}") if outbox.get("line_id") else None
                if line and line.get("finish_requested"):
                    self.store._put(c, "recovery_resume_outbox", outbox["id"], {**outbox, "status": "cancelled"}, outbox["research_id"])
                    continue
                if project["status"] in {"terminated", "terminating", "completed", "quarantined"}:
                    self.store._put(c, "recovery_resume_outbox", outbox["id"], {**outbox, "status": "cancelled"}, outbox["research_id"])
                elif task and task["status"] == "waiting_recovery" and task.get("resume_token") == outbox["resume_token"]:
                    self.store._put(c, "task", task["id"], {**task, "status": "pending", "owner": None,
                        "recovery_outcome": {**outbox["outcome"], "resume_token": outbox["resume_token"]}}, outbox["research_id"])
                    self.store._put(c, "recovery_resume_outbox", outbox["id"], {**outbox, "status": "delivered"}, outbox["research_id"])
                    self.store._emit(c, outbox["research_id"], "task.resumed", {"task_id": task["id"],
                        "resume_token": outbox["resume_token"], "arbitration_id": outbox.get("arbitration_id"),
                        "recovery_chain_id": outbox.get("recovery_chain_id")}, line_id=outbox.get("line_id"))
