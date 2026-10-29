"""Cross-line dependencies wait for Boss-authorized shared results, not a user fix."""
import asyncio
import re

from langgraph.types import interrupt

from backend.adapters.dsh import ResearchStopped, RuntimeBlocked
from backend.graphs.tasks import current_task
from backend.persistence.store import now
from packages.contracts.research import digest


class DependencyService:
    def __init__(self, engine):
        self.engine, self.store = engine, engine.store

    def line_ids(self, request, decision):
        if request.get("scope_kind") != "discussion" or not request.get("line_id"):
            return []
        known = {line["line_id"] for line in self.store.lines(request["research_id"])}
        explicit = decision.get("dependency_line_ids", [])
        if explicit and (len(set(explicit)) != len(explicit) or set(explicit) - known or request["line_id"] in explicit):
            raise ValueError("Dependencies must identify other existing research lines")
        if explicit:
            return sorted(explicit)
        # Compatibility for already-frozen decisions: only exact registered line
        # IDs in the missing-object fields count, never arbitrary target_role text.
        if decision.get("action") == "needs_external_input":
            text = "\n".join([*decision.get("missing_items", []), decision.get("why_unavailable", "")])
            mentioned = set(re.findall(r"(?<![\w-])line-\d+(?![\w-])", text))
            return sorted((mentioned & known) - {request["line_id"]})
        return []

    def can_route(self, request, decision):
        return bool(self.line_ids(request, decision))

    async def wait(self, request, decision):
        rid, identity = request["research_id"], request["id"] + ":dependencies"
        root = current_task.get()
        if not root:
            raise RuntimeBlocked("recovery_task_missing", "内部依赖需要可恢复的 Worker 父任务")
        upstream_ids = self.line_ids(request, decision)
        if not upstream_ids:
            raise ValueError("No registered internal dependency")
        token = digest([root["id"], identity])
        job = self.store.get("dependency_wait", identity)
        if not job:
            await self.engine.guard(rid)
            source = self.store.get("line", f"{rid}:{request['line_id']}")
            upstreams = [self.store.get("line", f"{rid}:{line_id}") for line_id in upstream_ids]
            with self.store.connect(True) as c:
                job = self.store._get(c, "dependency_wait", identity)
                if not job:
                    job = self.store._put(c, "dependency_wait", identity, {"status": "waiting_children",
                        "root_task_id": root["id"], "line_id": request["line_id"], "source_cycle_id": source.get("cycle_id"),
                        "direction_epoch": source["direction_epoch"], "control_generation": request["control_generation"],
                        "resume_token": token, "arbitration_id": request["id"], "recovery_chain_id": request["recovery_chain_id"],
                        "upstreams": [{"line_id": line["line_id"], "cycle_id": line.get("cycle_id"), "direction_epoch": line["direction_epoch"]} for line in upstreams],
                        "requirements": decision.get("missing_items", decision.get("result_requirements", [])),
                        "coordination_ids": [], "created_at": now()}, rid)
                    self.store._put(c, "recovery_wait", token, {"root_task_id": root["id"], "resume_token": token,
                        "dependency_wait_id": identity, "status": "waiting_children"}, rid)
                    self.store._emit(c, rid, "dependency.waiting", {"task_id": root["id"], "line_id": request["line_id"],
                        "arbitration_id": request["id"], "dependency_line_ids": upstream_ids}, line_id=request["line_id"])
        # Legacy needs_external_input is historical fact. Program routing gets its
        # own event; never rewrite that old God judgment into a newly applied one.
        if request["status"] in {"decided", "applying"}:
            await self.engine.arbitration.apply(request["id"], decision, effect_kind="internal_dependency",
                payload={"dependency_wait_id": identity, "resume_token": token})
        if request["status"] not in {"cancelled", "stale", "exhausted"}:
            self.store.patch("arbitration_request", request["id"], resolution_status="waiting_dependencies")
        await self.dispatch(rid)
        outcome = interrupt({"root_task_id": root["id"], "resume_token": token, "dependency_wait_id": identity})
        job = self.store.get("dependency_wait", identity)
        if job["status"] != "fulfilled" or outcome.get("resume_token") != token:
            raise RuntimeBlocked("dependency_coordination_failed", "内部依赖尚未通过 Boss 协调和共享结果验收")
        return job["result"]

    def _published(self, job, boss_id=None):
        # Only Boss-committed memory crosses a line boundary. A completed child,
        # unverified claim, or an artifact in another line is not permission.
        filters = {"boss_decision_id": boss_id} if boss_id else {}
        return [node for node in self.store.list("memory_node", job["research_id"], **filters)
            if node.get("receipt") and (node.get("source_ids") or node.get("source_refs")) and
            all(self.store.source_exists(job["research_id"], source) for source in (node.get("source_ids") or node.get("source_refs") or []))]

    async def dispatch(self, research_id=None):
        for job in self.store.list("dependency_wait", research_id):
            rid = job["research_id"]
            project = self.store.project(rid)
            source = self.store.get("line", f"{rid}:{job['line_id']}")
            if job["status"] == "cancelled":
                continue
            if (project["status"] in {"terminated", "terminating", "completed", "quarantined"} or
                    project["control_generation"] != job["control_generation"] or not source or source.get("finish_requested") or
                    source["direction_epoch"] != job["direction_epoch"] or source.get("cycle_id") != job.get("source_cycle_id")):
                with self.store.connect(True) as c:
                    if job["status"] != "fulfilled":
                        self.store._put(c, "dependency_wait", job["id"], {**job, "status": "cancelled"}, rid)
                        self.store._emit(c, rid, "dependency.cancelled", {"arbitration_id": job["arbitration_id"],
                            "task_id": job["root_task_id"], "reason": "scope_closed"}, line_id=job["line_id"])
                    for task_id in [job["root_task_id"], *job["coordination_ids"]]:
                        task = self.store._get(c, "task", task_id)
                        if task and task["status"] in {"pending", "waiting_recovery", "retry_wait"}:
                            self.store._put(c, "task", task_id, {**task, "status": "cancelled"}, rid)
                continue
            if project["status"] in {"paused", "pausing"}:
                continue
            if not await asyncio.to_thread(self.engine.control.allows_progress, rid, project["control_generation"]):
                continue
            if job["status"] == "fulfilled":
                parent = self.store.get("task", job["root_task_id"])
                if parent and parent["status"] == "waiting_recovery":
                    self.store.enqueue_recovery_resume(parent["id"], job["resume_token"], {"resume_token": job["resume_token"], "dependency_wait_id": job["id"]})
                continue
            if job["status"] != "waiting_children":
                continue
            for boss_id in job["coordination_ids"]:
                boss = self.store.get("task", boss_id)
                effective_id = self.store.effective_boss_task_id(boss_id)
                effects = self.store.get("boss_effects", effective_id)
                shared = self._published(job, effective_id)
                if boss and boss["status"] == "completed" and effects and effects["status"] == "applied" and shared:
                    # The original group still reviews whether these results meet
                    # each blocker. This wakes review, never fabricates acceptance.
                    result = {"boss_decision_id": effective_id, "shared_memory": shared,
                        "notice": "Boss 已提交可追溯的共享信息；仍需逐项审查原阻断项，未验证结果不得当作实测"}
                    with self.store.connect(True) as c:
                        current = self.store._get(c, "dependency_wait", job["id"])
                        if current["status"] == "waiting_children":
                            self.store._put(c, "dependency_wait", job["id"], {**current, "status": "fulfilled", "result": result}, rid)
                            self.store._emit(c, rid, "dependency.ready", {"task_id": job["root_task_id"], "arbitration_id": job["arbitration_id"], "boss_decision_id": boss_id}, line_id=job["line_id"])
                    break
            else:
                # A submission exists before Verify and ordinary Boss handoff.
                # Those later facts must also trigger a fresh coordination.
                upstream_ids = {line["line_id"] for line in job["upstreams"]}
                submissions = [s for s in self.store.list("submission", rid) if s.get("line_id") in upstream_ids]
                submission_ids = {s["id"] for s in submissions}
                verifications = [v for v in self.store.list("verification", rid)
                    if v.get("submission_version_id") in submission_ids]
                fingerprint = digest({"submissions": sorted(submission_ids),
                    "verifications": sorted(verifications, key=lambda v: v["id"]),
                    "shared_memory": sorted(self._published(job), key=lambda v: v["id"])})
                boss_id = job["id"] + ":boss:" + fingerprint[:16]
                if boss_id not in job["coordination_ids"]:
                    with self.store.connect(True) as c:
                        current = self.store._get(c, "dependency_wait", job["id"])
                        ids = list(current["coordination_ids"])
                        if boss_id not in ids:
                            self.store._put(c, "task", boss_id, {"kind": "boss", "status": "pending", "attempt": 0,
                                "created_at": now(), "control_generation": project["control_generation"],
                                "payload": {"reason": "internal_dependency", "dependency_wait_id": job["id"],
                                    "waiting_line_id": job["line_id"], "dependency_line_ids": sorted(upstream_ids),
                                    "requirements": job["requirements"], "upstream_submission_ids": [s["id"] for s in submissions],
                                    "upstream_verification_ids": [v["id"] for v in verifications]}}, rid)
                            self.store._put(c, "dependency_wait", job["id"], {**current, "coordination_ids": [*ids, boss_id]}, rid)
                            self.store._emit(c, rid, "dependency.coordination_requested", {"task_id": boss_id, "waiting_task_id": job["root_task_id"], "dependency_line_ids": sorted(upstream_ids)}, line_id=job["line_id"])
                continue
            # Rebuild wakeup after a crash between fulfilled and outbox creation.
            parent = self.store.get("task", job["root_task_id"])
            if parent and parent["status"] == "waiting_recovery":
                self.store.enqueue_recovery_resume(parent["id"], job["resume_token"], {"resume_token": job["resume_token"], "dependency_wait_id": job["id"]})
        self.store.dispatch_recovery_outbox(research_id)

    async def recover_legacy(self):
        for task in self.store.list("task", status="failed"):
            if (task.get("error") or {}).get("code") != "arbitration_needs_external_input":
                continue
            project = self.store.project(task["research_id"])
            if project["status"] not in {"running", "blocked"} or project.get("stop_reason"):
                continue
            if project["status"] == "blocked" and project.get("blocked_reason") != "all_lines_blocked" and (project.get("error") or {}).get("code") != "arbitration_needs_external_input":
                continue
            if not await asyncio.to_thread(self.engine.control.allows_progress, project["id"], project["control_generation"]):
                continue
            line_id = task.get("payload", {}).get("line_id")
            line = self.store.get("line", f"{project['id']}:{line_id}") if line_id else None
            if (not line or line.get("finish_requested") or
                    line["direction_epoch"] != task["payload"].get("direction_epoch", line["direction_epoch"]) or
                    line.get("cycle_id") != task["payload"].get("cycle_id", line.get("cycle_id")) or
                    task.get("control_generation", project["control_generation"]) != project["control_generation"]):
                continue
            for request in self.store.list("arbitration_request", project["id"], source_task_id=task["id"], status="needs_external_input"):
                decision = self.engine.arbitration._decision(request)
                if self.can_route(request, decision):
                    self.store.assert_progress(project)
                    self.store.patch("task", task["id"], status="pending", owner=None, error=None, last_error=task["error"],
                        execution_phase="waiting_dependencies", dependency_recovery_id=request["id"])
                    self.store.update_line(project["id"], line_id, phase="waiting_dependencies", error=None)
                    break
