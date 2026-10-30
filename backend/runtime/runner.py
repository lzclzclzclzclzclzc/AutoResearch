"""Independent experiment supervisor and shared transactional resource service."""
from __future__ import annotations

import argparse
import contextlib
import copy
import hashlib
import hmac
import json
import os
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from .control import ControlClient, ControlUnavailable, DurableJournal, _canonical, _request, iso_now, timestamp
from .processes import ManagedProcess, detect_resources, identity_alive, process_identity, sanitized_environment, terminate_identity
from .scientific_json import loads as scientific_loads,numeric_status

HELD = ("granted", "starting", "running", "reconciling", "stopping", "releasing")
TERMINAL = ("completed", "early_stopped", "execution_failed", "cancelled", "interrupted")


def read_metrics(run:dict)->dict|list|None:
    if not run.get("cwd"):
        return None
    root=Path(run["cwd"]).resolve()
    target=(root/"metrics.json").resolve()
    try:
        target.relative_to(root)
        if not target.is_file() or target.stat().st_size>1048576:
            return None
        return scientific_loads(target.read_text(encoding="utf-8"))
    except (OSError,ValueError):
        return None


class ResourceConflict(ValueError):
    pass


class ResourceService:
    """All capacity transitions occur under the business SQLite write lock.

    starting/running reservations are never released by grant expiry. Only a
    Runner terminal proof whose full binding matches the ledger can release them.
    """

    def __init__(self, db_path: str | Path, control: Any, capacity: dict | None = None):
        self.path = Path(db_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.control = control
        self.capacity = capacity or detect_resources(self.path.parent)
        with self.connect() as c:
            c.executescript("""
            CREATE TABLE IF NOT EXISTS runtime_allocations(
                id TEXT PRIMARY KEY, request_id TEXT UNIQUE NOT NULL, state TEXT NOT NULL, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS runtime_runs(
                id TEXT PRIMARY KEY, operation_id TEXT UNIQUE NOT NULL, allocation_id TEXT UNIQUE NOT NULL,
                state TEXT NOT NULL, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS runtime_metadata(key TEXT PRIMARY KEY, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS runtime_light_jobs(id TEXT PRIMARY KEY,research_id TEXT NOT NULL,state TEXT NOT NULL,data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS runtime_event_dedup(event_key TEXT PRIMARY KEY,research_id TEXT NOT NULL,event_id TEXT UNIQUE NOT NULL);
            """)

    @contextlib.contextmanager
    def connect(self, write: bool = False):
        c = sqlite3.connect(self.path, timeout=3, isolation_level=None)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA foreign_keys=ON")
        c.execute("PRAGMA journal_mode=WAL")
        try:
            if write:
                c.execute("BEGIN IMMEDIATE")
            yield c
            if write:
                c.commit()
        except BaseException:
            if write:
                c.rollback()
            raise
        finally:
            c.close()

    def _project(self, c, research_id: str, generation: int) -> dict:
        try:
            row = c.execute("SELECT status,data FROM research_projects WHERE id=?", (research_id,)).fetchone()
        except sqlite3.OperationalError as exc:
            raise ResourceConflict("Business store is unavailable; resources remain isolated") from exc
        if row is None:
            raise ResourceConflict("Unknown research project")
        project = json.loads(row["data"])
        if row["status"] in ("terminating", "terminated", "completed", "expired", "paused", "pausing", "quarantined", "blocked"):
            raise ResourceConflict("Project does not permit new experiments")
        if project.get("control_generation", 1) != generation or timestamp(project.get("deadline_at", 0)) <= time.time():
            raise ResourceConflict("Expired project or obsolete control generation")
        return project

    def _frozen_spec(self, c, payload: dict) -> None:
        row = c.execute("SELECT research_id,data FROM documents WHERE kind='spec' AND id=?", (payload["spec_id"],)).fetchone()
        if row is None or row["research_id"] != payload["research_id"]:
            raise ResourceConflict("Frozen experiment plan does not exist in this research")
        spec = json.loads(row["data"])
        from packages.contracts.research import ExperimentSpec
        spec = {key: value for key, value in spec.items() if key in ExperimentSpec.model_fields}
        if spec.get("version") != payload["spec_version"] or hashlib.sha256(_canonical(spec)).hexdigest() != payload["spec_hash"]:
            raise ResourceConflict("Frozen plan version or content hash does not match")

    def _put_allocation(self, c, allocation: dict) -> None:
        c.execute("INSERT INTO runtime_allocations(id,request_id,state,data) VALUES(?,?,?,?) "
            "ON CONFLICT(id) DO UPDATE SET state=excluded.state,data=excluded.data",
            (allocation["allocation_id"], allocation["resource_request_id"], allocation["state"], json.dumps(allocation)))
        self._document(c, "allocation", allocation["allocation_id"], allocation)

    def _put_run(self, c, run: dict) -> None:
        c.execute("INSERT INTO runtime_runs(id,operation_id,allocation_id,state,data) VALUES(?,?,?,?,?) "
            "ON CONFLICT(id) DO UPDATE SET state=excluded.state,data=excluded.data",
            (run["experiment_run_id"], run["start_operation_id"], run["allocation_id"], run["state"], json.dumps(run)))
        self._document(c, "experiment_run", run["experiment_run_id"], run)

    def _event(self,c,run:dict,event_type:str,suffix:str,payload:dict)->None:
        key=f"runner:{run['experiment_run_id']}:{event_type}:{suffix}"
        if c.execute("SELECT 1 FROM runtime_event_dedup WHERE event_key=?",(key,)).fetchone():
            return
        event_id="evt-runner-"+hashlib.sha256(key.encode()).hexdigest()
        seq=c.execute("SELECT COALESCE(MAX(seq),0)+1 FROM events WHERE research_id=?",(run["research_id"],)).fetchone()[0]
        references={"run_id":run["experiment_run_id"],"experiment_run_id":run["experiment_run_id"],
            "allocation_id":run["allocation_id"],"spec_id":run["spec_id"],"spec_version":run["spec_version"],
            "code_version":run.get("code_version"),"agent_id":run["implementation_agent_id"],"line_id":run.get("line_id"),
            "runner_generation":run["runner_generation"],"start_operation_id":run["start_operation_id"]}
        event={"schema_version":1,"event_id":event_id,"seq":seq,"research_id":run["research_id"],
            "type":event_type,"occurred_at":iso_now(),"agent_id":run["implementation_agent_id"],"line_id":run.get("line_id"),
            "payload":{**references,"source_system":"runner","source_event_id":key,**payload}}
        c.execute("INSERT INTO events(research_id,seq,event_id,data) VALUES(?,?,?,?)",(run["research_id"],seq,event_id,json.dumps(event,ensure_ascii=False)))
        c.execute("INSERT INTO runtime_event_dedup VALUES(?,?,?)",(key,run["research_id"],event_id))

    def _started_event(self,c,run:dict)->None:
        if run.get("started_at"):
            self._event(c,run,"experiment.started","start",{"state":"running","started_at":run["started_at"],
                "pid":run.get("pid"),"process_create_time":run.get("process_create_time"),"deadline_at":run["deadline_at"]})

    def _metric_event(self,c,run:dict)->None:
        if run.get("metrics_snapshot") is not None:
            digest=hashlib.sha256(_canonical(run["metrics_snapshot"])).hexdigest()
            self._event(c,run,"experiment.metric",f"metrics:{digest}",{"kind":"scientific_metrics",
                "metrics":run["metrics_snapshot"],"metrics_numeric_status":numeric_status(run["metrics_snapshot"]),
                "metrics_sha256":digest,"sampled_at":run.get("metrics_sampled_at",iso_now())})
        sample=run.get("resource_sample")
        if sample:
            self._event(c,run,"experiment.metric",f"resources:{sample['sampled_at']}",{"kind":"resource_sample",
                "resource_sample":sample,"sampled_at":sample["sampled_at"]})

    def _document(self, c, kind: str, identity: str, payload: dict) -> None:
        # Mirror runtime projections in the application's generic document table.
        exists = c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='documents'").fetchone()
        if exists:
            c.execute("INSERT INTO documents(kind,id,research_id,version,data) VALUES(?,?,?,?,?) "
                "ON CONFLICT(kind,id) DO UPDATE SET version=documents.version+1,data=excluded.data",
                (kind, identity, payload["research_id"], 1, json.dumps(payload)))

    def _held(self, c) -> list[dict]:
        placeholders = ",".join("?" for _ in HELD)
        formal = [json.loads(row[0]) for row in c.execute(f"SELECT data FROM runtime_allocations WHERE state IN ({placeholders})", HELD)]
        light = [json.loads(row[0]) for row in c.execute("SELECT data FROM runtime_light_jobs WHERE state IN ('starting','running','stopping','reconciling')")]
        return formal + light

    def _expire(self, c, now: float) -> None:
        for row in c.execute("SELECT data FROM runtime_allocations WHERE state='granted'"):
            allocation = json.loads(row[0])
            if allocation.get("bound_run_id") is None and allocation["grant_expires_at"] <= now:
                allocation.update(state="expired", released_at=iso_now())
                self._put_allocation(c, allocation)

    def request_allocation(self, payload: dict) -> dict:
        required = ("research_id", "implementation_agent_id", "spec_id", "spec_version", "spec_hash")
        if any(not payload.get(key) for key in required):
            raise ResourceConflict("Frozen spec and original Implementation identity are required")
        rid = payload["research_id"]
        generation = int(payload.get("control_generation", 1))
        if not self.control.allows_progress(rid, generation):
            raise ControlUnavailable("Independent control gate denied allocation")
        request_id = str(payload.get("resource_request_id") or payload.get("request_id") or uuid.uuid4().hex)
        cores = int(payload.get("cpu_cores", 1))
        memory = int(payload.get("memory_bytes", 268435456))
        gpu_count = int(payload.get("gpu_count", len(payload.get("gpu_uuids", []))))
        gpu_memory = int(payload.get("gpu_memory_bytes", 0))
        if cores < 1 or memory < 1 or gpu_count < 0 or gpu_memory < 0:
            raise ResourceConflict("Resource quantities must be positive (GPU count may be zero)")
        physical = detect_resources(self.path.parent)
        with self.connect(write=True) as c:
            now = time.time()  # authoritative time after acquiring write lock
            project = self._project(c, rid, generation)
            self._frozen_spec(c, payload)
            self._expire(c, now)
            existing = c.execute("SELECT data FROM runtime_allocations WHERE request_id=?", (request_id,)).fetchone()
            if existing:
                allocation = json.loads(existing[0])
                for key in required:
                    if allocation[key] != payload[key]:
                        raise ResourceConflict("Idempotency key was reused with changed allocation content")
                for key, value in (("cpu_cores", cores), ("memory_bytes", memory),
                                   ("gpu_count", gpu_count), ("gpu_memory_bytes", gpu_memory)):
                    if allocation[key] != value:
                        raise ResourceConflict("Idempotency key was reused with changed resource quantities")
                if (allocation["state"] == "expired" and allocation.get("bound_run_id") is None
                        and not allocation.get("stop_reason") and not allocation.get("terminal_proof")
                        and allocation.get("grant_expires_at", float("inf")) <= now):
                    # Renew only a timed-out, never-admitted lease. Rotate both
                    # fences so an old start payload can never use the new grant.
                    allocation.update(state="queued", grant_revision=allocation["grant_revision"] + 1,
                        fencing_token=uuid.uuid4().hex, gpu_uuids=[])
                    for key in ("granted_at", "grant_expires_at", "released_at"):
                        allocation.pop(key, None)
                elif allocation["state"] != "queued":
                    return allocation
            else:
                allocation = {**payload, "allocation_id": uuid.uuid4().hex, "resource_request_id": request_id,
                    "grant_revision": 1, "fencing_token": uuid.uuid4().hex, "state": "queued",
                    "requested_at": now, "control_generation": generation, "deadline_at": timestamp(project["deadline_at"]),
                    "cpu_cores": cores, "memory_bytes": memory, "gpu_count": gpu_count, "gpu_memory_bytes": gpu_memory,
                    "requested_gpu_uuids": list(payload.get("gpu_uuids", [])),
                    "bound_run_id": None}
            held = self._held(c)
            used_cpu = sum(a["cpu_cores"] for a in held)
            used_memory = sum(a["memory_bytes"] for a in held)
            used_gpu = {gpu for a in held for gpu in a.get("gpu_uuids", [])}
            requested_gpus = allocation.get("requested_gpu_uuids", payload.get("gpu_uuids", []))
            allowed_gpus={g["uuid"] for g in self.capacity.get("gpus",[])}
            free_gpus=[g for g in physical.get("gpus",[]) if g["uuid"] in allowed_gpus and g["uuid"] not in used_gpu
                and g.get("occupancy_known",False) and not g.get("process_ids")]
            available = [g for g in free_gpus if g["uuid"] not in used_gpu
                and g["memory_free_bytes"] >= gpu_memory
                and g.get("occupancy_known", False) and not g.get("process_ids")
                and (not requested_gpus or g["uuid"] in requested_gpus)]
            fits = (used_cpu + cores <= self.capacity["cpu_cores"] and used_memory + memory <= self.capacity["memory_total_bytes"]
                and memory <= physical["memory_available_bytes"])
            fits = fits and len(available) >= gpu_count
            config = project.get("config_snapshot", project.get("config", {}))
            if isinstance(config, dict):
                config = config.get("team", config)
            maximum = int(config.get("experiment_concurrency", 1)) if isinstance(config, dict) else 1
            fits = fits and sum(not item.get("light_cpu") and item["research_id"] == rid for item in held) < max(maximum, 1)
            # Aging plus explicit priority: a fitting older request may not be bypassed indefinitely.
            waiting = [json.loads(row[0]) for row in c.execute("SELECT data FROM runtime_allocations WHERE state='queued'")]
            score = float(allocation.get("priority", 0)) + (now - allocation["requested_at"]) / 30
            for other in waiting:
                if other["allocation_id"] == allocation["allocation_id"]:
                    continue
                try:
                    other_project = self._project(c, other["research_id"], other["control_generation"])
                    other_config = other_project.get("config_snapshot", {}).get("team", {})
                    other_max = max(1, int(other_config.get("experiment_concurrency", 1)))
                    if sum(not item.get("light_cpu") and item["research_id"] == other["research_id"] for item in held) >= other_max:
                        continue
                except ResourceConflict:
                    # A paused/blocked/obsolete request keeps its queue position
                    # for recovery but cannot hold up currently runnable work.
                    continue
                other_score = float(other.get("priority", 0)) + (now - other["requested_at"]) / 30
                other_requested = other.get("requested_gpu_uuids", other.get("gpu_uuids", []))
                other_devices=[g for g in free_gpus if g["memory_free_bytes"]>=other.get("gpu_memory_bytes",0)
                    and (not other_requested or g["uuid"] in other_requested)]
                if (other_score > score and used_cpu + other["cpu_cores"] <= self.capacity["cpu_cores"]
                    and used_memory + other["memory_bytes"] <= self.capacity["memory_total_bytes"]
                    and other["memory_bytes"] <= physical["memory_available_bytes"]
                    and len(other_devices)>=other.get("gpu_count",0)):
                    fits = False
            impossible = (cores > self.capacity["cpu_cores"] or memory > self.capacity["memory_total_bytes"]
                or gpu_count > len(self.capacity.get("gpus", [])))
            if impossible:
                allocation.update(state="unsatisfiable", waiting_reason="申请超过机器总容量，请调整执行资源", capacity=self.capacity)
            elif fits:
                allocation.update(state="granted", granted_at=now, grant_expires_at=min(now + 30, allocation["deadline_at"]),
                    gpu_uuids=[g["uuid"] for g in available[:gpu_count]])
                allocation.pop("waiting_reason", None)
            else:
                allocation["waiting_reason"] = "等待可用资源" if not gpu_count or self.capacity.get("gpus") else "无可用 GPU"
            self._put_allocation(c, allocation)
            return allocation

    def begin_start(self, payload: dict, runner_instance_id: str, runner_generation: str) -> dict:
        run_id = payload.get("experiment_run_id") or payload.get("run_id")
        op_id = payload.get("start_operation_id")
        if not run_id or not op_id or not payload.get("code_version"):
            raise ResourceConflict("Unique run/start operation and actual code version are required")
        with self.connect(write=True) as c:
            now = time.time()
            row = c.execute("SELECT data FROM runtime_allocations WHERE id=?", (payload.get("allocation_id"),)).fetchone()
            if row is None:
                raise ResourceConflict("Unknown resource allocation")
            allocation = json.loads(row[0])
            self._project(c, allocation["research_id"], allocation["control_generation"])
            self._frozen_spec(c, allocation)
            for key in ("research_id", "implementation_agent_id", "spec_id", "spec_version", "spec_hash", "grant_revision", "fencing_token"):
                if allocation.get(key) != payload.get(key):
                    raise ResourceConflict(f"Allocation binding mismatch: {key}")
            existing = c.execute("SELECT data FROM runtime_runs WHERE id=?", (run_id,)).fetchone()
            if existing:
                run = json.loads(existing[0])
                if any(run.get(k) != v for k, v in {"allocation_id": allocation["allocation_id"],
                    "start_operation_id": op_id, "runner_instance_id": runner_instance_id,
                    "runner_generation": runner_generation}.items()):
                    raise ResourceConflict("Run identity is permanently bound to its original start operation")
                return run
            if allocation["state"] != "granted" or allocation.get("bound_run_id") or allocation.get("grant_expires_at", 0) <= now:
                raise ResourceConflict("Grant expired, already bound, or no longer startable")
            binding = {"bound_run_id": run_id, "runner_instance_id": runner_instance_id,
                "runner_generation": runner_generation, "start_operation_id": op_id, "admitted_at": now}
            allocation.update(**binding, state="starting")
            run = {**allocation, "experiment_run_id": run_id, "run_id": run_id, "state": "starting",
                "code_version": payload["code_version"], "command": payload.get("command"), "cwd": payload.get("cwd"),
                "pid": None, "process_create_time": None, "verification_status": "verification_pending"}
            self._put_allocation(c, allocation)
            self._put_run(c, run)
            return run

    def update_running(self, run: dict) -> dict:
        with self.connect(write=True) as c:
            allocation = json.loads(c.execute("SELECT data FROM runtime_allocations WHERE id=?", (run["allocation_id"],)).fetchone()[0])
            current = json.loads(c.execute("SELECT data FROM runtime_runs WHERE id=?", (run["experiment_run_id"],)).fetchone()[0])
            if current["state"] in TERMINAL or allocation["state"] in ("released", "expired"):
                return current
            if current["runner_generation"] != run["runner_generation"]:
                raise ResourceConflict("Obsolete Runner generation")
            allocation["state"] = run["state"]
            self._put_allocation(c, allocation)
            self._put_run(c, run)
            self._started_event(c,run)
            self._metric_event(c,run)
            return run

    def release(self, proof: dict, final_state: str, reason: str | None = None) -> dict:
        if final_state not in TERMINAL:
            raise ResourceConflict("Resource release requires an explicit terminal execution state")
        if not proof.get("process_tree_exited") or not proof.get("start_operation_terminal"):
            raise ResourceConflict("Resource release requires process exit and irrevocably terminal start proof")
        with self.connect(write=True) as c:
            row = c.execute("SELECT data FROM runtime_allocations WHERE id=?", (proof["allocation_id"],)).fetchone()
            allocation = json.loads(row[0])
            for key in ("allocation_id", "grant_revision", "runner_generation", "start_operation_id"):
                if proof.get(key) != allocation.get(key):
                    raise ResourceConflict("Terminal proof does not match resource binding")
            if proof.get("experiment_run_id") != allocation.get("bound_run_id"):
                raise ResourceConflict("Terminal proof is for a different run")
            current = json.loads(c.execute("SELECT data FROM runtime_runs WHERE id=?", (proof["experiment_run_id"],)).fetchone()[0])
            if allocation["state"] == "released":
                return current
            mutable=("pid","process_create_time","process_backend","started_at","exit_code","log_path","error",
                "heartbeat_at","resource_sample","metrics_snapshot","metrics_sampled_at","metrics_numeric_status")
            details=proof.get("run_details",{})
            current.update({key:details[key] for key in mutable if key in details})
            current.update(state=final_state, terminal_proof=proof, stop_reason=reason, ended_at=iso_now())
            allocation.update(state="released", released_at=iso_now(), terminal_proof=proof)
            self._put_run(c, current)
            self._put_allocation(c, allocation)
            self._started_event(c,current)
            self._metric_event(c,current)
            event_type="experiment.failed" if final_state in ("execution_failed","interrupted") else "experiment.stopped"
            self._event(c,current,event_type,"terminal",{"state":final_state,"exit_code":current.get("exit_code"),
                "stop_reason":reason,"error":current.get("error"),"ended_at":current["ended_at"],"process_tree_exited":True})
            return current

    def run(self, run_id: str) -> dict | None:
        with self.connect() as c:
            row = c.execute("SELECT data FROM runtime_runs WHERE id=?", (run_id,)).fetchone()
            return json.loads(row[0]) if row else None

    def cancel_unbound(self, research_id: str, reason: str) -> None:
        with self.connect(write=True) as c:
            for row in c.execute("SELECT data FROM runtime_allocations WHERE state IN ('queued','granted')").fetchall():
                allocation = json.loads(row[0])
                if allocation["research_id"] != research_id or allocation.get("bound_run_id") is not None:
                    continue
                # Same write lock as begin_start: an expired unbound token can never admit a process.
                allocation.update(state="expired", released_at=iso_now(), stop_reason=reason,
                    terminal_proof={"allocation_id": allocation["allocation_id"], "grant_revision": allocation["grant_revision"],
                        "never_admitted": True, "start_operation_terminal": True, "process_tree_exited": True})
                self._put_allocation(c, allocation)

    def light_jobs(self) -> list[dict]:
        with self.connect() as c:
            return [json.loads(row[0]) for row in c.execute("SELECT data FROM runtime_light_jobs")]

    def reserve_light(self, job: dict) -> dict:
        with self.connect(write=True) as c:
            self._project(c, job["research_id"], job["control_generation"])
            old = c.execute("SELECT data FROM runtime_light_jobs WHERE id=?", (job["execution_id"],)).fetchone()
            if old:
                previous = json.loads(old[0])
                if previous["content_hash"] != job["content_hash"]:
                    raise ResourceConflict("Lightweight execution ID was reused with changed content")
                return previous
            held = self._held(c)
            # Lightweight work shares CPU time; idle cores are not an admission requirement.
            if sum(v["memory_bytes"] for v in held) + job["memory_bytes"] > self.capacity["memory_total_bytes"]:
                raise ResourceConflict("轻量计算内存当前繁忙")
            c.execute("INSERT INTO runtime_light_jobs VALUES(?,?,?,?)", (job["execution_id"],job["research_id"],job["state"],json.dumps(job)))
            self._document(c,"light_cpu",job["execution_id"],job)
            return job

    def update_light(self, job: dict, terminal_proof: bool = False) -> None:
        with self.connect(write=True) as c:
            row = c.execute("SELECT data FROM runtime_light_jobs WHERE id=?", (job["execution_id"],)).fetchone()
            if not row:
                raise ResourceConflict("Unknown lightweight reservation")
            current = json.loads(row[0])
            if current["state"] in TERMINAL and (job["state"] != current["state"] or not terminal_proof):
                return
            if job["state"] in TERMINAL and not terminal_proof:
                raise ResourceConflict("Lightweight reservation requires process tree exit proof")
            c.execute("UPDATE runtime_light_jobs SET state=?,data=? WHERE id=?", (job["state"],json.dumps(job),job["execution_id"]))
            self._document(c,"light_cpu",job["execution_id"],job)

    def runs(self) -> list[dict]:
        with self.connect() as c:
            return [json.loads(row[0]) for row in c.execute("SELECT data FROM runtime_runs")]

    def resources(self) -> dict:
        with self.connect() as c:
            return {"capacity": self.capacity, "allocations": [json.loads(row[0]) for row in c.execute("SELECT data FROM runtime_allocations")]}


class RunnerSupervisor:
    def __init__(self, data_dir: str | Path = "data", control: Any = None, capacity: dict | None = None,
                 self_url: str = "http://127.0.0.1:8767", process_factory=ManagedProcess):
        self.data_dir = Path(data_dir)
        self.runtime_dir = self.data_dir / "runner"
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self._lock_file = (self.runtime_dir / "supervisor.lock").open("a+b")
        self._lock_file.seek(0)
        if os.name == "nt":
            import msvcrt
            try:
                msvcrt.locking(self._lock_file.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError as exc:
                self._lock_file.close()
                raise RuntimeError("An existing Runner owns the machine lock") from exc
        else:
            import fcntl
            fcntl.flock(self._lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.instance_id = uuid.uuid4().hex
        self.generation = uuid.uuid4().hex
        self.control = control or ControlClient()
        self.service = ResourceService(self.data_dir / "business.sqlite", self.control, capacity)
        self.journal = DurableJournal(self.runtime_dir / "journal")
        self._creation_lock = threading.RLock()
        self._state_lock = threading.RLock()
        self._operations: dict[str, dict] = {}
        self._starting_ids: set[str] = set()
        self._processes: dict[str, ManagedProcess] = {}
        self._cancelled: set[str] = set()
        self._stop_reasons: dict[str, str] = {}
        self._stopped_projects: set[str] = set()
        self._unbound_reconciling: set[str] = set()
        self._shutdown = threading.Event()
        self._process_factory = process_factory
        self._self_url = self_url
        self._control_epoch = None
        self._control_ok = False
        records, self.journal_unknown = self.journal.read_all()
        self._historical = {r["experiment_run_id"]: r for r in records if r.get("kind") == "launch"}
        for record in records:
            if record.get("kind") == "cancel":
                self._cancelled.add(record["experiment_run_id"])
        self._reconcile_restart()
        from .light_cpu import LightCpuService
        self.light_cpu = LightCpuService(self.data_dir,self.control,self.service,self.instance_id,self.generation)
        self._monitor_thread = threading.Thread(target=self._monitor, name="runner-monitor", daemon=True)
        self._monitor_thread.start()
        self._deadline_thread = threading.Thread(target=self._deadlines, name="runner-deadlines", daemon=True)
        self._deadline_thread.start()

    def _connect_control(self) -> None:
        state = self.control.status()
        epoch = state["control_epoch"]
        if self._control_epoch and self._control_epoch != epoch:
            for run in self.service.runs():
                if run["state"] not in TERMINAL:
                    self.stop_project(run["research_id"], "control_restarted")
        self._control_epoch = epoch
        for rid, project in state.get("projects", {}).items():
            if project.get("stopped") and rid not in self._stopped_projects:
                self.stop_project(rid, project.get("reason", "user_requested"))
        if hasattr(self.control, "register_supervisor"):
            self.control.register_supervisor(self.instance_id, f"{self._self_url}/stop-project")
        self._control_ok = True

    def _reconcile_restart(self) -> None:
        # Exclusive OS lock proves all old Runner threads/late creates have ended.
        for run in self.service.runs():
            if run["state"] in TERMINAL:
                # A crash can happen after resource release commits and before
                # Control receives completion. Replaying the original start must
                # also return its historical result instead of creating a run.
                self._operations[run["experiment_run_id"]] = run
                if run.get("terminal_proof", {}).get("process_tree_exited"):
                    try:
                        self.control.complete_execution(run["experiment_run_id"])
                    except Exception:
                        run.update(reconciliation_pending=True, process_tree_exited=True, terminal_state=run["state"])
                continue
            self._cancelled.add(run["experiment_run_id"])
            record = self._historical.get(run["experiment_run_id"], run)
            pid = record.get("pid")
            created = record.get("process_create_time")
            exited = not pid or not identity_alive(pid, created)
            if pid and identity_alive(pid, created):
                exited = terminate_identity(pid, created)
            # A corrupt journal cannot prove an unrecorded operation/tree absent.
            if exited and not self.journal_unknown:
                for key in ("pid", "process_create_time", "process_backend", "log_path"):
                    if record.get(key) is not None:
                        run[key] = record[key]
                run.update(state="interrupted", stop_reason="runner_restarted",
                    recovery_proof_reason="old_runner_exited_and_job_closed")
                # The ordinary completion path durably tombstones the old launch,
                # releases its exact binding, clears Control, and retries failures.
                self._finish(run, True)

    def resources(self) -> dict:
        return {**self.service.resources(), "light_cpu": self.service.light_jobs(), "runner_instance_id": self.instance_id, "runner_generation": self.generation,
            "control_epoch": self._control_epoch, "control_connected": self._control_ok,
            "journal_unknown": self.journal_unknown}

    def run_light_cpu(self,payload:dict)->dict:
        return self.light_cpu.execute(payload)

    def cancel_light_execution(self,parent_execution_id:str)->dict:
        return self.light_cpu.cancel_parent(parent_execution_id)

    def request_allocation(self, payload: dict) -> dict:
        if self.journal_unknown or self._shutdown.is_set():
            raise ResourceConflict("Runner recovery is incomplete; resources remain isolated")
        self._connect_control()
        allocation = self.service.request_allocation(payload)
        rid = payload["research_id"]
        if rid in self._stopped_projects or not self.control.allows_progress(rid, payload.get("control_generation", 1), self._control_epoch):
            self._stopped_projects.add(rid)
            self.service.cancel_unbound(rid, "project_stopped_during_allocation")
            raise ControlUnavailable("Project stopped before allocation could be delivered")
        return allocation

    def start(self, payload: dict) -> dict:
        run_id = payload.get("experiment_run_id") or payload.get("run_id")
        if not run_id or not payload.get("command") or not payload.get("cwd"):
            raise ResourceConflict("Run identity, argument-list command and workspace are required")
        if not isinstance(payload["command"], list):
            raise ResourceConflict("Experiment commands must be an argument list; shell is disabled")
        rid = payload["research_id"]
        with self._state_lock:
            operation = self._operations.get(run_id)
            if operation:
                if operation["start_operation_id"] != payload["start_operation_id"]:
                    raise ResourceConflict("Run identity cannot be reused by another start operation")
                return self.status(run_id)
            if run_id in self._cancelled or rid in self._stopped_projects or self.journal_unknown:
                raise ResourceConflict("A terminal cancellation tombstone forbids this start")
            if run_id in self._starting_ids:
                return {"experiment_run_id": run_id, "start_operation_id": payload["start_operation_id"], "state": "starting"}
            self._starting_ids.add(run_id)
        try:
            self._connect_control()
            if not self.control.allows_progress(rid, payload.get("control_generation", 1), self._control_epoch):
                raise ControlUnavailable("Independent control gate denied experiment")
            run = self.service.begin_start(payload, self.instance_id, self.generation)
        except BaseException:
            with self._state_lock:
                self._starting_ids.discard(run_id)
            raise
        # The DB admission is permanent. I/O waits occur outside the creation lock.
        with self._state_lock:
            prior = self._operations.setdefault(run_id, {**run, "start_inflight": True})
            if prior["start_operation_id"] != run["start_operation_id"]:
                raise ResourceConflict("Start operation identity conflict")
            if not prior.get("start_inflight"):
                return self.status(run_id)
        prepared_process = None
        try:
            run.update(kind="launch", record_id=f"launch-{run_id}", control_epoch=self._control_epoch,
                log_path=str((self.runtime_dir / "logs" / f"{hashlib.sha256(run_id.encode()).hexdigest()}.log").resolve()))
            self.journal.append(run).result(timeout=5)
            self.control.register_process(rid, run_id, kind="experiment", control_epoch=self._control_epoch,
                control_generation=run["control_generation"], supervisor_id=self.instance_id)
            # Query physical availability outside creation critical section; ledger already holds reservation.
            physical = detect_resources(self.data_dir)
            if run["memory_bytes"] > physical["memory_available_bytes"]:
                raise ResourceConflict("Physical memory is occupied by external programs")
            physical_gpus = {g["uuid"]: g for g in physical.get("gpus", [])}
            for gpu in run.get("gpu_uuids", []):
                if gpu not in physical_gpus or physical_gpus[gpu]["memory_free_bytes"] < run.get("gpu_memory_bytes", 0):
                    raise ResourceConflict("Allocated GPU is unavailable or occupied externally")
                if not physical_gpus[gpu].get("occupancy_known") or physical_gpus[gpu].get("process_ids"):
                    raise ResourceConflict("GPU occupancy is unknown or an external process holds the device")
            environment = sanitized_environment(payload.get("env"), run.get("gpu_uuids"))
            log_path = Path(run["log_path"])
            log_path.parent.mkdir(parents=True, exist_ok=True)
            if not self.control.allows_progress(rid, run["control_generation"], self._control_epoch):
                raise ControlUnavailable("Project stopped before process creation")
            prepared_process = self._process_factory(payload["command"], payload["cwd"], log_path,
                environment, run["cpu_cores"], run["memory_bytes"], defer_creation=True)
            with self._creation_lock:
                self._assert_local_gate(run)
                process = prepared_process
                process.create_suspended()
                self._processes[run_id] = process
                run.update(pid=process.pid, process_backend=physical["process_backend"])
            process.capture_identity()
            run["process_create_time"] = process.create_time
            self.journal.append(run).result(timeout=5)
            self.control.register_process(rid, run_id, pid=process.pid, create_time=process.create_time,
                kind="experiment", control_epoch=self._control_epoch, control_generation=run["control_generation"], supervisor_id=self.instance_id)
            if not self.control.allows_progress(rid, run["control_generation"], self._control_epoch):
                raise ControlUnavailable("Project stopped before process resume")
            with self._creation_lock:
                self._assert_local_gate(run)
                process.resume()
                run.update(state="running", started_at=iso_now())
            with self._state_lock:
                self._operations[run_id].update(run)
            self.service.update_running(run)
            return self.status(run_id)
        except Exception as exc:
            with self._creation_lock:
                self._cancelled.add(run_id)
                process = self._processes.get(run_id) or prepared_process
            exited = process.terminate() if process else True
            run.update(state="cancelled" if rid in self._stopped_projects or not self.control.allows_progress(rid) else "execution_failed",
                error=str(exc), stop_reason="start_interrupted")
            self._finish(run, exited)
            if not exited:
                raise ResourceConflict("Process exit could not be confirmed; resource reservation is retained") from exc
            final = self.status(run_id)
            if final.get("state") == "execution_failed" and final.get("terminal_proof", {}).get("start_operation_terminal"):
                # Admission succeeded and cleanup has a durable terminal proof.
                # This is a scientific execution result, so callers can package it for Verify.
                return final
            raise
        finally:
            with self._state_lock:
                self._operations[run_id]["start_inflight"] = False
                self._starting_ids.discard(run_id)

    submit = start

    def _assert_local_gate(self, run: dict) -> None:
        # No DB, disk or network calls in this critical section.
        if (not self._control_ok or run["control_epoch"] != self._control_epoch or self._shutdown.is_set()
                or run["experiment_run_id"] in self._cancelled or run["research_id"] in self._stopped_projects
                or run["deadline_at"] <= time.time()):
            raise ControlUnavailable("Local execution gate is closed")

    def _proof(self, run: dict, reason: str = "process_tree_exited") -> dict:
        return {key: run[key] for key in ("allocation_id", "grant_revision", "experiment_run_id", "runner_generation", "start_operation_id")} | {
            "process_tree_exited": True, "start_operation_terminal": True, "proof_reason": reason, "confirmed_at": iso_now()}

    def _finish(self, run: dict, exited: bool) -> None:
        run_id = run["experiment_run_id"]
        requested_reason = self._stop_reasons.get(run_id)
        if requested_reason or run["research_id"] in self._stopped_projects:
            run["state"] = "cancelled" if run["research_id"] in self._stopped_projects else "early_stopped"
            run["stop_reason"] = requested_reason or "user_requested"
        # Tombstone becomes effective in memory before any blocking persistence.
        self._cancelled.add(run_id)
        actual_exited = exited
        terminal_state = run["state"]
        metrics=read_metrics(run)
        if metrics is not None:
            run.update(metrics_snapshot=metrics,metrics_sampled_at=iso_now(),metrics_numeric_status=numeric_status(metrics))
        record = {"kind": "cancel", "record_id": f"cancel-{run_id}", "experiment_run_id": run_id,
            "research_id": run["research_id"], "start_operation_id": run["start_operation_id"], "cancelled_at": iso_now()}
        try:
            self.journal.append(record).result(timeout=5)
        except Exception:
            exited = False  # no durable terminal proof, preserve isolation
        with self._state_lock:
            # Publish terminal status only after its authoritative record and lifecycle
            # event commit together. Callers must not outrun durable evidence.
            self._operations.setdefault(run_id, {}).update(run,state="reconciling",
                terminal_state=terminal_state,reconciliation_pending=True,process_tree_exited=actual_exited)
        if exited:
            proof = self._proof(run, run.get("recovery_proof_reason", "process_tree_exited"))
            proof["run_details"]={key:run[key] for key in ("pid","process_create_time","process_backend","started_at","exit_code",
                "log_path","error","heartbeat_at","resource_sample","metrics_snapshot","metrics_sampled_at","metrics_numeric_status") if key in run}
            try:
                final = self.service.release(proof, run["state"], run.get("stop_reason"))
                with self._state_lock:
                    self._operations[run_id].update(final, reconciliation_pending=False, process_tree_exited=True)
                self.control.complete_execution(run_id)
                process = self._processes.pop(run_id, None)
                if process:
                    process.close()
            except Exception:
                with self._state_lock:
                    self._operations[run_id].update(reconciliation_pending=True, process_tree_exited=True, terminal_state=terminal_state)
        else:
            with self._state_lock:
                self._operations[run_id].update(state="stopping", reconciliation_pending=True,
                    process_tree_exited=actual_exited, terminal_state=terminal_state)

    def request_stop(self, run_id: str, reason: str = "early_stopped") -> dict:
        # First step is an in-memory latch. It never waits for SQLite/journal.
        self._stop_reasons[run_id] = reason
        self._cancelled.add(run_id)
        with self._creation_lock:
            process = self._processes.get(run_id)
        with self._state_lock:
            run = copy.deepcopy(self._operations.get(run_id))
        if run is None:
            # A cancellation arriving before its start must still forbid that late start.
            self.journal.append({"kind": "cancel", "record_id": f"cancel-{run_id}", "experiment_run_id": run_id,
                "research_id": "unknown", "cancelled_at": iso_now()})
            return {"experiment_run_id": run_id, "state": "stopping", "start_operation_terminal": False,
                "reconciliation_pending": True}
        if run["state"] in TERMINAL:
            return self.status(run_id)
        run.update(state="cancelled" if reason in ("user_requested", "time_limit_reached", "control_lost", "control_restarted", "runner_shutdown") else "early_stopped",
            stop_reason=reason)
        def clean():
            exited = process.terminate() if process else True
            # A start still in its pre-create I/O stage observes cancelled before creating.
            with self._state_lock:
                inflight = self._operations.get(run_id, {}).get("start_inflight")
            if inflight:
                return  # start() itself will finish the cancelled operation and produce proof.
            self._finish(run, exited)
        threading.Thread(target=clean, name=f"runner-stop-{run_id}", daemon=True).start()
        return {**run, "state": "stopping", "process_tree_exited": False}

    def stop_project(self, research_id: str, reason: str = "user_requested") -> dict:
        self._stopped_projects.add(research_id)
        if hasattr(self,"light_cpu"):
            self.light_cpu.stop_project(research_id,reason)
        with self._state_lock:
            ids = [rid for rid, run in self._operations.items() if run.get("research_id") == research_id]
        # No business database lookup is needed to stop every registered operation.
        for rid in ids:
            self.request_stop(rid, reason)
        if research_id not in self._unbound_reconciling:
            self._unbound_reconciling.add(research_id)
            def release_unused():
                try:
                    while not self._shutdown.is_set():
                        try:
                            self.service.cancel_unbound(research_id, reason)
                            return
                        except sqlite3.Error:
                            self._shutdown.wait(0.5)
                finally:
                    self._unbound_reconciling.discard(research_id)
            threading.Thread(target=release_unused, name=f"runner-unbound-{research_id}", daemon=True).start()
        return {"research_id": research_id, "state": "terminating", "run_ids": ids}

    def status(self, run_id: str | None = None) -> dict:
        if run_id:
            with self._state_lock:
                run = copy.deepcopy(self._operations.get(run_id))
            if run is None:
                run = self.service.run(run_id)
            return run or {"experiment_run_id": run_id, "state": "unknown"}
        with self._state_lock:
            current = copy.deepcopy(self._operations)
        return {"runner_instance_id": self.instance_id, "runner_generation": self.generation,
            "control_epoch": self._control_epoch, "control_connected": self._control_ok, "runs": list(current.values())}

    def collect(self, run_id: str, offset: int = 0, limit: int = 65536) -> dict:
        run = self.status(run_id)
        path = run.get("log_path")
        offset = max(0, int(offset))
        text, next_offset, log_size, log_status = "", offset, 0, "not_created"
        if path:
            try:
                with Path(path).open("rb") as stream:
                    log_size = os.fstat(stream.fileno()).st_size
                    # A replaced/truncated log starts a new readable window rather than staying past EOF.
                    if offset > log_size:
                        offset = 0
                    stream.seek(offset)
                    content = stream.read(min(max(int(limit), 1), 262144, max(0, log_size - offset)))
                    next_offset = stream.tell()
                    text = content.decode("utf-8", errors="replace")
                    log_status = "available"
            except FileNotFoundError:
                pass  # A process-creation failure can legitimately have no stdout file.
        artifacts = [{"path": path, "kind": "stdout_stderr", "complete": run.get("state") in TERMINAL}] if log_status == "available" else []
        metrics = None
        if run.get("cwd"):
            base = Path(run["cwd"]).resolve()
            for candidate in base.rglob("*"):
                if len(artifacts) >= 200:
                    break
                try:
                    resolved = candidate.resolve()
                    resolved.relative_to(base)
                    if not resolved.is_file():
                        continue
                    size = resolved.stat().st_size
                    artifacts.append({"path": str(resolved), "relative_path": str(resolved.relative_to(base)),
                        "size_bytes": size, "kind": "experiment_artifact", "complete": run.get("state") in TERMINAL})
                    if resolved.name == "metrics.json" and size <= 1048576:
                        metrics = scientific_loads(resolved.read_text(encoding="utf-8"))
                except (OSError, ValueError):
                    continue
        return {"run": run, "text": text, "offset": offset, "next_offset": next_offset, "log_size": log_size,
            "has_more": next_offset < log_size, "log_status": log_status, "artifacts": artifacts, "metrics": metrics}

    def _deadlines(self) -> None:
        # A contended business write or slow control request cannot delay the local clock.
        while not self._shutdown.wait(0.1):
            self.light_cpu.expire_due()
            with self._state_lock:
                expired = {run["research_id"] for run in self._operations.values()
                    if run.get("state") not in TERMINAL and run.get("deadline_at", 0) <= time.time()}
            for rid in expired:
                if rid not in self._stopped_projects:
                    self.stop_project(rid, "time_limit_reached")

    def _sample(self, run: dict) -> None:
        try:
            import psutil
            parent = psutil.Process(run["pid"])
            if abs(parent.create_time() - run["process_create_time"]) >= 0.02:
                return
            processes = [parent, *parent.children(recursive=True)]
            cpu, rss, count = 0.0, 0, 0
            for process in processes:
                try:
                    timing = process.cpu_times()
                    cpu += timing.user + timing.system
                    rss += process.memory_info().rss
                    count += 1
                except psutil.Error:
                    pass
            heartbeat = {"heartbeat_at": iso_now(), "resource_sample": {
                "cpu_seconds": cpu, "memory_rss_bytes": rss, "process_count": count, "sampled_at": iso_now()}}
            metrics=read_metrics(run)
            if metrics is not None:
                heartbeat.update(metrics_snapshot=metrics,metrics_sampled_at=iso_now(),metrics_numeric_status=numeric_status(metrics))
            run.update(heartbeat)
            with self._state_lock:
                current = self._operations.get(run["experiment_run_id"])
                if current and current.get("state") not in TERMINAL:
                    current.update(heartbeat)
            self.service.update_running(run)
        except Exception:
            # Observability failures retain the reservation and never stop the deadline thread.
            pass

    def _monitor(self) -> None:
        last_control = 0.0
        last_sample: dict[str, float] = {}
        while not self._shutdown.wait(0.1):
            if time.monotonic() - last_control >= 0.5:
                last_control = time.monotonic()
                try:
                    self._connect_control()
                except Exception:
                    self._control_ok = False
                    with self._state_lock:
                        projects = {r["research_id"] for r in self._operations.values() if r.get("state") not in TERMINAL}
                    for rid in projects:
                        self.stop_project(rid, "control_lost")
            with self._state_lock:
                operations = [(rid, copy.deepcopy(run)) for rid, run in self._operations.items()]
            for rid, run in operations:
                if run.get("reconciliation_pending") and run.get("process_tree_exited"):
                    run["state"] = run.get("terminal_state", run["state"])
                    self._finish(run, True)
                    continue
                if run.get("state") in TERMINAL:
                    continue
                if run.get("deadline_at", 0) <= time.time():
                    self.stop_project(run["research_id"], "time_limit_reached")
                    continue
                if run.get("start_inflight"):
                    continue
                process = self._processes.get(rid)
                if process:
                    try:
                        code = process.poll()
                        if code is not None:
                            # Children that outlive the parent must also finish before release.
                            exited = process.terminate() if process.tree_alive() else True
                            run.update(state="completed" if code == 0 else "execution_failed", exit_code=code)
                            self._finish(run, exited)
                        elif time.monotonic() - last_sample.get(rid, 0) >= 5:
                            last_sample[rid] = time.monotonic()
                            self._sample(run)
                    except Exception:
                        with self._state_lock:
                            self._operations[rid].update(state="reconciling", reconciliation_pending=True)
                        self.request_stop(rid, "process_state_unknown")

    def close(self) -> None:
        self._shutdown.set()
        if hasattr(self,"light_cpu"):
            self.light_cpu.close()
        with self._state_lock:
            projects = {r["research_id"] for r in self._operations.values()}
        for rid in projects:
            self.stop_project(rid, "runner_shutdown")
        for process in list(self._processes.values()):
            process.terminate()
            process.close()
        self.journal.close()
        try:
            if os.name == "nt":
                import msvcrt
                self._lock_file.seek(0)
                msvcrt.locking(self._lock_file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(self._lock_file, fcntl.LOCK_UN)
        finally:
            self._lock_file.close()


class RunnerClient:
    def __init__(self, base_url: str | None = None, token: str | None = None):
        self.base_url = (base_url or os.environ.get("AUTORESEARCH_RUNNER_URL", "http://127.0.0.1:8767")).rstrip("/")
        self.token = token

    def resources(self) -> dict:
        return _request(f"{self.base_url}/resources", token=self.token)

    def request_allocation(self, payload: dict) -> dict:
        return _request(f"{self.base_url}/allocations", payload, self.token, timeout=8)

    def run_light_cpu(self, payload: dict) -> dict:
        # The operation itself obeys the project deadline. HTTP timeout only detaches observation.
        return _request(f"{self.base_url}/light-cpu",payload,self.token,timeout=3600)

    def cancel_light_execution(self,parent_execution_id:str)->dict:
        return _request(f"{self.base_url}/cancel-light-execution",{"parent_execution_id":parent_execution_id},self.token)

    def start(self, payload: dict) -> dict:
        return _request(f"{self.base_url}/start", payload, self.token, timeout=30)

    submit = start

    def status(self, run_id: str | None = None) -> dict:
        from urllib.parse import quote
        suffix = f"/{quote(run_id, safe='')}" if run_id else ""
        return _request(f"{self.base_url}/status{suffix}", token=self.token)

    def collect(self, run_id: str, offset: int = 0, limit: int = 65536) -> dict:
        return _request(f"{self.base_url}/collect", {"run_id": run_id, "offset": offset, "limit": limit}, self.token)

    def request_stop(self, run_id: str, reason: str = "early_stopped") -> dict:
        return _request(f"{self.base_url}/request-stop", dict(run_id=run_id, reason=reason), self.token)

    def stop_project(self, research_id: str, reason: str = "user_requested") -> dict:
        return _request(f"{self.base_url}/stop-project", dict(research_id=research_id, reason=reason), self.token)


def create_app(data_dir: str | Path = "data", supervisor: RunnerSupervisor | None = None,
               control_url: str | None = None, self_url: str = "http://127.0.0.1:8767"):
    from contextlib import asynccontextmanager
    from fastapi import FastAPI, Request
    from fastapi.responses import JSONResponse
    service = supervisor or RunnerSupervisor(data_dir, ControlClient(control_url), self_url=self_url)

    @asynccontextmanager
    async def lifespan(app):
        yield
        service.close()
    app = FastAPI(title="AutoResearch experiment supervisor", lifespan=lifespan)
    app.state.supervisor = service

    @app.middleware("http")
    async def local_auth(request: Request, call_next):
        secret = os.environ.get("AUTORESEARCH_SESSION_TOKEN")
        if secret and not hmac.compare_digest(request.headers.get("x-autoresearch-token", ""), secret):
            return JSONResponse(status_code=403, content={"code": "runner_auth_required"})
        if request.headers.get("origin"):
            return JSONResponse(status_code=403, content={"code": "runner_internal_only"})
        return await call_next(request)

    @app.exception_handler(ValueError)
    async def invalid(request, exc):
        return JSONResponse(status_code=409, content={"code": "resource_conflict", "message": str(exc)})

    @app.exception_handler(ControlUnavailable)
    async def control_error(request, exc):
        return JSONResponse(status_code=503 if exc.code == "control_unavailable" else 409, content={"code": exc.code, "message": str(exc)})

    @app.get("/health")
    def health():
        return {"status": "ok", "runner_instance_id": service.instance_id, "runner_generation": service.generation}

    @app.get("/resources")
    def resources():
        return service.resources()

    @app.post("/allocations")
    def allocate(payload: dict):
        return service.request_allocation(payload)

    @app.post("/light-cpu")
    def light_cpu(payload:dict):
        return service.light_cpu.execute(payload)

    @app.post("/cancel-light-execution")
    async def cancel_light_execution(payload:dict):
        return service.light_cpu.cancel_parent(**payload)

    @app.post("/start")
    def start(payload: dict):
        return service.start(payload)

    @app.get("/status")
    def status_all():
        return service.status()

    @app.get("/status/{run_id}")
    def status_one(run_id: str):
        return service.status(run_id)

    @app.post("/collect")
    def collect(payload: dict):
        return service.collect(**payload)

    @app.post("/request-stop")
    async def stop(payload: dict):
        return service.request_stop(**payload)

    @app.post("/stop-project")
    async def stop_project(payload: dict):
        return service.stop_project(**payload)
    return app


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default=os.environ.get("AUTORESEARCH_DATA_DIR", "data"))
    parser.add_argument("--port", type=int, default=8767)
    parser.add_argument("--control-url", default=None)
    args = parser.parse_args()
    import uvicorn
    uvicorn.run(create_app(args.data_dir, control_url=args.control_url, self_url=f"http://127.0.0.1:{args.port}"),
        host="127.0.0.1", port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
