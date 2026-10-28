"""Transactional business persistence; no runtime or model work occurs here."""
from __future__ import annotations

import hashlib
import json
import mimetypes
import os
import shutil
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def now() -> str:
    return utcnow().isoformat().replace("+00:00", "Z")


def new_id(prefix: str = "") -> str:
    return f"{prefix + '-' if prefix else ''}{uuid.uuid4().hex}"


def canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


class StoreError(Exception):
    def __init__(self, code: str, message: str, status: int = 409):
        self.code, self.message, self.status = code, message, status
        super().__init__(message)


class _DispatchDeferred(Exception):
    def __init__(self, state, reason):
        self.state, self.reason = state, reason


TERMINAL = {"terminating", "terminated", "completed", "quarantined"}
DEFAULT_TEAM = {"n": 2, "m1": 2, "m2": 2, "k": 3, "boss_members": 3,
                "max_runtime_seconds": 3600, "model_concurrency": 4, "experiment_concurrency": 1}


class Store:
    def __init__(self, data_dir: str | Path = "data"):
        self.data_dir = Path(data_dir).resolve()
        self.data_dir.mkdir(parents=True, exist_ok=True)
        (self.data_dir / "artifacts").mkdir(exist_ok=True)
        self.path = self.data_dir / "business.sqlite"
        with self.connect() as c:
            c.executescript("""
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS research_projects (
                id TEXT PRIMARY KEY, client_request_id TEXT UNIQUE NOT NULL,
                status TEXT NOT NULL, version INTEGER NOT NULL, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS documents (
                kind TEXT NOT NULL, id TEXT NOT NULL, research_id TEXT,
                version INTEGER NOT NULL DEFAULT 1, data TEXT NOT NULL,
                PRIMARY KEY(kind,id), FOREIGN KEY(research_id) REFERENCES research_projects(id));
            CREATE INDEX IF NOT EXISTS idx_docs_project ON documents(kind,research_id);
            CREATE INDEX IF NOT EXISTS idx_docs_status ON documents(kind,json_extract(data,'$.status'),research_id);
            CREATE TABLE IF NOT EXISTS events (
                research_id TEXT NOT NULL, seq INTEGER NOT NULL, event_id TEXT UNIQUE NOT NULL,
                data TEXT NOT NULL, PRIMARY KEY(research_id,seq),
                FOREIGN KEY(research_id) REFERENCES research_projects(id));
            CREATE TABLE IF NOT EXISTS messages (
                cursor INTEGER PRIMARY KEY AUTOINCREMENT, message_id TEXT UNIQUE NOT NULL,
                research_id TEXT NOT NULL, conversation_id TEXT NOT NULL,
                source_system TEXT NOT NULL, source_event_id TEXT NOT NULL, data TEXT NOT NULL,
                UNIQUE(source_system,source_event_id), FOREIGN KEY(research_id) REFERENCES research_projects(id));
            CREATE INDEX IF NOT EXISTS idx_messages_conversation ON messages(conversation_id,cursor);
            CREATE INDEX IF NOT EXISTS idx_messages_project ON messages(research_id,cursor);
            CREATE INDEX IF NOT EXISTS idx_messages_edge ON messages(research_id,json_extract(data,'$.edge_id'),cursor);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_message_stream_source ON messages(
                COALESCE(json_extract(data,'$.stream_source_system'),json_extract(data,'$.source_system')),
                json_extract(data,'$.stream_source_event_id')) WHERE json_extract(data,'$.streaming_message')=1;
            CREATE TABLE IF NOT EXISTS message_stream_chunks (
                message_id TEXT NOT NULL, content_version INTEGER NOT NULL, chunk_id TEXT NOT NULL,
                content_hash TEXT NOT NULL, event_seq INTEGER,
                PRIMARY KEY(message_id,content_version,chunk_id),
                FOREIGN KEY(message_id) REFERENCES messages(message_id));
            CREATE INDEX IF NOT EXISTS idx_events_message ON events(research_id,json_extract(data,'$.payload.message_id'),seq);
            CREATE TABLE IF NOT EXISTS business_operations (
                id TEXT PRIMARY KEY, research_id TEXT NOT NULL, kind TEXT NOT NULL,
                unique_key TEXT UNIQUE NOT NULL, content_hash TEXT NOT NULL,
                payload TEXT NOT NULL, status TEXT NOT NULL, receipt TEXT, result TEXT,
                FOREIGN KEY(research_id) REFERENCES research_projects(id));
            CREATE TABLE IF NOT EXISTS stop_sync (
                research_id TEXT NOT NULL, stop_id TEXT NOT NULL, PRIMARY KEY(research_id,stop_id));
            CREATE TABLE IF NOT EXISTS idempotency (
                scope TEXT NOT NULL, request_id TEXT NOT NULL, request_hash TEXT NOT NULL,
                result TEXT NOT NULL, PRIMARY KEY(scope,request_id));
            """)
        if not self.get("settings", "team"):
            self.put("settings", "team", {**DEFAULT_TEAM, "version": 1})
        if not self.get("provider", "deepseek-default"):
            self.put("provider", "deepseek-default", {
                "id": "deepseek-default", "name": "DeepSeek", "display_name": "DeepSeek V4.1 Flash",
                "model": "deepseek-flash", "base_url": "https://api.deepseek.com/anthropic", "version": 1,
                "configured": False, "credential_version": 0, "test_status": "untested"})

    @contextmanager
    def connect(self, write: bool = False):
        c = sqlite3.connect(self.path, timeout=3, isolation_level=None)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA foreign_keys=ON")
        c.execute("PRAGMA busy_timeout=3000")
        try:
            if write:
                c.execute("BEGIN IMMEDIATE")
            yield c
            if write:
                c.commit()
        except Exception:
            if write:
                c.rollback()
            raise
        finally:
            c.close()

    transaction = connect

    def _get(self, c, kind, object_id):
        row = c.execute("SELECT data FROM documents WHERE kind=? AND id=?", (kind, object_id)).fetchone()
        return json.loads(row[0]) if row else None

    def _put(self, c, kind, object_id, data, research_id=None):
        existing = self._get(c, kind, object_id)
        if existing and research_id and existing.get("research_id") not in {None, research_id}:
            raise StoreError("cross_project_reference", "记录不属于当前研究", 403)
        if existing and kind in {"submission", "spec", "experiment_spec", "memory_version", "verification", "report_snapshot"}:
            ignore = {"id", "research_id"} if kind in {"spec", "experiment_spec"} else {"version", "id", "research_id"}
            original = {k:v for k,v in existing.items() if k not in ignore}
            supplied = {k:v for k,v in data.items() if k not in ignore}
            if original != supplied:
                raise StoreError("immutable_evidence", "已提交证据不可修改；请创建新版本")
            return existing
        value = dict(data)
        value.setdefault("id", object_id)
        if research_id:
            value.setdefault("research_id", research_id)
        value["version"] = max(int(value.get("version", 1)), int((existing or {}).get("version", 0)) + 1)
        c.execute("INSERT INTO documents(kind,id,research_id,version,data) VALUES (?,?,?,?,?) "
                  "ON CONFLICT(kind,id) DO UPDATE SET data=excluded.data,version=excluded.version",
                  (kind, object_id, research_id or (existing or {}).get("research_id"), value["version"], canonical(value)))
        return value

    def put(self, kind, object_id, data, research_id=None):
        with self.connect(True) as c:
            existed = self._get(c, kind, object_id)
            result = self._put(c, kind, object_id, data, research_id)
            if not existed and research_id and kind == "submission":
                self._emit(c, research_id, "submission.created", {"submission_id": object_id}, line_id=data.get("line_id"), cycle_id=data.get("cycle_id"))
            return result

    def get(self, kind, object_id):
        with self.connect() as c:
            return self._get(c, kind, object_id)

    def patch(self, kind, object_id, **changes):
        with self.connect(True) as c:
            obj = self._get(c, kind, object_id)
            if obj is None:
                raise StoreError("not_found", "记录不存在", 404)
            return self._put(c, kind, object_id, {**obj, **changes}, obj.get("research_id"))

    def list(self, kind, research_id=None, **filters):
        with self.connect() as c:
            query = "SELECT data FROM documents WHERE kind=?"
            params = [kind]
            if research_id is not None:
                query += " AND research_id=?"
                params.append(research_id)
            items = [json.loads(r[0]) for r in c.execute(query, params)]
        return [x for x in items if all(x.get(k) == v for k, v in filters.items())]

    def _project(self, c, research_id):
        row = c.execute("SELECT data FROM research_projects WHERE id=?", (research_id,)).fetchone()
        if not row:
            raise StoreError("research_not_found", "研究不存在", 404)
        return json.loads(row[0])

    def project(self, research_id):
        with self.connect() as c:
            return self._project(c, research_id)

    def projects(self, include_deleted=False):
        with self.connect() as c:
            query = "SELECT data FROM research_projects"
            if not include_deleted:
                query += " WHERE json_extract(data,'$.deleted_at') IS NULL"
            return [json.loads(r[0]) for r in c.execute(query + " ORDER BY rowid DESC")]

    def set_project_deleted(self, research_id, deleted, request_id):
        def change(c):
            project = self._project(c, research_id)
            if project["status"] not in {"completed", "terminated", "quarantined"}:
                raise StoreError("research_not_finished", "请先终止研究并等待执行退出，再删除记录")
            if bool(project.get("deleted_at")) != deleted:
                project = self._update_project(c, research_id, deleted_at=now() if deleted else None)
                self._emit(c, research_id, "research.record_deleted" if deleted else "research.record_restored",
                    {"title": project["title"]})
            return {"research_id": research_id, "title": project["title"], "deleted": deleted,
                "deleted_at": project.get("deleted_at")}
        return self.idempotent("research_visibility:" + research_id, request_id, {"deleted": deleted}, change)

    def _update_project(self, c, research_id, **changes):
        project = self._project(c, research_id)
        project.update(changes)
        project["version"] += 1
        project["updated_at"] = now()
        c.execute("UPDATE research_projects SET status=?,version=?,data=? WHERE id=?",
                  (project["status"], project["version"], canonical(project), research_id))
        return project

    def update_project(self, research_id, **changes):
        with self.connect(True) as c:
            return self._update_project(c, research_id, **changes)

    def _emit(self, c, research_id, event_type, payload=None, **refs):
        seq = c.execute("SELECT COALESCE(MAX(seq),0)+1 FROM events WHERE research_id=?", (research_id,)).fetchone()[0]
        event = {"schema_version": 1, "event_id": new_id("evt"), "seq": seq, "type": event_type,
                 "research_id": research_id, "occurred_at": now(), "payload": payload or {}, **refs}
        c.execute("INSERT INTO events VALUES(?,?,?,?)", (research_id, seq, event["event_id"], canonical(event)))
        return event

    def emit(self, research_id, type, payload=None, **refs):
        with self.connect(True) as c:
            return self._emit(c, research_id, type, payload, **refs)

    def events(self, research_id, after=0, limit=500):
        with self.connect() as c:
            return [json.loads(r[0]) for r in c.execute(
                "SELECT data FROM events WHERE research_id=? AND seq>? ORDER BY seq LIMIT ?",
                (research_id, after, max(1, min(limit, 2000))))]

    def create_project(self, payload):
        request_id = payload.get("client_request_id")
        request_hash = payload.get("_client_payload_hash") or hashlib.sha256(canonical(payload).encode()).hexdigest()
        if not request_id:
            raise StoreError("request_id_required", "需要幂等请求 ID", 422)
        with self.connect(True) as c:
            row = c.execute("SELECT data FROM research_projects WHERE client_request_id=?", (request_id,)).fetchone()
            if row:
                existing = json.loads(row[0])
                if existing.get("request_hash") and existing["request_hash"] != request_hash:
                    raise StoreError("idempotency_conflict", "同一创建请求 ID 不能用于不同研究内容")
                return existing
            team = {**DEFAULT_TEAM, **(self._get(c, "settings", "team") or {}), **payload.get("team", {})}
            team.pop("id", None)
            duration = payload.get("max_runtime_seconds", team["max_runtime_seconds"])
            team["max_runtime_seconds"] = duration
            research_id = new_id("research")
            provider_id = payload.get("provider_config_id", "deepseek-default")
            provider = self._get(c, "provider", provider_id)
            if not provider:
                raise StoreError("provider_not_found", "模型配置不存在", 422)
            project = {"id": research_id, "research_id": research_id, "title": payload.get("title") or "未命名研究",
                "request_hash": request_hash,
                "topic": payload["topic"], "description": payload.get("description", ""), "status": "created",
                "mode": payload.get("mode", "live"), "version": 1, "created_at": now(), "started_at": None,
                "deadline_at": None, "max_runtime_seconds": duration, "control_generation": 1,
                "stop_reason": None, "memory_version": 0, "provider_config_id": provider_id,
                "credential_version": provider.get("credential_version", 0), "workspace_path": payload.get("workspace_path"),
                "config_snapshot": {"team": team, "provider": provider, "graph_version": "3",
                    "tool_policy_version": "2", "prompt_version": "2.1", "resource_profile_id": payload.get("resource_profile_id", "local-default")}}
            c.execute("INSERT INTO research_projects VALUES (?,?,?,?,?)",
                      (research_id, request_id, project["status"], 1, canonical(project)))
            topology = build_topology(team)
            self._put(c, "topology", research_id, topology, research_id)
            for node in topology["nodes"]:
                if node["type"] != "group":
                    self._put(c, "agent", f"{research_id}:{node['id']}", node["data"], research_id)
            for i in range(1, team["n"] + 1):
                line_id = f"line-{i:02d}"
                self._put(c, "line", f"{research_id}:{line_id}", {
                    "line_id": line_id, "title": f"研究线 {i}", "direction_epoch": 1, "phase": "awaiting_boss",
                    "completed_verify_rounds": 0, "k": team["k"], "cycle_id": None,
                    "exploration_group_id": f"{line_id}/exploration", "critic_group_id": f"{line_id}/critic",
                    "implementation_agent_id": f"{line_id}/implementation", "verify_agent_id": f"{line_id}/verify"}, research_id)
            self._emit(c, research_id, "research.created", {"mode": project["mode"], "title": project["title"], "topic": project["topic"]})
            self._put(c, "task", f"{research_id}:supervisor", {
                "kind": "supervisor", "payload": {}, "status": "pending", "created_at": now(),
                "idempotency_key": f"{research_id}:supervisor", "attempt": 0}, research_id)
            return project

    def start_project(self, research_id):
        with self.connect(True) as c:
            p = self._project(c, research_id)
            if p["started_at"]:
                return p
            if p["status"] in TERMINAL:
                raise StoreError("research_stopped", "研究已停止")
            start = utcnow()
            p = self._update_project(c, research_id, started_at=start.isoformat().replace("+00:00", "Z"),
                deadline_at=(start + timedelta(seconds=p["max_runtime_seconds"])).isoformat().replace("+00:00", "Z"), status="running")
            self._emit(c, research_id, "research.started", {"deadline_at": p["deadline_at"]})
            return p

    def lines(self, research_id):
        return self.list("line", research_id)

    def update_line(self, research_id, line_id, **changes):
        with self.connect(True) as c:
            key = f"{research_id}:{line_id}"
            line = self._get(c, "line", key)
            if not line:
                raise StoreError("line_not_found", "研究线不存在", 404)
            result = self._put(c, "line", key, {**line, **changes}, research_id)
            if "phase" in changes:
                self._emit(c, research_id, "line.phase_changed", changes, line_id=line_id)
            return result

    def snapshot(self, research_id):
        with self.connect() as c:
            c.execute("BEGIN")
            project = self._project(c, research_id)
            docs = list(c.execute("SELECT kind,data FROM documents WHERE research_id=? ORDER BY rowid", (research_id,)))
            seq = c.execute("SELECT COALESCE(MAX(seq),0) FROM events WHERE research_id=?", (research_id,)).fetchone()[0]
            recent = [json.loads(r[0]) for r in c.execute("SELECT data FROM events WHERE research_id=? ORDER BY seq DESC LIMIT 50", (research_id,))][::-1]
            creation_events = [json.loads(r[0]) for r in c.execute(
                "SELECT data FROM events WHERE research_id=? AND json_extract(data,'$.type') IN ('submission.created','verification.completed','report.completed','experiment.started') ORDER BY seq", (research_id,))]
            temporal = {}
            for event in creation_events:
                for reference in ("submission_id", "submission_version_id", "report_id", "experiment_run_id", "run_id"):
                    identity = event["payload"].get(reference)
                    if identity:
                        temporal.setdefault(identity, {"created_at": event["occurred_at"], "created_event_seq": event["seq"]})
            by_kind = {}
            for row in docs:
                by_kind.setdefault(row["kind"], []).append(json.loads(row["data"]))
            executions = by_kind.get("execution", []) + by_kind.get("agent_execution", [])
            latest_execution = {}
            for execution in sorted(executions, key=lambda x: x.get("started_at", x.get("created_at", ""))):
                latest_execution[execution.get("agent_id")] = execution
            agents = [{**agent, "status": latest_execution.get(agent.get("agent_id"), {}).get("status", agent.get("status", "idle"))}
                      for agent in by_kind.get("agent", [])]
            node_counts = {row["identity"]: {"message_count": row["message_count"], "latest_message_seq": row["latest_message_seq"]}
                for row in c.execute("""
                WITH involved AS (
                  SELECT cursor,json_extract(data,'$.sender.id') AS identity,CAST(COALESCE(json_extract(data,'$.latest_event_seq'),json_extract(data,'$.event_seq')) AS INTEGER) AS seq
                    FROM messages WHERE research_id=?
                  UNION
                  SELECT m.cursor,json_extract(r.value,'$.id'),CAST(COALESCE(json_extract(m.data,'$.latest_event_seq'),json_extract(m.data,'$.event_seq')) AS INTEGER)
                    FROM messages m,json_each(m.data,'$.recipients') r WHERE m.research_id=?
                ), expanded AS (
                  SELECT cursor,identity,seq FROM involved
                  UNION
                  SELECT cursor,substr(identity,1,instr(identity,'/member-')-1),seq FROM involved WHERE instr(identity,'/member-')>0
                ) SELECT identity,COUNT(*) AS message_count,COALESCE(MAX(seq),0) AS latest_message_seq
                  FROM expanded WHERE identity IS NOT NULL GROUP BY identity
                """, (research_id, research_id))}
            edge_counts = {row["identity"]: {"message_count": row["message_count"], "latest_message_seq": row["latest_message_seq"]}
                for row in c.execute("""SELECT json_extract(data,'$.edge_id') AS identity,COUNT(*) AS message_count,
                    COALESCE(MAX(CAST(COALESCE(json_extract(data,'$.latest_event_seq'),json_extract(data,'$.event_seq')) AS INTEGER)),0) AS latest_message_seq
                    FROM messages WHERE research_id=? AND json_extract(data,'$.edge_id') IS NOT NULL GROUP BY identity""", (research_id,))}
            for agent in agents:
                agent.update(node_counts.get(agent.get("agent_id"), {"message_count": 0, "latest_message_seq": 0}))
            topology = (by_kind.get("topology") or [{}])[0]
            for node in topology.get("nodes", []):
                agent = next((a for a in agents if a.get("agent_id") == node["id"]), None)
                if agent:
                    node["data"]["status"] = agent["status"]
                node["data"].update(node_counts.get(node["id"], {"message_count": 0, "latest_message_seq": 0}))
                if node["type"] == "group":
                    children = [a["status"] for a in agents if a.get("agent_id", "").startswith(node["id"] + "/member-")]
                    node["data"]["status"] = "running" if any(status in {"running", "registered"} for status in children) else "failed" if "failed" in children else "completed" if children and all(status == "completed" for status in children) else "idle"
            for edge in topology.get("edges", []):
                edge.setdefault("data", {}).update(edge_counts.get(edge["id"], {"message_count": 0, "latest_message_seq": 0}))
            runs = {}
            for run in by_kind.get("run", []) + by_kind.get("experiment_run", []):
                identity = run.get("experiment_run_id", run.get("run_id", run.get("id")))
                runs[identity] = {**temporal.get(identity, {}), **run, "id": identity, "run_id": identity, "status": run.get("state", run.get("status"))}
            verifications = by_kind.get("verification", [])
            for verification in verifications:
                for run_id in verification.get("run_ids", []):
                    if run_id in runs:
                        runs[run_id]["observation"] = verification.get("scientific_observation")
                        runs[run_id]["verification_status"] = "passed" if verification["passed"] else "failed"
            submissions = []
            for submission in by_kind.get("submission", []):
                reports = [v for v in verifications if v.get("submission_version_id") == submission["id"]]
                archives = [a for a in by_kind.get("submission_archive", []) if a.get("submission_id") == submission["id"]]
                timestamps = temporal.get(submission["id"], {})
                if not timestamps:
                    dates = [a.get("created_at") for a in submission.get("artifacts", []) if a.get("created_at")]
                    if dates:
                        timestamps = {"created_at": max(dates)}
                projection = {**timestamps, **submission, "verifications": reports, "manifest_status": submission.get("status")}
                if reports:
                    projection["verification_status"] = "passed" if reports[-1]["passed"] else "failed"
                elif archives:
                    projection["verification_status"] = "verification_pending"
                submissions.append(projection)
            return {"project": project, "topology": topology,
                "lines": by_kind.get("line", []), "agents": agents,
                "recent_events": recent, "snapshot_seq": seq, "executions": by_kind.get("execution", []),
                "runs": list(runs.values()), "submissions": submissions,
                "reports": [{**temporal.get(report["id"], {}), **report} for report in by_kind.get("report", [])], "memory": by_kind.get("memory_node", []),
                "verification_reports": verifications, "submission_archives": by_kind.get("submission_archive", []),
                "tasks": by_kind.get("task", []), "operations": self._operations(c, research_id)}

    def create_conversation(self, research_id, conversation_id, **fields):
        with self.connect(True) as c:
            existing = self._get(c, "conversation", conversation_id)
            if existing:
                if existing["research_id"] != research_id:
                    raise StoreError("cross_project_reference", "会话不属于当前研究", 403)
                participants = {canonical(p): p for p in existing.get("participants", []) + fields.get("participants", [])}
                if len(participants) != len(existing.get("participants", [])):
                    return self._put(c, "conversation", conversation_id, {**existing, "participants": list(participants.values())}, research_id)
                return existing
            return self._put(c, "conversation", conversation_id, {"conversation_id": conversation_id,
                "title": fields.pop("title", conversation_id), "kind": fields.pop("kind", "discussion"),
                "created_at": now(), "unread_position": 0, **fields}, research_id)

    def create_message(self, research_id, conversation_id, sender, content_markdown, **fields):
        with self.connect(True) as c:
            return self._create_message(c, research_id, conversation_id, sender, content_markdown, **fields)

    def _create_message(self, c, research_id, conversation_id, sender, content_markdown, **fields):
        conversation = self._get(c, "conversation", conversation_id)
        if conversation and conversation["research_id"] != research_id:
            raise StoreError("cross_project_reference", "会话不属于当前研究", 403)
        if not conversation:
            self._put(c, "conversation", conversation_id, {"conversation_id": conversation_id,
                "title": conversation_id, "kind": "discussion", "created_at": now(), "unread_position": 0}, research_id)
        source_system = fields.pop("source_system", "business")
        source_id = fields.pop("source_event_id", None) or fields.get("message_id") or new_id("source")
        existing = c.execute("SELECT data,cursor FROM messages WHERE source_system=? AND source_event_id=?", (source_system, source_id)).fetchone()
        if existing:
            original = json.loads(existing[0])
            if original["research_id"] != research_id:
                raise StoreError("cross_project_reference", "消息来源 ID 已属于另一研究", 403)
            return {**original, "cursor": existing[1]}
        msg = {"message_id": fields.pop("message_id", new_id("msg")), "research_id": research_id,
            "conversation_id": conversation_id, "sender": sender if isinstance(sender, dict) else {"type": "agent", "id": sender},
            "content_markdown": content_markdown, "recipients": [], "kind": "discussion",
            "status": "complete", "created_at": now(), "source_system": source_system, "source_event_id": source_id, **fields}
        conversation = self._get(c, "conversation", conversation_id)
        participants = {canonical(p): p for p in conversation.get("participants", []) + [msg["sender"]] + msg["recipients"]}
        groups = set(conversation.get("group_ids", []))
        for participant in participants.values():
            identity = participant.get("id", "") if isinstance(participant, dict) else participant
            if isinstance(participant, dict) and participant.get("type") == "group":
                groups.add(identity)
            elif "/member-" in identity:
                groups.add(identity.rsplit("/", 1)[0])
        self._put(c, "conversation", conversation_id, {**conversation, "participants": list(participants.values()),
            "group_ids": sorted(groups), "line_id": conversation.get("line_id") or msg.get("line_id"), "last_message_at": msg["created_at"]}, research_id)
        cursor = c.execute("INSERT INTO messages(message_id,research_id,conversation_id,source_system,source_event_id,data) VALUES(?,?,?,?,?,?)",
            (msg["message_id"], research_id, conversation_id, source_system, source_id, canonical(msg))).lastrowid
        event = self._emit(c, research_id, "message.completed" if msg["status"] == "complete" else "message.interrupted",
            {"message_id": msg["message_id"], "message": msg}, conversation_id=conversation_id,
            agent_id=msg["sender"].get("id"), edge_id=msg.get("edge_id"), cycle_id=msg.get("cycle_id"))
        msg["event_seq"] = event["seq"]
        c.execute("UPDATE messages SET data=? WHERE message_id=?", (canonical(msg), msg["message_id"]))
        event["payload"]["message"] = msg
        c.execute("UPDATE events SET data=? WHERE research_id=? AND seq=?", (canonical(event), research_id, event["seq"]))
        return {**msg, "cursor": cursor}

    def _stream_conversation(self, c, message):
        conversation_id, research_id = message["conversation_id"], message["research_id"]
        conversation = self._get(c, "conversation", conversation_id)
        if conversation and conversation["research_id"] != research_id:
            raise StoreError("cross_project_reference", "会话不属于当前研究", 403)
        conversation = conversation or {"conversation_id": conversation_id, "title": conversation_id,
            "kind": "discussion", "created_at": message["created_at"], "unread_position": 0}
        participants = {canonical(p): p for p in conversation.get("participants", []) +
                        [message["sender"]] + message.get("recipients", [])}
        groups = set(conversation.get("group_ids", []))
        for participant in participants.values():
            identity = participant.get("id", "") if isinstance(participant, dict) else participant
            if isinstance(participant, dict) and participant.get("type") == "group":
                groups.add(identity)
            elif "/member-" in identity:
                groups.add(identity.rsplit("/", 1)[0])
        self._put(c, "conversation", conversation_id, {**conversation, "participants": list(participants.values()),
            "group_ids": sorted(groups), "line_id": conversation.get("line_id") or message.get("line_id"),
            "last_message_at": message.get("updated_at", message["created_at"])}, research_id)

    def _stream_event(self, c, event_type, message, payload):
        refs = {key: message[key] for key in ("line_id", "group_id", "execution_id", "task_id", "edge_id",
            "cycle_id", "graph_node_id", "causation_id", "correlation_id") if message.get(key) is not None}
        return self._emit(c, message["research_id"], event_type, payload,
            conversation_id=message["conversation_id"], agent_id=message["sender"].get("id"), **refs)

    def _complete_stream(self, c, row, full_content, status, source_system, source_event_id, fields):
        message = json.loads(row["data"])
        if not message.get("streaming_message"):
            raise StoreError("not_stream_message", "该消息不是流式消息", 409)
        if message["status"] == "interrupted" or (message["status"] == "complete" and status == "interrupted"):
            return {**message, "cursor": row["cursor"]}
        project = self._project(c, message["research_id"])
        try:
            self.assert_progress(project)
        except StoreError:
            if status == "complete":
                if message["status"] == "complete":
                    return {**message, "cursor": row["cursor"]}
                status, full_content = "interrupted", message["content_markdown"]
        if status == "interrupted" and not full_content:
            full_content = message["content_markdown"]
        reserved = {"message_id", "research_id", "conversation_id", "sender", "created_at", "event_seq",
                    "started_event_seq", "latest_event_seq", "streaming_message", "stream_source_system", "stream_source_event_id", "content_markdown", "status", "cursor"}
        if any(key in fields for key in reserved):
            raise StoreError("immutable_message_identity", "流式消息的身份字段不能修改", 422)
        version = int(fields.pop("content_version", message.get("content_version", 1)))
        if version < message.get("content_version", 1):
            raise StoreError("stale_message_version", "不能以旧内容版本覆盖消息", 409)
        if full_content != message["content_markdown"] and version <= message.get("content_version", 1):
            version = message.get("content_version", 1) + 1
        source_id = source_event_id or row["source_event_id"]
        conflict = c.execute("SELECT message_id FROM messages WHERE source_system=? AND source_event_id=? AND message_id<>?",
            (source_system, source_id, message["message_id"])).fetchone()
        if conflict:
            raise StoreError("message_source_conflict", "消息来源已经绑定其他消息", 409)
        changed = (message["status"] != status or message["content_markdown"] != full_content or
                   any(message.get(key) != value for key, value in fields.items()) or
                   row["source_system"] != source_system or row["source_event_id"] != source_id)
        if not changed:
            return {**message, "cursor": row["cursor"]}
        message.update(fields)
        message.update(content_markdown=full_content, status=status, partial=status == "interrupted",
            content_version=version, source_system=source_system, source_event_id=source_id,
            updated_at=now(), completed_at=now())
        self._stream_conversation(c, message)
        event = self._stream_event(c, "message.completed" if status == "complete" else "message.interrupted", message,
            {"message_id": message["message_id"], "message": message, "full_content": full_content,
             "content_version": version, "replace": True})
        message["latest_event_seq"] = event["seq"]
        c.execute("UPDATE messages SET source_system=?,source_event_id=?,data=? WHERE message_id=?",
            (source_system, source_id, canonical(message), message["message_id"]))
        event["payload"]["message"] = message
        c.execute("UPDATE events SET data=? WHERE research_id=? AND seq=?", (canonical(event), message["research_id"], event["seq"]))
        return {**message, "cursor": row["cursor"]}

    def upsert_stream_message(self, research_id, message_id, conversation_id, sender, full_content,
                              *, content_version=1, source_event_id, **fields):
        """Persist a cumulative real text snapshot, emitting each delta once.

        Empty first snapshots create no placeholder. A stream keeps one cursor;
        chunk receipts reject duplicate/late append attempts and survive restart.
        """
        if not isinstance(full_content, str) or type(content_version) is not int or content_version < 1:
            raise StoreError("invalid_stream_content", "流式正文与内容版本无效", 422)
        if not source_event_id:
            raise StoreError("message_source_required", "流式消息需要稳定来源 ID", 422)
        chunk_id = str(fields.pop("chunk_id", "") or hashlib.sha256(full_content.encode()).hexdigest())
        source_system = fields.pop("source_system", "dsh")
        sender = sender if isinstance(sender, dict) else {"type": "agent", "id": sender}
        protected = {"message_id", "research_id", "conversation_id", "sender", "content_markdown", "status",
                     "created_at", "event_seq", "latest_event_seq", "started_event_seq", "streaming_message", "stream_source_system", "stream_source_event_id", "cursor"}
        if any(key in fields for key in protected):
            raise StoreError("immutable_message_identity", "流式消息的身份字段不能修改", 422)
        with self.connect(True) as c:
            row = c.execute("SELECT * FROM messages WHERE message_id=?", (message_id,)).fetchone()
            message = json.loads(row["data"]) if row else None
            if message and (message["research_id"] != research_id or message["conversation_id"] != conversation_id or message["sender"] != sender):
                raise StoreError("cross_project_reference", "流式消息身份与原记录不匹配", 403)
            if message and not message.get("streaming_message"):
                raise StoreError("not_stream_message", "该消息不是流式消息", 409)
            if message and message.get("stream_source_event_id") != source_event_id:
                raise StoreError("message_source_conflict", "流式消息的来源身份不能变化", 409)
            if message and message["status"] in {"complete", "interrupted"}:
                return {**message, "cursor": row["cursor"]}
            project = self._project(c, research_id)
            try:
                self.assert_progress(project)
            except StoreError:
                return self._complete_stream(c, row, message["content_markdown"], "interrupted",
                    row["source_system"], None, {}) if row else None
            if not full_content and not message:
                return None
            content_hash = hashlib.sha256(full_content.encode()).hexdigest()
            seen = c.execute("SELECT content_hash FROM message_stream_chunks WHERE message_id=? AND content_version=? AND chunk_id=?",
                (message_id, content_version, chunk_id)).fetchone()
            if seen:
                if seen["content_hash"] != content_hash:
                    raise StoreError("stream_chunk_conflict", "相同流式分片 ID 的内容发生变化", 409)
                return {**message, "cursor": row["cursor"]}
            previous_content = message["content_markdown"] if message else ""
            previous_version = message.get("content_version", 1) if message else content_version
            if message and (content_version < previous_version or
                    (content_version == previous_version and previous_content.startswith(full_content))):
                c.execute("INSERT INTO message_stream_chunks VALUES(?,?,?,?,NULL)", (message_id, content_version, chunk_id, content_hash))
                return {**message, "cursor": row["cursor"]}
            replacing = bool(message and content_version > previous_version)
            if message and not replacing and not full_content.startswith(previous_content):
                raise StoreError("stream_content_conflict", "正文修订必须增加内容版本", 409)
            if message is None:
                conflict = c.execute("""SELECT 1 FROM messages WHERE (source_system=? AND source_event_id=?) OR
                    (COALESCE(json_extract(data,'$.stream_source_system'),json_extract(data,'$.source_system'))=? AND
                     json_extract(data,'$.stream_source_event_id')=?)""", (source_system, source_event_id, source_system, source_event_id)).fetchone()
                if conflict:
                    raise StoreError("message_source_conflict", "消息来源已经绑定其他消息", 409)
                message = {"message_id": message_id, "research_id": research_id, "conversation_id": conversation_id,
                    "sender": sender, "content_markdown": full_content, "recipients": [], "kind": "discussion",
                    **fields, "status": "streaming", "partial": True, "created_at": now(), "updated_at": now(),
                    "content_version": content_version, "streaming_message": True, "source_system": source_system,
                    "source_event_id": source_event_id, "stream_source_event_id": source_event_id, "stream_source_system": source_system}
                cursor = c.execute("INSERT INTO messages(message_id,research_id,conversation_id,source_system,source_event_id,data) VALUES(?,?,?,?,?,?)",
                    (message_id, research_id, conversation_id, source_system, source_event_id, canonical(message))).lastrowid
                started = self._stream_event(c, "message.started", message, {"message_id": message_id,
                    "content_version": content_version, "sender": sender, "created_at": message["created_at"]})
                message["event_seq"] = message["started_event_seq"] = started["seq"]
                started["payload"]["message"] = {key: value for key, value in message.items() if key != "content_markdown"}
                c.execute("UPDATE events SET data=? WHERE research_id=? AND seq=?", (canonical(started), research_id, started["seq"]))
            else:
                cursor = row["cursor"]
                message.update(fields)
                message.update(content_markdown=full_content, content_version=content_version, updated_at=now())
            self._stream_conversation(c, message)
            payload = {"message_id": message_id, "delta": full_content[len(previous_content):] if not replacing else "",
                "content_version": content_version, "chunk_id": chunk_id, "replace": replacing,
                "content_length": len(full_content)}
            if replacing:
                payload["full_content"] = full_content
            event = self._stream_event(c, "message.delta", message, payload)
            message["latest_event_seq"] = event["seq"]
            c.execute("UPDATE messages SET data=? WHERE message_id=?", (canonical(message), message_id))
            c.execute("INSERT INTO message_stream_chunks VALUES(?,?,?,?,?)", (message_id, content_version, chunk_id, content_hash, event["seq"]))
            return {**message, "cursor": cursor}

    def complete_stream_message(self, message_id, full_content, status="complete", source_system="dsh", source_event_id=None, **fields):
        if not isinstance(full_content, str) or status not in {"complete", "interrupted"}:
            raise StoreError("invalid_stream_completion", "流式消息完成状态无效", 422)
        with self.connect(True) as c:
            row = c.execute("SELECT * FROM messages WHERE message_id=?", (message_id,)).fetchone()
            if row is None:
                return None
            return self._complete_stream(c, row, full_content, status, source_system, source_event_id, fields)

    def _messages_at(self, c, rows, at_seq):
        if not rows:
            return []
        identities = [json.loads(row["data"])["message_id"] for row in rows]
        placeholders = ",".join("?" for _ in identities)
        research_id = json.loads(rows[0]["data"])["research_id"]
        events = c.execute(f"""SELECT data FROM events WHERE research_id=?
            AND json_extract(data,'$.payload.message_id') IN ({placeholders}) AND seq<=? ORDER BY seq""",
            [research_id, *identities, at_seq])
        histories = {}
        for row in events:
            event = json.loads(row["data"])
            payload, event_type = event["payload"], event["type"]
            identity = payload["message_id"]
            if event_type == "message.started":
                message = dict(payload.get("message", {}))
                message.setdefault("message_id", identity)
                message.setdefault("research_id", research_id)
                message.setdefault("conversation_id", event.get("conversation_id"))
                message.setdefault("sender", payload.get("sender"))
                message.setdefault("created_at", payload.get("created_at", event["occurred_at"]))
                message.update(content_markdown="", status="streaming", partial=True,
                    content_version=payload.get("content_version", 1), event_seq=event["seq"], latest_event_seq=event["seq"])
                histories[identity] = message
            elif event_type == "message.delta":
                message = histories.get(identity)
                if message is None:
                    continue
                version = payload.get("content_version", message.get("content_version", 1))
                if version < message.get("content_version", 1):
                    continue
                if payload.get("replace"):
                    message["content_markdown"] = payload.get("full_content", "")
                elif version == message.get("content_version", 1):
                    message["content_markdown"] += payload.get("delta", "")
                else:
                    continue
                message.update(content_version=version, latest_event_seq=event["seq"], updated_at=event["occurred_at"])
            elif event_type in {"message.completed", "message.interrupted"}:
                previous = histories.get(identity, {})
                message = {**previous, **payload.get("message", {})}
                message["content_markdown"] = payload.get("full_content", message.get("content_markdown", ""))
                message.update(status="complete" if event_type == "message.completed" else "interrupted",
                    partial=event_type == "message.interrupted", latest_event_seq=event["seq"])
                message.setdefault("event_seq", event["seq"])
                histories[identity] = message
        projected = []
        for row in rows:
            current = json.loads(row["data"])
            message = histories.get(current["message_id"])
            if message is None:
                if current.get("streaming_message"):
                    raise StoreError("message_history_unavailable", "无法还原指定游标的流式消息，请重新获取历史快照", 409)
                message = current
            projected.append({**message, "cursor": row["cursor"], "history_at_seq": at_seq})
        return projected

    def _message_page(self, c, sql, args, limit, q=None, at_seq=None):
        if at_seq is None:
            if q:
                sql += " AND instr(lower(json_extract(data,'$.content_markdown')),lower(?))>0"
                args = [*args, q]
            rows = c.execute(sql + " ORDER BY cursor DESC LIMIT ?", [*args, limit+1]).fetchall()
            items = [{**json.loads(row["data"]), "cursor": row["cursor"]} for row in rows]
        else:
            sql += " AND CAST(COALESCE(json_extract(data,'$.event_seq'),0) AS INTEGER)<=?"
            args = [*args, at_seq]
            items, scan_before = [], 9223372036854775807
            while len(items) <= limit:
                rows = c.execute(sql + " AND cursor<? ORDER BY cursor DESC LIMIT ?", [*args, scan_before, max(50, limit+1)]).fetchall()
                if not rows:
                    break
                for message in self._messages_at(c, rows, at_seq):
                    if not q or q.casefold() in message.get("content_markdown", "").casefold():
                        items.append(message)
                        if len(items) > limit:
                            break
                scan_before = rows[-1]["cursor"]
                if len(rows) < max(50, limit+1):
                    break
        return {"items": items[:limit][::-1], "next_cursor": str(items[limit-1]["cursor"]) if len(items)>limit else None}

    def messages(self, conversation_id, before=None, limit=50, q=None, at_seq=None):
        limit = min(max(int(limit), 1), 200)
        with self.connect() as c:
            sql = "SELECT cursor,data FROM messages WHERE conversation_id=? AND cursor<?"
            args = [conversation_id, int(before) if before else 9223372036854775807]
            return self._message_page(c, sql, args, limit, q=q, at_seq=at_seq)

    def message(self, research_id, message_id):
        with self.connect() as c:
            row = c.execute("SELECT data FROM messages WHERE research_id=? AND message_id=?", (research_id, message_id)).fetchone()
            if not row:
                raise StoreError("message_not_found", "消息不存在", 404)
            return json.loads(row[0])

    def edge_messages(self, research_id, edge_id, before=None, limit=100, q=None, at_seq=None):
        limit = min(max(int(limit), 1), 200)
        with self.connect() as c:
            sql = "SELECT cursor,data FROM messages WHERE research_id=? AND json_extract(data,'$.edge_id')=? AND cursor<?"
            args = [research_id, edge_id, int(before) if before else 9223372036854775807]
            return self._message_page(c, sql, args, limit, q=q, at_seq=at_seq)

    def add_task(self, research_id, kind, payload, idempotency_key=None):
        task_id = idempotency_key or new_id("task")
        with self.connect(True) as c:
            existing = self._get(c, "task", task_id)
            if existing:
                return existing
            return self._put(c, "task", task_id, {"kind": kind, "payload": payload,
                "status": "pending", "created_at": now(), "attempt": 0}, research_id)

    def pending_tasks(self, limit=100):
        return [t for t in self.list("task") if t["status"] == "pending"][:limit]

    def claim_task(self, task_id, owner):
        with self.connect(True) as c:
            t = self._get(c, "task", task_id)
            if not t or t["status"] != "pending":
                return None
            return self._put(c, "task", task_id, {**t, "status": "running", "owner": owner,
                "claimed_at": now(), "attempt": t.get("attempt", 0)+1}, t["research_id"])

    def finish_task(self, task_id, result=None, error=None):
        return self.patch("task", task_id, status="failed" if error else "completed",
                          result=result, error=error, completed_at=now())

    def _handoff_gate(self, control, research_id, generation):
        try:
            if control.allows_progress(research_id, generation):
                return "allow"
        except Exception:
            return "wait"
        try:
            status = control.status(research_id)
        except Exception:
            return "cancel" if getattr(control, "stopped", False) else "wait"
        if (status.get("stopped") or status.get("completed") or status.get("state") in {"unknown", "quarantined", "completed", "terminated", "terminating"}
                or status.get("control_generation", generation) != generation):
            return "cancel"
        return "allow" if status.get("allowed") else "wait"

    def _verification_handoff(self, c, outbox):
        rid, payload = outbox["research_id"], outbox["payload"]
        project = self._project(c, rid)
        self.assert_progress(project, outbox["control_generation"])
        if project["status"] == "paused":
            raise _DispatchDeferred("wait", "research_paused")
        cycle = self._get(c, "cycle", payload["cycle_id"])
        submission = self._get(c, "submission", payload["submission_version_id"])
        if (not cycle or not submission or cycle.get("research_id") != rid or submission.get("research_id") != rid
                or submission.get("cycle_id") != cycle["id"] or cycle.get("status") not in {"active", "verifying", "waiting_boss"}):
            raise StoreError("stale_handoff", "交接的研究周期或提交包已失效")
        line = self._get(c, "line", f"{rid}:{cycle['line_id']}")
        if not line or line.get("cycle_id") not in {None, cycle["id"]}:
            raise StoreError("stale_handoff", "交接不属于研究线当前周期")
        operation = c.execute("SELECT * FROM business_operations WHERE id=?", (outbox["id"],)).fetchone()
        if (not operation or operation["research_id"] != rid or operation["kind"] != "verification"
                or operation["status"] != "applied" or not operation["receipt"] or not operation["result"]
                or json.loads(operation["result"]) != payload):
            raise StoreError("invalid_handoff", "交接需要已生效的验证与控制回执")
        receipt = json.loads(operation["receipt"])
        if (not receipt.get("persisted") or receipt.get("content_hash") != operation["content_hash"]
                or receipt.get("operation_id") != operation["id"] or receipt.get("research_id") != rid
                or receipt.get("control_generation") != outbox["control_generation"]):
            raise StoreError("invalid_handoff", "交接的控制回执无效")
        report_row = c.execute("SELECT data FROM documents WHERE kind='verification' AND research_id=? AND json_extract(data,'$.operation_id')=?",
            (rid, outbox["id"])).fetchone()
        if not report_row:
            raise StoreError("invalid_handoff", "交接缺少有效验证正文")
        report = json.loads(report_row[0])
        expected = [{"type": "group", "id": "boss"}] if payload["route"] == "boss" else [
            {"type": "group", "id": f"{cycle['line_id']}/exploration"}, {"type": "group", "id": f"{cycle['line_id']}/critic"}]
        if sorted(map(canonical, outbox.get("recipients", []))) != sorted(map(canonical, expected)):
            raise StoreError("invalid_handoff_recipients", "验证交接收件人与确定性路由不匹配")
        return cycle, line, report

    def _cancel_outbox(self, outbox_id, reason):
        with self.connect(True) as c:
            outbox = self._get(c, "outbox", outbox_id)
            if outbox and outbox.get("status") == "pending":
                self._put(c, "outbox", outbox_id, {**outbox, "status": "cancelled", "cancel_reason": reason,
                    "cancelled_at": now()}, outbox["research_id"])
                self._emit(c, outbox["research_id"], "handoff.cancelled", {"outbox_id": outbox_id, "reason": reason})

    def _defer_outbox(self, outbox_id, reason):
        with self.connect(True) as c:
            outbox = self._get(c, "outbox", outbox_id)
            if outbox and outbox.get("status") == "pending":
                self._put(c, "outbox", outbox_id, {**outbox, "last_dispatch_error": reason}, outbox["research_id"])

    def dispatch_outbox(self, control, limit=100, research_id=None):
        """At-least-once Verify delivery with idempotent durable recipient inboxes.

        The message is committed first. Its acknowledgement, all inbox receipts,
        and the deterministic Boss task share one transaction. Replaying a crash
        between these commits reuses the message's original source identity.
        """
        limit = max(1, min(int(limit), 1000))
        with self.connect() as c:
            sql = "SELECT data,rowid,ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY COALESCE(json_extract(data,'$.dispatch_attempted_at'),''),rowid) AS project_rank FROM documents WHERE kind='outbox' AND json_extract(data,'$.status')='pending'"
            args = []
            if research_id is not None:
                sql += " AND research_id=?"
                args.append(research_id)
            # Interleave projects and rotate attempts so an unavailable project's
            # old messages cannot monopolize every bounded Worker pass.
            candidates = [json.loads(row[0]) for row in c.execute("SELECT data FROM (" + sql +
                ") ORDER BY project_rank,COALESCE(json_extract(data,'$.dispatch_attempted_at'),''),rowid LIMIT ?", [*args, limit])]
        result = {"delivered": [], "cancelled": [], "pending": []}
        for candidate in candidates:
            outbox_id, rid = candidate["id"], candidate["research_id"]
            try:
                with self.connect(True) as c:
                    current = self._get(c, "outbox", outbox_id)
                    if not current or current.get("status") != "pending":
                        continue
                    candidate = self._put(c, "outbox", outbox_id, {**current, "dispatch_attempted_at": now(),
                        "dispatch_attempts": current.get("dispatch_attempts", 0)+1}, rid)
                with self.connect() as c:
                    cycle, line, report = self._verification_handoff(c, candidate)
                gate = self._handoff_gate(control, rid, candidate["control_generation"])
                if gate != "allow":
                    raise _DispatchDeferred(gate, "control_denied" if gate == "cancel" else "control_unavailable")
                line_id, payload = cycle["line_id"], candidate["payload"]
                submission_id = payload["submission_version_id"]
                edge_id = f"{line_id}/verify->boss" if payload["route"] == "boss" else f"{line_id}/verify->{line_id}/exploration"
                message = self.create_message(rid, cycle["id"] + ":feedback", f"{line_id}/verify",
                    report.get("content_markdown") or canonical(report), message_id="handoff-" + hashlib.sha256(outbox_id.encode()).hexdigest()[:32],
                    source_system="verification_dispatch", source_event_id=submission_id,
                    recipients=candidate["recipients"], kind="handoff", authored_by_agent_id=f"{line_id}/verify",
                    cycle_id=cycle["id"], line_id=line_id, edge_id=edge_id, outbox_id=outbox_id,
                    submission_id=submission_id, artifact_refs=[], structured={"transition": payload, "verification": report})
                with self.connect(True) as c:
                    outbox = self._get(c, "outbox", outbox_id)
                    if not outbox or outbox.get("status") != "pending":
                        result["delivered" if outbox and outbox.get("status") == "delivered" else "cancelled"].append(outbox_id)
                        continue
                    cycle, line, report = self._verification_handoff(c, outbox)
                    gate = self._handoff_gate(control, rid, outbox["control_generation"])
                    if gate != "allow":
                        raise _DispatchDeferred(gate, "control_denied" if gate == "cancel" else "control_unavailable")
                    received_at, inbox_ids = now(), []
                    for recipient in outbox["recipients"]:
                        identity = hashlib.sha256(canonical([outbox_id, recipient]).encode()).hexdigest()[:32]
                        inbox_id, delivery_id = "inbox-" + identity, "delivery-" + identity
                        if not self._get(c, "inbox", inbox_id):
                            self._put(c, "inbox", inbox_id, {"inbox_id": inbox_id, "outbox_id": outbox_id,
                                "message_id": message["message_id"], "delivery_id": delivery_id, "recipient": recipient,
                                "recipient_id": recipient["id"], "cycle_id": cycle["id"], "line_id": cycle["line_id"],
                                "direction_epoch": cycle.get("direction_epoch", line["direction_epoch"]),
                                "submission_id": submission_id, "payload": outbox["payload"], "verification": report,
                                "status": "delivered", "received_at": received_at, "control_generation": outbox["control_generation"]}, rid)
                            self._put(c, "message_delivery", delivery_id, {"delivery_id": delivery_id,
                                "message_id": message["message_id"], "inbox_id": inbox_id, "outbox_id": outbox_id,
                                "recipient": recipient, "status": "delivered", "delivered_at": received_at}, rid)
                            self._emit(c, rid, "message.delivered", {"message_id": message["message_id"], "delivery_id": delivery_id,
                                "inbox_id": inbox_id, "recipient": recipient}, line_id=cycle["line_id"], cycle_id=cycle["id"], conversation_id=message["conversation_id"])
                            self._emit(c, rid, "handoff.accepted", {"message_id": message["message_id"], "inbox_id": inbox_id,
                                "outbox_id": outbox_id, "recipient": recipient}, line_id=cycle["line_id"], cycle_id=cycle["id"])
                        inbox_ids.append(inbox_id)
                    # A stop during inbox preparation rolls this whole transaction
                    # back. Control's critical section never waits for SQLite.
                    project = self._project(c, rid)
                    self.assert_progress(project, outbox["control_generation"])
                    if project["status"] == "paused":
                        raise _DispatchDeferred("wait", "research_paused")
                    gate = self._handoff_gate(control, rid, outbox["control_generation"])
                    if gate != "allow":
                        raise _DispatchDeferred(gate, "control_denied" if gate == "cancel" else "control_unavailable")
                    if outbox["payload"]["route"] == "boss":
                        task_id = submission_id + ":boss"
                        task = self._get(c, "task", task_id)
                        task_payload = {"line_id": cycle["line_id"], "cycle_id": cycle["id"], "submission_id": submission_id}
                        if task and (task.get("research_id") != rid or task.get("kind") != "boss" or task.get("payload") != task_payload):
                            raise StoreError("handoff_task_conflict", "Boss 任务身份已经绑定其他提交")
                        if not task:
                            self._put(c, "task", task_id, {"kind": "boss", "payload": task_payload, "status": "pending",
                                "created_at": received_at, "attempt": 0, "outbox_id": outbox_id,
                                "control_generation": outbox["control_generation"]}, rid)
                        if line.get("cycle_id") == cycle["id"] and not line.get("finish_requested"):
                            self._put(c, "line", f"{rid}:{cycle['line_id']}", {**line, "phase": "waiting_boss"}, rid)
                            self._emit(c, rid, "line.phase_changed", {"phase": "waiting_boss", "cycle_id": cycle["id"]}, line_id=cycle["line_id"], cycle_id=cycle["id"])
                    self._put(c, "outbox", outbox_id, {**outbox, "status": "delivered", "delivered_at": received_at,
                        "message_id": message["message_id"], "inbox_ids": inbox_ids, "line_id": cycle["line_id"],
                        "cycle_id": cycle["id"], "submission_id": submission_id}, rid)
                result["delivered"].append(outbox_id)
            except _DispatchDeferred as exc:
                if exc.state == "cancel":
                    self._cancel_outbox(outbox_id, exc.reason)
                else:
                    self._defer_outbox(outbox_id, exc.reason)
                result["cancelled" if exc.state == "cancel" else "pending"].append(outbox_id)
            except StoreError as exc:
                if exc.code not in {"research_stopped", "deadline_reached", "stale_generation", "stale_handoff", "invalid_handoff", "invalid_handoff_recipients"}:
                    self._defer_outbox(outbox_id, exc.code)
                    result["pending"].append(outbox_id)
                else:
                    self._cancel_outbox(outbox_id, exc.code)
                    result["cancelled"].append(outbox_id)
            except Exception as exc:
                # One malformed destination or failed acknowledgement must not
                # prevent delivery to the other projects in this bounded pass.
                self._defer_outbox(outbox_id, type(exc).__name__)
                result["pending"].append(outbox_id)
        return result

    def consume_inbox(self, research_id, recipient_id, consumer_id, cycle_id=None, control=None):
        """Bind received context to a durable graph consumer, preserving retries."""
        if control is None:
            raise StoreError("control_required", "读取交接上下文需要有效控制门禁", 503)
        project = self.project(research_id)
        self.assert_progress(project)
        if project["status"] == "paused" or self._handoff_gate(control, research_id, project["control_generation"]) != "allow":
            raise StoreError("inbox_control_unavailable", "控制门禁暂不可用，交接上下文尚未读取", 503)
        with self.connect(True) as c:
            current = self._project(c, research_id)
            self.assert_progress(current, project["control_generation"])
            if current["status"] == "paused" or self._handoff_gate(control, research_id, project["control_generation"]) != "allow":
                raise StoreError("inbox_control_unavailable", "控制门禁暂不可用，交接上下文尚未读取", 503)
            sql = "SELECT data FROM documents WHERE kind='inbox' AND research_id=? AND json_extract(data,'$.recipient_id')=?"
            args = [research_id, recipient_id]
            if cycle_id is not None:
                sql += " AND json_extract(data,'$.cycle_id')=?"
                args.append(cycle_id)
            entries = [json.loads(row[0]) for row in c.execute(sql + " ORDER BY rowid", args)]
            accepted = []
            for inbox in entries:
                if inbox.get("control_generation") != project["control_generation"] or inbox.get("status") not in {"delivered", "consumed"}:
                    continue
                if inbox.get("consumed_by") not in {None, consumer_id}:
                    continue
                row = c.execute("SELECT data,cursor FROM messages WHERE message_id=? AND research_id=?", (inbox["message_id"], research_id)).fetchone()
                if not row:
                    raise StoreError("inbox_message_missing", "交接消息缺失，不能提供不完整上下文", 409)
                if inbox.get("status") != "consumed":
                    inbox = self._put(c, "inbox", inbox["id"], {**inbox, "status": "consumed", "consumed_by": consumer_id, "consumed_at": now()}, research_id)
                    delivery = self._get(c, "message_delivery", inbox["delivery_id"])
                    self._put(c, "message_delivery", inbox["delivery_id"], {**delivery, "status": "acknowledged",
                        "acknowledged_by": consumer_id, "acknowledged_at": inbox["consumed_at"]}, research_id)
                    self._emit(c, research_id, "handoff.consumed", {"inbox_id": inbox["id"], "message_id": inbox["message_id"],
                        "recipient_id": recipient_id, "consumer_id": consumer_id}, line_id=inbox["line_id"], cycle_id=inbox["cycle_id"])
                accepted.append({**inbox, "message": {**json.loads(row["data"]), "cursor": row["cursor"]}})
            if self._handoff_gate(control, research_id, project["control_generation"]) != "allow":
                raise StoreError("inbox_control_unavailable", "交接上下文读取尚未确认，请在门禁恢复后重试", 503)
            return accepted

    def source_exists(self, research_id, source_id):
        with self.connect() as c:
            return bool(c.execute("SELECT 1 FROM messages WHERE research_id=? AND message_id=? UNION ALL "
                "SELECT 1 FROM documents WHERE research_id=? AND id=? LIMIT 1",
                (research_id, source_id, research_id, source_id)).fetchone())

    def deliver_agent_message(self, context, recipient_ids, content, call_id, control, *, existing_message_id=None, wake=True):
        """One local transaction commits the message, each member's inbox and wake-up jobs."""
        rid, generation = context["research_id"], context["control_generation"]
        recipients = sorted(set(recipient_ids))
        fingerprint = hashlib.sha256(canonical([rid, context["agent_id"], recipients, content]).encode()).hexdigest()
        source_id = context["execution_id"] + ":" + call_id
        with self.connect(True) as c:
            project = self._project(c, rid)
            self.assert_progress(project, generation)
            if project["status"] in {"paused", "pausing", "blocked"} or self._handoff_gate(control, rid, generation) != "allow":
                raise StoreError("communication_control_unavailable", "当前门禁不允许投递消息", 503)
            old = (c.execute("SELECT data FROM messages WHERE message_id=? AND research_id=?", (existing_message_id, rid)).fetchone()
                   if existing_message_id else c.execute("SELECT data FROM messages WHERE source_system='dsh_tool' AND source_event_id=?", (source_id,)).fetchone())
            if old:
                message = json.loads(old[0])
                if message["research_id"] != rid:
                    raise StoreError("cross_project_reference", "消息不属于当前研究", 403)
                if message.get("delivery_hash") and message["delivery_hash"] != fingerprint:
                    raise StoreError("idempotency_conflict", "同一消息调用 ID 不能发送不同内容或收件人")
                if message.get("delivery_hash"):
                    return {"message_id": message["message_id"], "delivery_status": "delivered", "inbox_ids": message["inbox_ids"]}
                if message["content_markdown"] != content or message["sender"]["id"] != context["agent_id"]:
                    raise StoreError("message_delivery_conflict", "只能投递当前成员的真实完整消息")
            elif existing_message_id:
                raise StoreError("inbox_message_missing", "待投递的回复正文不存在", 409)
            topology = self._get(c, "topology", rid)
            nodes = {n["id"]: n for n in topology["nodes"]}
            if any(r not in nodes for r in recipients):
                raise StoreError("unknown_recipient", "消息接收者未登记", 403)
            source_line = context.get("line_id")
            if source_line and context.get("direction_epoch") is not None:
                line = self._get(c, "line", f"{rid}:{source_line}")
                if line["direction_epoch"] != context["direction_epoch"]:
                    raise StoreError("stale_direction", "发送方研究方向代次已失效")
            message_id = message["message_id"] if old else new_id("msg")
            addresses = [{"type": nodes[r]["type"], "id": r} for r in recipients]
            targets = {}
            for recipient in recipients:
                members = [n["id"] for n in topology["nodes"] if n.get("parentId") == recipient] if nodes[recipient]["type"] == "group" else [recipient]
                if not members:
                    raise StoreError("recipient_absent", "接收组没有可投递的已登记成员")
                targets[recipient] = members
            member_ids = sorted({m for members in targets.values() for m in members})
            inbox_ids = {m: "inbox-" + hashlib.sha256(canonical([message_id, m]).encode()).hexdigest()[:32] for m in member_ids}
            if old:
                message.update(recipients=addresses, delivery_status="delivered", delivery_hash=fingerprint, inbox_ids=list(inbox_ids.values()))
                c.execute("UPDATE messages SET data=? WHERE message_id=?", (canonical(message), message_id))
            else:
                message = self._create_message(c, rid, context["conversation_id"], context["agent_id"], content,
                    message_id=message_id, source_system="dsh_tool", source_event_id=source_id, recipients=addresses,
                    line_id=source_line, direction_epoch=context.get("direction_epoch"), kind="communication", delivery_status="delivered", delivery_hash=fingerprint,
                    inbox_ids=list(inbox_ids.values()), task_id=context["task_id"])
            for member, inbox_id in inbox_ids.items():
                delivery_id = "delivery-" + inbox_id.removeprefix("inbox-")
                node = nodes[member]
                line_id = node["data"].get("line_id")
                line = self._get(c, "line", f"{rid}:{line_id}") if line_id else None
                self._put(c, "inbox", inbox_id, {"inbox_id": inbox_id, "message_id": message_id, "delivery_id": delivery_id,
                    "recipient_id": member, "recipient": {"type": "agent", "id": member}, "message_kind": "communication",
                    "cycle_id": None, "line_id": line_id,
                    "direction_epoch": context.get("recipient_direction_epochs", {}).get(member, line["direction_epoch"] if line else None),
                    "control_generation": generation, "status": "delivered", "received_at": now()}, rid)
                self._put(c, "message_delivery", delivery_id, {"delivery_id": delivery_id, "message_id": message_id,
                    "inbox_id": inbox_id, "recipient": {"type": "agent", "id": member}, "status": "delivered", "delivered_at": now()}, rid)
                self._emit(c, rid, "message.delivered", {"message_id": message_id, "delivery_id": delivery_id,
                    "inbox_id": inbox_id, "recipient": {"type": "agent", "id": member}}, conversation_id=context["conversation_id"])
            for recipient, members in targets.items():
                if not wake:
                    continue  # A reply is delivered as context without an automatic reply-to-reply loop.
                task_id = "communication-" + hashlib.sha256(canonical([message_id, recipient]).encode()).hexdigest()[:32]
                self._put(c, "task", task_id, {"kind": "communication", "status": "pending", "attempt": 0, "created_at": now(),
                    "control_generation": generation, "payload": {"recipient_id": recipient, "message_id": message_id,
                    "inbox_ids": [inbox_ids[m] for m in members]}}, rid)
            if self._handoff_gate(control, rid, generation) != "allow":
                raise StoreError("communication_control_unavailable", "投递期间门禁已关闭，消息尚未提交", 503)
            return {"message_id": message_id, "delivery_status": "delivered", "inbox_ids": list(inbox_ids.values())}

    def consume_agent_inbox(self, research_id, agent_id, consumer_id, control, direction_epoch=None):
        """Freeze exactly which messages entered a logical model turn, including replay."""
        batch_id = "agent-inbox-" + hashlib.sha256(canonical([research_id, agent_id, consumer_id]).encode()).hexdigest()
        with self.connect(True) as c:
            project = self._project(c, research_id)
            self.assert_progress(project)
            if self._handoff_gate(control, research_id, project["control_generation"]) != "allow":
                raise StoreError("inbox_control_unavailable", "无法确认读取收件箱的控制门禁", 503)
            old = self._get(c, "agent_inbox_batch", batch_id)
            if old:
                return old["items"]
            rows = c.execute("SELECT data FROM documents WHERE kind='inbox' AND research_id=? "
                "AND json_extract(data,'$.recipient_id')=? AND json_extract(data,'$.message_kind')='communication' "
                "AND json_extract(data,'$.status')='delivered' ORDER BY rowid", (research_id, agent_id)).fetchall()
            accepted = []
            for row in rows:
                inbox = json.loads(row[0])
                if inbox["control_generation"] != project["control_generation"]:
                    continue
                if inbox.get("direction_epoch") is not None and inbox["direction_epoch"] != direction_epoch:
                    continue
                message = c.execute("SELECT data FROM messages WHERE research_id=? AND message_id=?", (research_id, inbox["message_id"])).fetchone()
                if not message:
                    raise StoreError("inbox_message_missing", "接收消息正文缺失", 409)
                consumed = self._put(c, "inbox", inbox["id"], {**inbox, "status": "consumed", "consumed_by": consumer_id, "consumed_at": now()}, research_id)
                delivery = self._get(c, "message_delivery", inbox["delivery_id"])
                self._put(c, "message_delivery", inbox["delivery_id"], {**delivery, "status": "acknowledged",
                    "acknowledged_by": consumer_id, "acknowledged_at": consumed["consumed_at"]}, research_id)
                self._emit(c, research_id, "message.acknowledged", {"message_id": inbox["message_id"], "inbox_id": inbox["id"],
                    "recipient_id": agent_id, "consumer_id": consumer_id}, line_id=inbox.get("line_id"))
                accepted.append({**consumed, "message": json.loads(message[0])})
            self._put(c, "agent_inbox_batch", batch_id, {"agent_id": agent_id, "consumer_id": consumer_id, "items": accepted}, research_id)
            if self._handoff_gate(control, research_id, project["control_generation"]) != "allow":
                raise StoreError("inbox_control_unavailable", "读取期间门禁已关闭，消费尚未提交", 503)
            return accepted

    def assert_progress(self, project, generation=None):
        if project["status"] in TERMINAL:
            raise StoreError("research_stopped", "研究已停止，不能继续推进")
        if project.get("deadline_at") and utcnow() >= datetime.fromisoformat(project["deadline_at"].replace("Z", "+00:00")):
            raise StoreError("deadline_reached", "研究总运行时间已到期")
        if generation is not None and generation != project["control_generation"]:
            raise StoreError("stale_generation", "控制代次已失效")

    def _operations(self, c, research_id):
        return [{"id": r["id"], "kind": r["kind"], "status": r["status"]} for r in c.execute(
            "SELECT id,kind,status FROM business_operations WHERE research_id=?", (research_id,))]

    def _stage(self, c, research_id, kind, unique_key, payload):
        existing = c.execute("SELECT * FROM business_operations WHERE unique_key=?", (unique_key,)).fetchone()
        if existing:
            if existing["content_hash"] != hashlib.sha256(canonical(payload).encode()).hexdigest():
                raise StoreError("idempotency_conflict", "同一业务操作不能提交不同内容")
            return dict(existing)
        operation = {"id": new_id("operation"), "research_id": research_id, "kind": kind,
            "unique_key": unique_key, "content_hash": hashlib.sha256(canonical(payload).encode()).hexdigest(),
            "payload": canonical(payload), "status": "pending_control_confirmation", "receipt": None, "result": None}
        c.execute("INSERT INTO business_operations VALUES(:id,:research_id,:kind,:unique_key,:content_hash,:payload,:status,:receipt,:result)", operation)
        return operation

    def _confirm(self, operation, generation, control):
        try:
            receipt = control.confirm_operation(operation["research_id"], operation["id"], operation["content_hash"], generation)
        except Exception:
            return None
        if not receipt or not receipt.get("persisted", receipt.get("receipt_persisted", False)):
            return None
        if (receipt.get("operation_id") != operation["id"] or receipt.get("content_hash") != operation["content_hash"]
                or receipt.get("research_id") != operation["research_id"] or not receipt.get("control_epoch")
                or not isinstance(receipt.get("control_seq"), int) or receipt["control_seq"] < 1):
            raise StoreError("invalid_control_receipt", "控制确认回执与候选不匹配")
        return receipt

    def recover_confirmed_operations(self, control):
        """Replay durable receipts without creating confirmations or model work.

        In particular, a stop must not discard a report that was durably confirmed
        before that stop merely because its business projection could not commit.
        Unconfirmed candidates remain diagnostic history and never affect counters.
        """
        with self.connect() as c:
            operations = [dict(row) for row in c.execute(
                "SELECT * FROM business_operations WHERE status='pending_control_confirmation' ORDER BY rowid")]
        result = {"applied": [], "pending": [], "conflicts": []}
        for operation in operations:
            try:
                receipt = control.get_confirmation(operation["id"])
            except Exception:
                result["pending"].append(operation["id"])
                continue
            class ReceiptOnly:
                def confirm_operation(self, *_args, **_kwargs):
                    return receipt

                def allows_progress(self, *args, **kwargs):
                    return control.allows_progress(*args, **kwargs)
            receipt = self._confirm(operation, 0, ReceiptOnly())
            if not receipt:
                result["pending"].append(operation["id"])
                continue
            payload = json.loads(operation["payload"])
            rid, kind = operation["research_id"], operation["kind"]
            if kind == "boss_effects":
                try:
                    applied = self.apply_boss_effects(rid, payload["decision_task_id"], control=ReceiptOnly())
                    result["applied" if applied.get("status") == "applied" else "pending"].append(operation["id"])
                except StoreError as exc:
                    if exc.code != "line_version_conflict":
                        raise
                    with self.connect(True) as c:
                        c.execute("UPDATE business_operations SET status='recovery_conflict',receipt=? WHERE id=?", (canonical(receipt), operation["id"]))
                    result["conflicts"].append(operation["id"])
                continue
            try:
                allowed = bool(control.allows_progress(rid, receipt.get("control_generation")))
            except Exception:
                allowed = False
            with self.connect(True) as c:
                current = c.execute("SELECT status FROM business_operations WHERE id=?", (operation["id"],)).fetchone()
                if current["status"] != "pending_control_confirmation":
                    continue
                project = self._project(c, rid)
                try:
                    self.assert_progress(project, receipt.get("control_generation"))
                except StoreError:
                    allowed = False
                projection = None
                if kind == "verification":
                    cycle = self._get(c, "cycle", payload["cycle_id"])
                    if cycle is None or cycle.get("completed_verify_rounds", 0) != payload["base_round"]:
                        c.execute("UPDATE business_operations SET status='recovery_conflict',receipt=? WHERE id=?", (canonical(receipt), operation["id"]))
                        result["conflicts"].append(operation["id"])
                        continue
                    report, round_no = payload["report"], payload["base_round"] + 1
                    route = "boss" if round_no >= cycle["k"] or report["passed"] else "exploration_and_critic"
                    projection = {"operation_id": operation["id"], "route": route, "round_no": round_no,
                        "reason": "periodic_report" if round_no >= cycle["k"] else "verification_passed" if report["passed"] else "revision_required",
                        "passed": report["passed"], "cycle_id": payload["cycle_id"], "submission_version_id": payload["submission_version_id"]}
                    self._put(c, "cycle", payload["cycle_id"], {**cycle, "completed_verify_rounds": round_no,
                        "status": ("waiting_boss" if route == "boss" else "active") if allowed else "stopped"}, rid)
                    self._put(c, "verification", operation["id"], {**report, **projection, "receipt": receipt}, rid)
                    recipients = [{"type": "group", "id": "boss"}] if route == "boss" else [
                        {"type": "group", "id": f"{cycle['line_id']}/exploration"}, {"type": "group", "id": f"{cycle['line_id']}/critic"}]
                    self._put(c, "outbox", operation["id"], {"status": "pending" if allowed else "cancelled", "recipients": recipients,
                        "payload": projection, "control_generation": payload["generation"]}, rid)
                    self._emit(c, rid, "verification.completed", {**projection, "historical": not allowed}, line_id=cycle.get("line_id"), cycle_id=payload["cycle_id"])
                elif kind == "boss_decision":
                    decision_id = operation["unique_key"][len("boss:"):]
                    projection = self._put(c, "boss_decision", decision_id, {**payload, "decision_id": decision_id,
                        "receipt": receipt, "status": "applied"}, rid)
                    self._emit(c, rid, "boss.decision_created", {**projection, "historical": not allowed})
                elif kind == "memory":
                    if project.get("memory_version", 0) != payload["base_memory_version"]:
                        c.execute("UPDATE business_operations SET status='recovery_conflict',receipt=? WHERE id=?", (canonical(receipt), operation["id"]))
                        result["conflicts"].append(operation["id"])
                        continue
                    version = payload["base_memory_version"] + 1
                    for index, update in enumerate(payload["updates"]):
                        node_id = update.get("node_id") or f"{operation['id']}:{index}"
                        value = {**update, "node_id": node_id, "memory_version": version, "boss_decision_id": payload["boss_decision_id"],
                            "created_at": receipt.get("confirmed_at", now()), "receipt": receipt}
                        self._put(c, "memory_node", f"{rid}:{node_id}", value, rid)
                        self._put(c, "memory_version", f"{rid}:{node_id}:{version}", value, rid)
                    self._update_project(c, rid, memory_version=version)
                    projection = {"memory_version": version, "status": "applied"}
                    self._emit(c, rid, "memory.version_created", {**projection, "historical": not allowed})
                elif kind == "report":
                    report_id = operation["unique_key"][len("report:"):]
                    projection = self._put(c, "report", report_id, {**payload, "receipt": receipt, "status": "completed"}, rid)
                    self._emit(c, rid, "report.completed", {"report_id": report_id, "artifact_id": payload.get("artifact_id"), "historical": not allowed})
                if projection is not None:
                    c.execute("UPDATE business_operations SET status='applied',receipt=?,result=? WHERE id=?",
                              (canonical(receipt), canonical(projection), operation["id"]))
                    result["applied"].append(operation["id"])
                else:
                    result["pending"].append(operation["id"])
        return result

    def commit_verification(self, cycle_id, submission_version_id, report, execution_generation, control):
        with self.connect(True) as c:
            cycle = self._get(c, "cycle", cycle_id)
            submission = self._get(c, "submission", submission_version_id)
            if not cycle or not submission:
                raise StoreError("verification_input_missing", "验证周期或提交包不存在", 422)
            research_id = cycle["research_id"]
            if submission["research_id"] != research_id or submission.get("cycle_id") != cycle_id:
                raise StoreError("cross_project_reference", "送验包与周期不匹配", 403)
            existing = c.execute("SELECT * FROM business_operations WHERE unique_key=?", (f"verify:{cycle_id}:{submission_version_id}",)).fetchone()
            if existing and existing["status"] == "applied":
                return json.loads(existing["result"])
            p = self._project(c, research_id)
            try:
                self.assert_progress(p, execution_generation)
            except StoreError:
                self._put(c, "verification_archive", new_id("late"), {"report": report, "status": "verification_pending"}, research_id)
                return {"route": "halted"}
            if type(report.get("passed")) is not bool or not report.get("checks"):
                raise StoreError("invalid_verification", "验证报告需要通过状态与检查依据", 422)
            if cycle.get("status", "active") not in {"active", "verifying"}:
                raise StoreError("cycle_inactive", "当前周期不能接受验证")
            for field in ("spec_id", "spec_version", "code_version", "run_ids"):
                if field in submission and report.get(field) != submission[field]:
                    raise StoreError("verification_binding_mismatch", f"验证输入 {field} 与提交包不匹配", 422)
            count = cycle.get("completed_verify_rounds", 0)
            if count >= cycle["k"]:
                raise StoreError("cycle_waiting_boss", "已到周期汇报轮次，须等待 Boss")
            operation = self._stage(c, research_id, "verification", f"verify:{cycle_id}:{submission_version_id}",
                {"cycle_id": cycle_id, "submission_version_id": submission_version_id, "report": report,
                 "base_round": count, "generation": execution_generation})
        receipt = self._confirm(operation, execution_generation, control)
        if not receipt:
            return {"route": "awaiting_confirmation_sync", "operation_id": operation["id"]}
        with self.connect(True) as c:
            op = c.execute("SELECT * FROM business_operations WHERE id=?", (operation["id"],)).fetchone()
            if op["status"] == "applied":
                return json.loads(op["result"])
            cycle = self._get(c, "cycle", cycle_id)
            if cycle.get("completed_verify_rounds", 0) != count:
                raise StoreError("cycle_version_conflict", "验证周期版本已变化")
            round_no = count + 1
            route = "boss" if round_no >= cycle["k"] or report["passed"] else "exploration_and_critic"
            result = {"operation_id": operation["id"], "route": route, "round_no": round_no,
                "reason": "periodic_report" if round_no >= cycle["k"] else "verification_passed" if report["passed"] else "revision_required",
                "passed": report["passed"], "cycle_id": cycle_id, "submission_version_id": submission_version_id}
            self._put(c, "cycle", cycle_id, {**cycle, "completed_verify_rounds": round_no,
                "status": "waiting_boss" if route == "boss" else "active"}, research_id)
            report_id = new_id("verification")
            self._put(c, "verification", report_id, {**report, **result, "receipt": receipt}, research_id)
            c.execute("UPDATE business_operations SET status='applied',receipt=?,result=? WHERE id=?",
                      (canonical(receipt), canonical(result), operation["id"]))
            self._emit(c, research_id, "verification.completed", result, line_id=cycle.get("line_id"), cycle_id=cycle_id)
            recipients = [{"type": "group", "id": "boss"}] if route == "boss" else [
                {"type": "group", "id": f"{cycle['line_id']}/exploration"}, {"type": "group", "id": f"{cycle['line_id']}/critic"}]
            self._put(c, "outbox", operation["id"], {"status": "pending", "recipients": recipients,
                "payload": result, "control_generation": execution_generation}, research_id)
            return result

    def commit_boss_decision(self, research_id, decision_task_id, decision, control):
        p = self.project(research_id)
        with self.connect(True) as c:
            self.assert_progress(self._project(c, research_id), p["control_generation"])
            op = self._stage(c, research_id, "boss_decision", f"boss:{decision_task_id}", decision)
            if op["status"] == "applied":
                return json.loads(op["result"])
        receipt = self._confirm(op, p["control_generation"], control)
        if not receipt:
            return {"status": "awaiting_confirmation_sync", "id": op["id"]}
        with self.connect(True) as c:
            existing = self._get(c, "boss_decision", decision_task_id)
            if existing:
                return existing
            result = self._put(c, "boss_decision", decision_task_id, {**decision,
                "decision_id": decision_task_id, "receipt": receipt, "status": "applied"}, research_id)
            c.execute("UPDATE business_operations SET status='applied',receipt=?,result=? WHERE id=?",
                      (canonical(receipt), canonical(result), op["id"]))
            self._emit(c, research_id, "boss.decision_created", result)
            return result

    def write_memory(self, research_id, boss_decision_id, update_batch_id, updates, base_memory_version, control, role="boss"):
        if role != "boss":
            raise StoreError("boss_only_memory", "只有 Boss 决议可以维护共享记忆", 403)
        decision = self.get("boss_decision", boss_decision_id)
        if not decision or decision.get("research_id") != research_id or decision.get("status") != "applied" or not decision.get("receipt"):
            raise StoreError("boss_decision_required", "记忆写入需要有效的 Boss 决议", 403)
        with self.connect() as c:
            prior = c.execute("SELECT payload,status,result FROM business_operations WHERE unique_key=?",
                              (f"memory:{boss_decision_id}:{update_batch_id}",)).fetchone()
            if prior and prior["status"] == "applied":
                if json.loads(prior["payload"])["updates"] != updates:
                    raise StoreError("idempotency_conflict", "已应用记忆批次的内容不能变化")
                return json.loads(prior["result"])
        p = self.project(research_id)
        payload = {"updates": updates, "base_memory_version": base_memory_version, "boss_decision_id": boss_decision_id}
        with self.connect(True) as c:
            self.assert_progress(self._project(c, research_id), p["control_generation"])
            op = self._stage(c, research_id, "memory", f"memory:{boss_decision_id}:{update_batch_id}", payload)
            if op["status"] == "applied":
                return json.loads(op["result"])
        receipt = self._confirm(op, p["control_generation"], control)
        if not receipt:
            return {"status": "awaiting_confirmation_sync"}
        with self.connect(True) as c:
            op_state = c.execute("SELECT status,result FROM business_operations WHERE id=?", (op["id"],)).fetchone()
            if op_state["status"] == "applied":
                return json.loads(op_state["result"])
            p = self._project(c, research_id)
            if p.get("memory_version", 0) != base_memory_version:
                raise StoreError("memory_version_conflict", "共享记忆已更新，请读取后合并")
            version = base_memory_version + 1
            for update in updates:
                node_id = update.get("node_id") or new_id("memory")
                value = {**update, "node_id": node_id, "memory_version": version, "boss_decision_id": boss_decision_id,
                         "created_at": now(), "receipt": receipt}
                self._put(c, "memory_node", f"{research_id}:{node_id}", value, research_id)
                self._put(c, "memory_version", f"{research_id}:{node_id}:{version}", value, research_id)
            self._update_project(c, research_id, memory_version=version)
            result = {"memory_version": version, "status": "applied"}
            c.execute("UPDATE business_operations SET status='applied',receipt=?,result=? WHERE id=?",
                      (canonical(receipt), canonical(result), op["id"]))
            self._emit(c, research_id, "memory.version_created", result)
            return result

    def apply_boss_effects(self, research_id, decision_task_id, line_id=None, snapshot=None, control=None):
        """Confirm and materialize one decision's downstream work exactly once.

        The candidate freezes epochs before control confirmation. Its projection,
        line updates, tasks, report snapshot and marker share one short transaction.
        A receipt confirmed before stopping may still be recorded as history, but
        its tasks become cancelled and cannot reopen a stopped project's lines.
        """
        key = f"boss_effects:{decision_task_id}"
        with self.connect(True) as c:
            marker = self._get(c, "boss_effects", decision_task_id)
            if marker:
                if marker["research_id"] != research_id:
                    raise StoreError("cross_project_reference", "Boss 决议不属于当前研究", 403)
                return marker
            decision = self._get(c, "boss_decision", decision_task_id)
            if not decision or decision.get("research_id") != research_id or decision.get("status") != "applied" or not decision.get("receipt"):
                raise StoreError("boss_decision_required", "任务下发需要有效的 Boss 决议", 403)
            previous = c.execute("SELECT * FROM business_operations WHERE unique_key=?", (key,)).fetchone()
            project = self._project(c, research_id)
            if previous:
                operation = dict(previous)
                candidate = json.loads(operation["payload"])
            else:
                self.assert_progress(project)
                assignments = decision.get("assignments", [])
                action = decision["action"]
                if action in {"assign", "continue", "request_evidence", "change_direction"} and project.get("finish_requested"):
                    raise StoreError("research_finished", "Boss 已结束研究，不能再次下发新周期")
                if action in {"continue", "request_evidence", "change_direction"} and not assignments and line_id:
                    assignments = [{"line_id": line_id, "objective": decision.get("rationale", ""), "priority": 0}]
                targets = []
                seen = set()
                for assignment in assignments if action in {"assign", "continue", "request_evidence", "change_direction"} else []:
                    identity = assignment["line_id"]
                    current = self._get(c, "line", f"{research_id}:{identity}")
                    if not current or identity in seen:
                        raise StoreError("invalid_boss_assignment", "Boss 任务包含未知或重复研究线", 422)
                    if current.get("finish_requested"):
                        raise StoreError("line_finished", "Boss 已结束该方向，不能再次下发新周期")
                    seen.add(identity)
                    targets.append({**assignment, "original_epoch": current["direction_epoch"],
                        "direction_epoch": current["direction_epoch"] + (1 if action == "change_direction" else 0),
                        "old_cycle_id": current.get("cycle_id"), "k": current["k"]})
                ending = []
                if action in {"finish_line", "finish_project"}:
                    ending = [json.loads(row[0])["line_id"] for row in c.execute(
                        "SELECT data FROM documents WHERE kind='line' AND research_id=?", (research_id,))
                        if action == "finish_project" or json.loads(row[0])["line_id"] == line_id]
                    if not ending:
                        raise StoreError("invalid_boss_assignment", "结束方向需要有效的研究线", 422)
                candidate = {"decision_task_id": decision_task_id, "generation": project["control_generation"],
                    "targets": targets, "ending": ending, "action": action, "deadline_at": project.get("deadline_at"),
                    "report_snapshot": snapshot if snapshot is not None else {"decision": decision},
                    "request_report": bool(decision.get("request_report")), "created_at": now()}
                operation = self._stage(c, research_id, "boss_effects", key, candidate)
        if control is None:
            raise StoreError("control_confirmation_required", "新周期与报告任务需要独立控制确认", 503)
        receipt = self._confirm(operation, candidate["generation"], control)
        if not receipt:
            return {"status": "awaiting_confirmation_sync", "operation_id": operation["id"]}
        try:
            dispatch_allowed = bool(control.allows_progress(research_id, candidate["generation"]))
        except Exception:
            dispatch_allowed = False
        with self.connect(True) as c:
            marker = self._get(c, "boss_effects", decision_task_id)
            if marker:
                return marker
            project = self._project(c, research_id)
            try:
                self.assert_progress(project, candidate["generation"])
            except StoreError:
                dispatch_allowed = False
            created_tasks, cycles = [], []
            def task(task_id, kind, payload):
                existing = self._get(c, "task", task_id)
                if not existing:
                    self._put(c, "task", task_id, {"kind": kind, "payload": payload,
                        "status": "pending" if dispatch_allowed else "cancelled", "created_at": candidate["created_at"],
                        "attempt": 0, "control_generation": candidate["generation"], "boss_decision_id": decision_task_id}, research_id)
                created_tasks.append(task_id)
            for target in candidate["targets"]:
                identity = target["line_id"]
                cycle_id = f"{decision_task_id}:{identity}:cycle"
                line_key = f"{research_id}:{identity}"
                current = self._get(c, "line", line_key)
                cycle = self._get(c, "cycle", cycle_id)
                if not cycle:
                    if current["direction_epoch"] != target["original_epoch"] or current.get("cycle_id") != target.get("old_cycle_id"):
                        raise StoreError("line_version_conflict", "研究周期或方向代次已变化，不能重放旧 Boss 任务")
                    cycle = self._put(c, "cycle", cycle_id, {"line_id": identity, "direction_epoch": target["direction_epoch"],
                        "k": target["k"], "completed_verify_rounds": 0, "status": "active" if dispatch_allowed else "archived",
                        "objective": target["objective"], "deadline_at": candidate["deadline_at"], "boss_decision_id": decision_task_id}, research_id)
                    if dispatch_allowed:
                        old_cycle = self._get(c, "cycle", target.get("old_cycle_id")) if target.get("old_cycle_id") else None
                        if old_cycle:
                            self._put(c, "cycle", target["old_cycle_id"], {**old_cycle, "status": "closed", "closed_at": now()}, research_id)
                        self._put(c, "line", line_key, {**current, "direction_epoch": target["direction_epoch"],
                            "cycle_id": cycle_id, "completed_verify_rounds": 0, "phase": "exploration", "objective": target["objective"],
                            "scientific_end_requested": False, "boss_decision_id": decision_task_id}, research_id)
                        self._emit(c, research_id, "line.phase_changed", {"phase": "exploration", "direction_epoch": target["direction_epoch"],
                            "cycle_id": cycle_id, "completed_verify_rounds": 0}, line_id=identity, cycle_id=cycle_id)
                task(cycle_id + ":line", "line", {"line_id": identity, "cycle_id": cycle_id, "direction_epoch": cycle["direction_epoch"]})
                cycles.append(cycle_id)
            if candidate["request_report"]:
                snapshot_id = decision_task_id + ":report"
                frozen = candidate["report_snapshot"]
                evidence = [frozen["submission"]["id"]] if isinstance(frozen.get("submission"), dict) and frozen["submission"].get("id") else []
                self._put(c, "report_snapshot", snapshot_id, {"snapshot": frozen, "evidence_ids": evidence}, research_id)
                task(snapshot_id, "paper", {"snapshot_id": snapshot_id})
            if dispatch_allowed:
                for identity in candidate["ending"]:
                    current = self._get(c, "line", f"{research_id}:{identity}")
                    self._put(c, "line", f"{research_id}:{identity}", {**current, "phase": "completed",
                        "finish_requested": True, "scientific_end_requested": True, "boss_decision_id": decision_task_id}, research_id)
                    self._emit(c, research_id, "line.phase_changed", {"phase": "completed", "direction_epoch": current["direction_epoch"],
                        "cycle_id": current.get("cycle_id"), "finish_requested": True}, line_id=identity, cycle_id=current.get("cycle_id"))
                if candidate["action"] == "finish_project":
                    self._update_project(c, research_id, finish_requested=True, scientific_end_requested=True)
            result = self._put(c, "boss_effects", decision_task_id, {"status": "applied", "operation_id": operation["id"],
                "task_ids": created_tasks, "cycle_ids": cycles, "finish_line_ids": candidate["ending"],
                "dispatch_allowed": dispatch_allowed, "receipt": receipt}, research_id)
            c.execute("UPDATE business_operations SET status='applied',receipt=?,result=? WHERE id=?",
                      (canonical(receipt), canonical(result), operation["id"]))
            self._emit(c, research_id, "boss.tasks_materialized", {"decision_id": decision_task_id,
                "cycle_ids": cycles, "task_ids": created_tasks, "historical": not dispatch_allowed})
            return result

    def commit_report(self, research_id, report_id, report, control):
        with self.connect(True) as c:
            existing = self._get(c, "report", report_id)
            if existing:
                if existing.get("research_id") != research_id:
                    raise StoreError("cross_project_reference", "报告不属于当前研究", 403)
                return existing
            project = self._project(c, research_id)
            self.assert_progress(project)
            operation = self._stage(c, research_id, "report", f"report:{report_id}", report)
            generation = project["control_generation"]
        receipt = self._confirm(operation, generation, control)
        if not receipt:
            return {"status": "awaiting_confirmation_sync", "operation_id": operation["id"]}
        with self.connect(True) as c:
            existing = self._get(c, "report", report_id)
            if existing:
                return existing
            result = self._put(c, "report", report_id, {**report, "receipt": receipt, "status": "completed"}, research_id)
            c.execute("UPDATE business_operations SET status='applied',receipt=?,result=? WHERE id=?",
                      (canonical(receipt), canonical(result), operation["id"]))
            self._emit(c, research_id, "report.completed", {"report_id": report_id, "artifact_id": report.get("artifact_id")})
            return result

    def sync_stop(self, research_id, status):
        stop_id = status.get("stop_id")
        if not stop_id:
            return False
        with self.connect(True) as c:
            existing = c.execute("SELECT 1 FROM stop_sync WHERE research_id=? AND stop_id=?", (research_id, stop_id)).fetchone()
            p = self._project(c, research_id)
            if not existing:
                c.execute("INSERT INTO stop_sync VALUES(?,?)", (research_id, stop_id))
                p = self._update_project(c, research_id, status="terminating", stop_id=stop_id,
                    stop_reason=status.get("reason", "user_requested"), stop_requested_at=status.get("stop_requested_at", status.get("requested_at", now())),
                    control_generation=p["control_generation"]+1)
                self._emit(c, research_id, "research.terminating", {"stop_id": stop_id, "reason": p["stop_reason"]})
                for kind in ("task", "outbox"):
                    for row in c.execute("SELECT id,data FROM documents WHERE kind=? AND research_id=?", (kind, research_id)).fetchall():
                        value = json.loads(row["data"])
                        if value.get("status") in {"pending", "running"}:
                            self._put(c, kind, row["id"], {**value, "status": "cancelled"}, research_id)
            # Resource allocations must be reconciled before reporting full termination.
            active = [json.loads(r[0]) for r in c.execute("SELECT data FROM documents WHERE kind='allocation' AND research_id=?", (research_id,))]
            light_jobs = [json.loads(r[0]) for r in c.execute("SELECT data FROM documents WHERE kind='light_cpu' AND research_id=?", (research_id,))]
            for message_row in c.execute("SELECT * FROM messages WHERE research_id=? AND json_extract(data,'$.streaming_message')=1 AND json_extract(data,'$.status')='streaming'", (research_id,)).fetchall():
                message = json.loads(message_row["data"])
                self._complete_stream(c, message_row, message["content_markdown"], "interrupted", message_row["source_system"], None, {})
            pending = status.get("unfinished", status.get("pending", status.get("active_executions", [])))
            execution_stopped = status.get("execution_stopped", status.get("stopped", False) and not pending)
            if (p["status"] != "terminated" and not pending and execution_stopped and all(a.get("state") in {"released", "expired"} for a in active)
                    and all(job.get("state") in {"completed", "execution_failed", "cancelled", "interrupted"} for job in light_jobs)):
                terminated_at = now()
                self._update_project(c, research_id, status="terminated", terminated_at=terminated_at)
                self._emit(c, research_id, "research.terminated", {"stop_id": stop_id,
                    "reason": p.get("stop_reason", status.get("reason", "user_requested")), "terminated_at": terminated_at})
            return True

    def register_artifact(self, research_id, path, media_type="application/octet-stream", **fields):
        file_path = Path(path).resolve()
        if not file_path.is_relative_to(self.data_dir) or not file_path.is_file():
            raise StoreError("unsafe_artifact_path", "产物必须位于研究数据目录内", 422)
        if file_path.is_relative_to(self.data_dir / "credentials") or file_path.is_relative_to(self.data_dir / "control") or file_path.suffix in {".sqlite", ".dpapi"}:
            raise StoreError("protected_artifact_path", "凭据与运行控制记录不能登记为研究产物", 403)
        requested_id = fields.pop("artifact_id", None)
        with file_path.open("rb") as file:
            sha = hashlib.file_digest(file, "sha256").hexdigest()
        artifact_id = requested_id or "artifact-" + hashlib.sha256(canonical([research_id, sha, file_path.name, media_type, fields]).encode()).hexdigest()[:32]
        existing = self.get("artifact", artifact_id)
        if existing:
            if existing.get("research_id") != research_id or existing["sha256"] != sha:
                raise StoreError("artifact_identity_conflict", "产物身份已经绑定其他文件", 409)
            return existing
        # Mutable implementation workspaces are never the backing store for evidence.
        destination = self.data_dir / "artifacts" / sha[:2] / sha[2:] / file_path.name
        destination.parent.mkdir(parents=True, exist_ok=True)
        if not destination.exists():
            temporary = destination.with_name(f".{uuid.uuid4().hex[:8]}.tmp")
            shutil.copyfile(file_path, temporary)
            with temporary.open("rb+") as file:
                os.fsync(file.fileno())
            os.replace(temporary, destination)
        return self.put("artifact", artifact_id, {"artifact_id": artifact_id, "relative_path": destination.relative_to(self.data_dir).as_posix(),
            "media_type": media_type or mimetypes.guess_type(file_path.name)[0], "size_bytes": destination.stat().st_size,
            "sha256": sha, "name": file_path.name, "created_at": now(), **fields}, research_id)

    def artifact(self, artifact_id):
        artifact = self.get("artifact", artifact_id)
        if not artifact:
            raise StoreError("artifact_not_found", "产物不存在", 404)
        path = (self.data_dir / artifact["relative_path"]).resolve()
        if not path.is_relative_to(self.data_dir) or not path.is_file():
            raise StoreError("artifact_unavailable", "产物文件不可用", 404)
        return artifact, path

    def idempotent(self, scope, request_id, payload, action):
        request_hash = hashlib.sha256(canonical(payload).encode()).hexdigest()
        with self.connect(True) as c:
            row = c.execute("SELECT * FROM idempotency WHERE scope=? AND request_id=?", (scope, request_id)).fetchone()
            if row:
                if row["request_hash"] != request_hash:
                    raise StoreError("idempotency_conflict", "相同请求 ID 的内容发生变化")
                return json.loads(row["result"])
            result = action(c)
            c.execute("INSERT INTO idempotency VALUES(?,?,?,?)", (scope, request_id, request_hash, canonical(result)))
            return result


def build_topology(team):
    nodes, edges = [], []
    names = {"exploration": "探索组", "critic": "评审组", "implementation": "实施", "verify": "验证", "boss": "Boss 决策组", "organizer": "资源协调", "paper": "阶段报告"}
    def node(id, role, label=None, parent=None, **data):
        value = {"id": id, "type": "group" if role in {"exploration", "critic", "boss"} else "agent",
                 "data": {"agent_id": id, "role": role, "label": label or names.get(role, role), "status": "idle", **data},
                 "position": {"x": 0, "y": 0}}
        if parent:
            value["parentId"] = parent
            value["extent"] = "parent"
        nodes.append(value)
    def edge(source, target, label):
        edges.append({"id": f"{source}->{target}", "source": source, "target": target, "label": label, "data": {"status": "idle"}})
    node("boss", "boss", member_count=team["boss_members"])
    for i in range(1, team["boss_members"]+1):
        node(f"boss/member-{i:02d}", "boss_member", f"Boss {i}", "boss")
    node("organizer", "organizer", machine_shared=True)
    node("paper", "paper")
    edge("boss", "paper", "报告任务")
    for n in range(1, team["n"]+1):
        line = f"line-{n:02d}"
        for role, count in (("exploration", team["m1"]), ("critic", team["m2"])):
            group = f"{line}/{role}"
            node(group, role, f"{names[role]} {n}", line_id=line, member_count=count)
            for m in range(1, count+1):
                node(f"{group}/member-{m:02d}", f"{role}_member", f"{names[role][:-1]} {m}", group, line_id=line)
        node(f"{line}/implementation", "implementation", f"实施 {n}", line_id=line)
        node(f"{line}/verify", "verify", f"验证 {n}", line_id=line)
        for source, target, label in (("boss", f"{line}/exploration", "研究任务"),
            (f"{line}/exploration", f"{line}/critic", "方案与质疑"), (f"{line}/critic", f"{line}/implementation", "冻结方案"),
            (f"{line}/implementation", "organizer", "申请与授权"), (f"{line}/implementation", f"{line}/verify", "送验"),
            (f"{line}/verify", "boss", "验证汇报"), (f"{line}/verify", f"{line}/exploration", "修订意见")):
            edge(source,target,label)
    return {"nodes": nodes, "edges": edges, "agent_count": team["n"]*(team["m1"]+team["m2"]+2)+team["boss_members"]+2}
