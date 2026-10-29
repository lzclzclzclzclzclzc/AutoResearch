"""Database-independent, fail-closed execution control.

The short in-memory lock orders confirmation receipts and stop latches. Disk I/O,
network broadcasts and process termination always run outside that lock.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import copy
import hashlib
import hmac
import json
import os
import queue
import threading
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class ControlUnavailable(RuntimeError):
    def __init__(self, message: str, code: str = "control_unavailable"):
        super().__init__(message)
        self.code = code


def timestamp(value: Any) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, datetime):
        return value.timestamp()
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


class DurableJournal:
    """A dedicated writer, atomic checksummed records and fsync acknowledgements."""

    def __init__(self, directory: str | Path):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.queue: queue.Queue = queue.Queue()
        self.closed = False
        self.writer = threading.Thread(target=self._run, name="control-journal", daemon=True)
        self.writer.start()

    def read_all(self) -> tuple[list[dict], bool]:
        records, unknown = [], False
        for path in self.directory.iterdir():
            if path.suffix == ".tmp":
                # A failed Windows replacement can leave an already committed
                # stop snapshot behind. Recover only a checksummed duplicate of
                # the same durable tombstone; all other incomplete files block.
                if not self._archive_redundant_stop(path):
                    unknown = True
            if path.suffix != ".json":
                continue
            try:
                wrapper = json.loads(path.read_text(encoding="utf-8"))
                record = wrapper["record"]
                if not hmac.compare_digest(wrapper["sha256"], hashlib.sha256(_canonical(record)).hexdigest()):
                    raise ValueError("checksum mismatch")
                records.append(record)
            except (OSError, ValueError, KeyError, TypeError):
                unknown = True
        return records, unknown

    @staticmethod
    def _checked_record(path: Path) -> dict:
        wrapper = json.loads(path.read_text(encoding="utf-8"))
        record = wrapper["record"]
        if not isinstance(record, dict) or not hmac.compare_digest(
                wrapper["sha256"], hashlib.sha256(_canonical(record)).hexdigest()):
            raise ValueError("Invalid journal checksum")
        return record

    def _archive_redundant_stop(self, path: Path) -> bool:
        try:
            record = self._checked_record(path)
            if (record.get("kind") != "stop" or record.get("stopped") is not True
                    or record.get("enabled") is not False
                    or not all(isinstance(record.get(key), str) and record[key]
                               for key in ("research_id", "record_id", "stop_id"))):
                return False
            key = hashlib.sha256(f"{record['research_id']}/{record['record_id']}".encode()).hexdigest()
            if path.name.split(".")[0] != key:
                return False
            committed = self._checked_record(self.directory / f"{key}.json")
            # Replaying a stop updates only its runtime epoch. That cannot undo
            # the durable stop identity, time, reason, generation or latch.
            ignored = {"control_epoch", "previous_control_epoch"}
            stable = lambda item: {k: v for k, v in item.items() if k not in ignored}
            if stable(record) != stable(committed):
                return False
            archive = self.directory / "recovered-temporaries"
            archive.mkdir(exist_ok=True)
            self._replace(path, archive / path.name)
            return True
        except (OSError, ValueError, KeyError, TypeError):
            return False

    @staticmethod
    def _replace(source: Path, target: Path) -> None:
        # Antivirus/indexers may briefly open the target without delete sharing.
        # This bounded wait runs on the journal thread, never the stop latch lock.
        for attempt in range(6):
            try:
                os.replace(source, target)
                return
            except OSError as error:
                if getattr(error, "winerror", None) not in {5, 32, 33} or attempt == 5:
                    raise
                time.sleep(0.02 * 2 ** attempt)

    def append(self, record: dict) -> concurrent.futures.Future:
        future: concurrent.futures.Future = concurrent.futures.Future()
        self.queue.put((copy.deepcopy(record), future))
        return future

    def _write(self, record: dict) -> None:
        key = f"{record.get('research_id', '')}/{record['record_id']}"
        name = hashlib.sha256(key.encode()).hexdigest()
        target = self.directory / f"{name}.json"
        temporary = self.directory / f"{name}.{uuid.uuid4().hex}.tmp"
        wrapper = {"record": record, "sha256": hashlib.sha256(_canonical(record)).hexdigest()}
        with temporary.open("xb") as stream:
            stream.write(_canonical(wrapper))
            stream.flush()
            os.fsync(stream.fileno())
        self._replace(temporary, target)
        # Windows atomic replacement plus flushed file; Unix additionally flushes directory metadata.
        if os.name != "nt":
            descriptor = os.open(self.directory, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)

    def _run(self) -> None:
        while True:
            item = self.queue.get()
            if item is None:
                return
            record, future = item
            try:
                self._write(record)
                future.set_result(True)
            except Exception as exc:
                future.set_exception(exc)

    def close(self) -> None:
        self.closed = True
        self.queue.put(None)
        self.writer.join(timeout=3)


class ControlSupervisor:
    def __init__(self, data_dir: str | Path = "data/control", journal: DurableJournal | None = None):
        self.epoch = uuid.uuid4().hex
        self._lock = threading.RLock()
        self._seq = 0
        self.projects: dict[str, dict] = {}
        self.receipts: dict[str, dict] = {}
        self.executions: dict[str, dict] = {}
        self.endpoints: dict[str, str] = {}
        self.journal = journal or DurableJournal(data_dir)
        self._shutdown = threading.Event()
        records, self.journal_unknown = self.journal.read_all()
        for record in sorted(records, key=lambda r: r.get("control_seq", r.get("stop_seq", 0))):
            rid = record.get("research_id")
            if not rid:
                continue
            if record["kind"] == "project":
                self.projects[rid] = {**record, "enabled": False, "stopped": True,
                    "reason": "control_restarted", "stop_journal_persisted": False, "db_synced": False,
                    "previous_control_epoch": record.get("control_epoch"), "control_epoch": self.epoch}
            elif record["kind"] == "stop":
                self.projects[rid] = {**self.projects.get(rid, {}), **record, "enabled": False,
                    "stopped": True, "stop_journal_persisted": True, "db_synced": False,
                    "previous_control_epoch": record.get("control_epoch"), "control_epoch": self.epoch}
            elif record["kind"] == "confirmation":
                self.receipts[record["operation_id"]] = {**record, "persisted": True}
            elif record["kind"] == "complete":
                self.projects[rid] = {**self.projects.get(rid, {}), **record, "enabled": False,
                    "completed": True, "stopped": False, "control_epoch": self.epoch}
            elif record["kind"] == "execution":
                self.executions[record["execution_id"]] = record
        # A new control epoch can never revive an old project, even without a stop record.
        for rid, project in list(self.projects.items()):
            if project.get("completed"):
                continue
            if not project.get("stop_id"):
                project["stopped"] = False
                self.terminate(rid, "control_restarted")
            else:
                self._dispatch_stop(rid)
        self._timer = threading.Thread(target=self._monitor, name="control-deadlines", daemon=True)
        self._timer.start()

    def _next(self) -> int:
        self._seq += 1
        return self._seq

    def register_project(self, research_id: str, deadline_at: Any, control_generation: int = 1, **_: Any) -> dict:
        deadline = timestamp(deadline_at)
        if not research_id or deadline <= time.time():
            raise ValueError("Project deadline must be in the future")
        with self._lock:
            existing = self.projects.get(research_id)
            if existing:
                if existing.get("deadline_at") != deadline or existing.get("control_generation", 1) != control_generation:
                    raise ValueError("A project's frozen deadline and generation cannot change")
                return self.status(research_id)
            if self.journal_unknown:
                raise ControlUnavailable("控制日志存在未确认或损坏记录，启动门禁已关闭；请检查控制服务恢复状态。", "control_journal_incomplete")
            record = {"kind": "project", "record_id": f"project-{research_id}", "research_id": research_id,
                "deadline_at": deadline, "control_generation": control_generation, "control_epoch": self.epoch,
                "registered_at": iso_now(), "enabled": False, "stopped": False, "db_synced": False}
            self.projects[research_id] = record
        try:
            self.journal.append(record).result(timeout=5)
        except Exception as exc:
            self.terminate(research_id, "control_journal_unavailable")
            raise ControlUnavailable("Could not persist project control identity") from exc
        with self._lock:
            if not record["stopped"]:
                record["enabled"] = True
        return self.status(research_id)

    def _allows_locked(self, rid: str, generation: int | None = None, epoch: str | None = None) -> bool:
        project = self.projects.get(rid)
        return bool(project and project.get("enabled") and not project.get("stopped")
            and project.get("deadline_at", 0) > time.time()
            and (generation is None or generation == project.get("control_generation"))
            and (epoch is None or epoch == self.epoch))

    def allows_progress(self, research_id: str, control_generation: int | None = None, control_epoch: str | None = None) -> bool:
        with self._lock:
            return self._allows_locked(research_id, control_generation, control_epoch)

    def confirm_operation(self, research_id: str, operation_id: str, content_hash: str,
                          control_generation: int = 1, **_: Any) -> dict:
        with self._lock:
            old = self.receipts.get(operation_id)
            if old:
                if old["research_id"] != research_id or old["content_hash"] != content_hash:
                    raise ValueError("Operation identity cannot be reused with different content")
                return copy.deepcopy(old)
            if not self._allows_locked(research_id, control_generation):
                raise ControlUnavailable("Project is stopped, expired or has an obsolete generation")
            receipt = {"kind": "confirmation", "record_id": f"confirm-{operation_id}",
                "research_id": research_id, "operation_id": operation_id, "content_hash": content_hash,
                "control_generation": control_generation, "control_epoch": self.epoch,
                "control_seq": self._next(), "confirmed_at": iso_now(), "persisted": False}
            self.receipts[operation_id] = receipt
        # stop can acquire the lock and broadcast while this writer is blocked.
        future = self.journal.append(receipt)
        def persisted(done: concurrent.futures.Future) -> None:
            try:
                done.result()
            except Exception:
                return
            # The HTTP caller may already have timed out. Persistence belongs to
            # the writer acknowledgement, so historical recovery can still find it.
            with self._lock:
                receipt["persisted"] = True
        future.add_done_callback(persisted)
        try:
            future.result(timeout=5)
        except Exception as exc:
            raise ControlUnavailable("Confirmation was not durably recorded") from exc
        with self._lock:
            receipt["persisted"] = True
            return copy.deepcopy(receipt)

    def register_process(self, research_id: str, execution_id: str, pid: int | None = None,
                         create_time: float | None = None, kind: str = "agent", control_epoch: str | None = None,
                         control_generation: int = 1, supervisor_id: str | None = None, **extra: Any) -> dict:
        if pid is not None and create_time is None:
            from .processes import process_identity
            create_time = process_identity(pid)
            if create_time is None:
                raise ValueError("Process registration requires a verifiable creation identity")
        with self._lock:
            if not self._allows_locked(research_id, control_generation, control_epoch):
                raise ControlUnavailable("Execution start forbidden")
            old = self.executions.get(execution_id)
            if old and (old["research_id"] != research_id or old.get("state") == "finished"):
                raise ValueError("Execution identity already used")
            entry = {"kind": "execution", "record_id": f"execution-{execution_id}", "research_id": research_id,
                "execution_id": execution_id, "pid": pid, "create_time": create_time, "execution_kind": kind,
                "control_epoch": self.epoch, "control_generation": control_generation,
                "supervisor_id": supervisor_id, "state": "registered", "registered_at": iso_now()}
            self.executions[execution_id] = entry
        future = self.journal.append(entry)
        if pid is None:
            try:
                future.result(timeout=5)
            except Exception as exc:
                raise ControlUnavailable("Execution registration was not durable") from exc
        # Recheck after registration persistence; stop cannot be reversed by this response.
        if not self.allows_progress(research_id, control_generation, self.epoch):
            self._dispatch_stop(research_id)
            raise ControlUnavailable("Project stopped during registration")
        return {**entry, "allowed": True}

    def get_confirmation(self,operation_id:str)->dict|None:
        with self._lock:
            receipt=self.receipts.get(operation_id)
            return copy.deepcopy(receipt) if receipt and receipt.get("persisted") else None

    def complete_execution(self, execution_id: str, **_: Any) -> dict:
        with self._lock:
            entry = self.executions.get(execution_id)
            if not entry:
                return {"state": "unknown"}
            entry["state"] = "finished"
            entry["finished_at"] = iso_now()
            saved = copy.deepcopy(entry)
        self.journal.append(saved)
        return saved

    def register_supervisor(self, supervisor_id: str, stop_url: str, **_: Any) -> dict:
        from urllib.parse import urlparse
        parsed = urlparse(stop_url)
        if parsed.scheme != "http" or parsed.hostname not in ("127.0.0.1", "localhost", "::1"):
            raise ValueError("Supervisor endpoint must be loopback HTTP")
        with self._lock:
            self.endpoints[supervisor_id] = stop_url
        return {"control_epoch": self.epoch, "registered": True}

    def terminate(self, research_id: str, reason: str = "user_requested", stop_id: str | None = None, **_: Any) -> dict:
        with self._lock:
            project = self.projects.setdefault(research_id, {"research_id": research_id,
                "control_generation": 1, "deadline_at": 0})
            if project.get("stop_id"):
                result = self.status(research_id)
                repeat = True
            else:
                repeat = False
                project.update({"kind": "stop", "record_id": f"stop-{stop_id or uuid.uuid4().hex}",
                    "stop_id": stop_id or uuid.uuid4().hex, "reason": reason, "stop_reason": reason,
                    "stopped": True, "enabled": False, "stop_requested_at": iso_now(), "control_epoch": self.epoch,
                    "stop_seq": self._next(), "stop_journal_persisted": False, "db_synced": False})
                project["control_generation"] = int(project.get("control_generation", 1)) + 1
                record = copy.deepcopy(project)
                result = self.status(research_id)
        self._dispatch_stop(research_id)
        if not repeat:
            future = self.journal.append(record)
            def persisted(done: concurrent.futures.Future) -> None:
                with self._lock:
                    try:
                        done.result()
                        project["stop_journal_persisted"] = True
                    except Exception:
                        project["stop_journal_persisted"] = False
                        project["persistence_error"] = "停止记录未持久化"
            future.add_done_callback(persisted)
        return result

    stop = terminate

    def complete_project(self, research_id: str, **_: Any) -> dict:
        with self._lock:
            project = self.projects.get(research_id)
            if project is None:
                raise ValueError("Unknown project")
            if project.get("completed") or project.get("stopped"):
                return self.status(research_id)
            if not self._allows_locked(research_id):
                raise ControlUnavailable("Expired project cannot complete new research")
            project.update(enabled=False, completed=True, completed_at=iso_now())
            record = {**project, "kind": "complete", "record_id": f"complete-{research_id}", "control_seq": self._next()}
        self.journal.append(record)
        return self.status(research_id)

    def _dispatch_stop(self, research_id: str) -> None:
        with self._lock:
            endpoints = list(self.endpoints.values())
            entries = [copy.deepcopy(e) for e in self.executions.values()
                if e["research_id"] == research_id and e.get("state") != "finished"]
            reason = self.projects[research_id].get("reason", "user_requested")
        def dispatch() -> None:
            for url in endpoints:
                def notify(target=url):
                    try:
                        _request(target, {"research_id": research_id, "reason": reason}, timeout=1)
                    except ControlUnavailable:
                        pass
                threading.Thread(target=notify, daemon=True).start()
            from .processes import terminate_identity
            for entry in entries:
                if entry.get("pid"):
                    success = terminate_identity(entry["pid"], entry.get("create_time"))
                    if success:
                        self.complete_execution(entry["execution_id"])
                # A missing PID may mean creation is still in flight. Only the
                # owning supervisor can prove that admission irrevocably ended.
        threading.Thread(target=dispatch, name=f"stop-{research_id}", daemon=True).start()

    def status(self, research_id: str | None = None) -> dict:
        with self._lock:
            if research_id is None:
                return {"control_epoch": self.epoch, "journal_unknown": self.journal_unknown,
                    "projects": {rid: self.status(rid) for rid in self.projects}}
            project = self.projects.get(research_id)
            if project is None:
                return {"research_id": research_id, "control_epoch": self.epoch, "allowed": False,
                    "state": "unknown", "stopped": True, "stop_journal_persisted": False, "db_synced": False}
            result = copy.deepcopy(project)
            pending = [copy.deepcopy(e) for e in self.executions.values()
                if e["research_id"] == research_id and e.get("state") != "finished"]
            result.update({"control_epoch": self.epoch, "allowed": self._allows_locked(research_id),
                "unfinished": pending, "execution_stopped": result.get("stopped", False) and not pending,
                "state": "completed" if result.get("completed") else
                "terminated" if result.get("stopped") and not pending and result.get("db_synced") and result.get("resources_reconciled") else
                "terminating" if result.get("stopped") and pending else
                "stopped_pending_sync" if result.get("stopped") else "running", "db_synced": result.get("db_synced", False)})
            return result

    def mark_db_synced(self, research_id: str, stop_id: str, resources_reconciled: bool = False, **_: Any) -> dict:
        with self._lock:
            project = self.projects.get(research_id)
            if not project or project.get("stop_id") != stop_id:
                raise ValueError("Stop identity mismatch")
            if project.get("db_synced") and project.get("resources_reconciled") == resources_reconciled:
                return self.status(research_id)
            project["db_synced"] = True
            project["resources_reconciled"] = resources_reconciled
            record = copy.deepcopy(project)
        self.journal.append(record)
        return self.status(research_id)

    def _monitor(self) -> None:
        while not self._shutdown.wait(0.1):
            with self._lock:
                expired = [rid for rid, p in self.projects.items()
                    if not p.get("stopped") and not p.get("completed") and p.get("deadline_at", 0) <= time.time()]
            for rid in expired:
                self.terminate(rid, "time_limit_reached")

    def close(self) -> None:
        self._shutdown.set()
        with self._lock:
            ids = list(self.projects)
        for rid in ids:
            if not self.projects[rid].get("completed"):
                self.terminate(rid, "control_shutdown")
        self.journal.close()


def _request(url: str, body: dict | None = None, token: str | None = None, timeout: float = 2) -> dict:
    headers = {"Content-Type": "application/json"}
    secret = token if token is not None else os.environ.get("AUTORESEARCH_SESSION_TOKEN")
    if secret:
        headers["X-AutoResearch-Token"] = secret
    req = urllib.request.Request(url, data=_canonical(body) if body is not None else None, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        try:
            detail = json.loads(exc.read(8192))
        except (OSError, ValueError):
            detail = {}
        if detail.get("code") == "control_journal_incomplete":
            raise ControlUnavailable("控制日志存在未确认或损坏记录，启动门禁已关闭；请检查控制服务恢复状态。", "control_journal_incomplete") from exc
        if 400 <= exc.code < 500 and exc.code not in {408, 429}:
            raise ControlUnavailable("Local supervisor rejected the operation", "local_operation_rejected") from exc
        raise ControlUnavailable("Local supervisor is unavailable or rejected the operation") from exc
    except (OSError, ValueError) as exc:
        raise ControlUnavailable("Local supervisor is unavailable or rejected the operation") from exc


class ControlClient:
    def __init__(self, base_url: str | None = None, token: str | None = None):
        self.base_url = (base_url or os.environ.get("AUTORESEARCH_CONTROL_URL", "http://127.0.0.1:8766")).rstrip("/")
        self.token = token

    def _post(self, path: str, body: dict, timeout: float = 7) -> dict:
        return _request(f"{self.base_url}{path}", body, self.token, timeout)

    def register_project(self, research_id: str, deadline_at: Any, control_generation: int = 1, **kwargs: Any) -> dict:
        return self._post("/register-project", dict(research_id=research_id, deadline_at=deadline_at,
            control_generation=control_generation, **kwargs))

    def allows_progress(self, research_id: str, control_generation: int | None = None,
                        control_epoch: str | None = None) -> bool:
        # Transport failure is unknown admission, not a durable stop decision.
        # Callers still fail closed, but can recover after Control is reachable.
        return bool(self._post("/allows-progress", dict(research_id=research_id,
            control_generation=control_generation, control_epoch=control_epoch), timeout=1)["allowed"])

    def status(self, research_id: str | None = None) -> dict:
        from urllib.parse import quote
        suffix = f"/{quote(research_id, safe='')}" if research_id else ""
        return _request(f"{self.base_url}/control-status{suffix}", token=self.token)

    def terminate(self, research_id: str, reason: str = "user_requested", stop_id: str | None = None) -> dict:
        return self._post("/terminate", dict(research_id=research_id, reason=reason, stop_id=stop_id), timeout=2)

    stop = terminate

    def confirm_operation(self, research_id: str, operation_id: str, content_hash: str,
                          control_generation: int = 1, **kwargs: Any) -> dict:
        return self._post("/confirm-operation", dict(research_id=research_id, operation_id=operation_id,
            content_hash=content_hash, control_generation=control_generation, **kwargs))

    def register_process(self, research_id: str, execution_id: str, pid: int | None = None,
                         create_time: float | None = None, kind: str = "agent", **kwargs: Any) -> dict:
        return self._post("/register-process", dict(research_id=research_id, execution_id=execution_id,
            pid=pid, create_time=create_time, kind=kind, **kwargs))

    def complete_execution(self, execution_id: str) -> dict:
        return self._post("/complete-execution", {"execution_id": execution_id})

    def get_confirmation(self,operation_id:str)->dict|None:
        from urllib.parse import quote
        return _request(f"{self.base_url}/confirmations/{quote(operation_id,safe='')}",token=self.token).get("receipt")

    def complete_project(self, research_id: str) -> dict:
        return self._post("/complete-project", {"research_id": research_id})

    def register_supervisor(self, supervisor_id: str, stop_url: str) -> dict:
        return self._post("/register-supervisor", dict(supervisor_id=supervisor_id, stop_url=stop_url))

    def mark_db_synced(self, research_id: str, stop_id: str, resources_reconciled: bool = False) -> dict:
        return self._post("/db-synced", dict(research_id=research_id, stop_id=stop_id, resources_reconciled=resources_reconciled))


def create_app(data_dir: str | Path = "data/control", supervisor: ControlSupervisor | None = None):
    from contextlib import asynccontextmanager
    from fastapi import FastAPI, HTTPException, Request
    from fastapi.responses import JSONResponse
    service = supervisor or ControlSupervisor(data_dir)

    @asynccontextmanager
    async def lifespan(app):
        yield
        service.close()

    app = FastAPI(title="AutoResearch independent control", lifespan=lifespan)
    app.state.supervisor = service

    @app.middleware("http")
    async def local_auth(request: Request, call_next):
        secret = os.environ.get("AUTORESEARCH_SESSION_TOKEN")
        if secret and not hmac.compare_digest(request.headers.get("x-autoresearch-token", ""), secret):
            return JSONResponse(status_code=403, content={"code": "control_auth_required"})
        origin = request.headers.get("origin")
        if origin:
            from urllib.parse import urlparse
            if urlparse(origin).hostname not in ("127.0.0.1", "localhost", "::1"):
                return JSONResponse(status_code=403, content={"code": "origin_rejected"})
        return await call_next(request)

    @app.exception_handler(ControlUnavailable)
    async def unavailable(request, exc):
        return JSONResponse(status_code=409, content={"code": exc.code, "message": str(exc)})

    @app.exception_handler(ValueError)
    async def invalid(request, exc):
        return JSONResponse(status_code=422, content={"code": "invalid_control_operation", "message": str(exc)})

    @app.get("/health")
    def health():
        return {"status": "ok", "control_epoch": service.epoch, "journal_unknown": service.journal_unknown}

    @app.get("/control-status")
    async def status_all():
        return service.status()

    @app.get("/control-status/{research_id}")
    async def status_one(research_id: str):
        return service.status(research_id)

    @app.post("/terminate")
    async def terminate(payload: dict):
        return service.terminate(**payload)

    @app.get("/confirmations/{operation_id:path}")
    async def get_confirmation(operation_id:str):
        return {"receipt":service.get_confirmation(operation_id)}

    for path, method in {"register-project": service.register_project,
        "confirm-operation": service.confirm_operation, "register-process": service.register_process,
        "complete-execution": service.complete_execution, "register-supervisor": service.register_supervisor,
        "complete-project": service.complete_project,
        "db-synced": service.mark_db_synced}.items():
        def endpoint_factory(fn):
            def endpoint(payload: dict):
                return fn(**payload)
            return endpoint
        app.post(f"/{path}")(endpoint_factory(method))

    @app.post("/allows-progress")
    def allowed(payload: dict):
        return {"allowed": service.allows_progress(**payload), "control_epoch": service.epoch}
    return app


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default=os.environ.get("AUTORESEARCH_CONTROL_DIR", "data/control"))
    parser.add_argument("--port", type=int, default=8766)
    args = parser.parse_args()
    import uvicorn
    uvicorn.run(create_app(args.data_dir), host="127.0.0.1", port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
