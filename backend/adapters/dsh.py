from __future__ import annotations

import asyncio
import importlib.metadata
import json
import os
import sys
import shutil
import subprocess
from functools import lru_cache
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psutil

from packages.contracts.research import digest
from backend.adapters.provider_endpoints import messages_base_url
from backend.adapters.token_limits import output_tokens, truncated_output_error
from backend.persistence.resilience import is_storage_busy


class RuntimeBlocked(RuntimeError):
    def __init__(self, code: str, message: str, *, http_status=None, provider_error_code=None):
        super().__init__(message)
        self.code = code
        self.http_status = http_status
        self.provider_error_code = provider_error_code


class ResearchStopped(RuntimeError):
    pass


def parse_final_object(response, schema, *, finish_reason, record_repair):
    """Repair only one missing object terminator in a normally completed reply."""
    try:
        return schema.model_validate_json(response).model_dump()
    except ValueError as original:
        raw = response.rstrip()
        if finish_reason != "completed" or not raw.startswith("{"):
            raise
        try:
            json.loads(raw)
        except json.JSONDecodeError as syntax:
            if syntax.pos != len(raw):
                raise original
        else:
            # Valid JSON with a contract error belongs to the usual correction path.
            raise original
        audit = {"original_response": response, "method": "append_single_right_brace",
                 "adopted": False}
        try:
            candidate = json.loads(raw + "}")
            if not isinstance(candidate, dict):
                raise original
            result = schema.model_validate(candidate).model_dump()
            if validator := getattr(schema, "validate_repaired_output", None):
                validator(result)
        except ValueError as error:
            audit["validation_error"] = str(error)
            record_repair(audit)
            raise error from original
        audit["adopted"] = True
        record_repair(audit)
        return result


@lru_cache(maxsize=8)
def _external_runtime(path):
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file():
        return None
    try:
        result = subprocess.run([str(resolved), "--version"], capture_output=True, text=True, timeout=10,
            creationflags=0x08000000 if os.name == "nt" else 0)
        if result.returncode == 0 and result.stdout.strip() == "0.2.1-rc.2":
            return str(resolved)
    except (OSError, subprocess.TimeoutExpired):
        pass
    return None


def runtime_availability():
    try:
        sdk = importlib.metadata.version("deepseek-harness-sdk")
    except importlib.metadata.PackageNotFoundError:
        return {"available": False, "code": "dsh_sdk_missing", "message": "未安装官方 deepseek-harness-sdk；真实研究被阻止"}
    if sdk != "0.2.1rc2":
        return {"available": False, "code": "dsh_version_unverified", "message": "角色工具策略仅核验 SDK/runtime 0.2.1rc2"}
    explicit = os.environ.get("AUTORESEARCH_DSH_BIN")
    if explicit:
        executable = _external_runtime(explicit)
        if not executable:
            return {"available": False, "code": "dsh_runtime_version_mismatch", "message": "指定的官方 DSH 可执行文件不是锁定的 0.2.1-rc.2 版本"}
        return {"available": True, "sdk_version": sdk, "runtime_version": sdk, "runtime_source": "explicit_official_cli",
                "dsh_bin": executable, "profile": "sdk"}
    try:
        runtime = importlib.metadata.version("deepseek-harness-runtime-bin")
    except importlib.metadata.PackageNotFoundError:
        return {"available": False, "code": "dsh_runtime_missing", "message": "未安装匹配的原生 runtime；也可以通过 AUTORESEARCH_DSH_BIN 显式选择已安装且匹配版本的官方 CLI"}
    if runtime != sdk:
        return {"available": False, "code": "dsh_version_mismatch", "message": "DSH SDK 与 runtime 版本不匹配"}
    return {"available": True, "sdk_version": sdk, "runtime_version": runtime, "runtime_source": "bundled_native", "profile": "sdk"}


def kill_tree(pid: int) -> bool:
    try:
        parent = psutil.Process(pid)
        tree = parent.children(recursive=True) + [parent]
        for process in reversed(tree):
            try:
                process.kill()
            except psutil.NoSuchProcess:
                pass
        _, alive = psutil.wait_procs(tree, timeout=3)
        return not alive
    except psutil.NoSuchProcess:
        return True


class RuntimeMessageReader:
    """Read JSONL in chunks without StreamReader's fixed line-length limit.

    Partial messages survive the caller's periodic timeout, so stop checks can
    interrupt a slow producer without discarding bytes already received.
    """

    def __init__(self, stream):
        self.stream = stream
        self.buffer = bytearray()
        self.scan_from = 0

    async def readline(self):
        while True:
            newline = self.buffer.find(b"\n", self.scan_from)
            if newline >= 0:
                line = bytes(self.buffer[:newline + 1])
                del self.buffer[:newline + 1]
                self.scan_from = 0
                return line
            self.scan_from = len(self.buffer)
            chunk = await self.stream.read(64 * 1024)
            if not chunk:
                line = bytes(self.buffer)
                self.buffer.clear()
                self.scan_from = 0
                return line
            self.buffer.extend(chunk)


class DshRuntimeAdapter:
    """Official synchronous SDK in registered, independently killable children.

    Each instance is serialized per durable session. Separate instances admit
    parallel work up to the model semaphore. SDK homes never read the user's
    normal Harness home. The business ledger owns attempts, not the SDK.
    """

    def __init__(self, store, control, credentials, concurrency: int | None = None, runner=None):
        self.store, self.control, self.credentials = store, control, credentials
        self.root = store.data_dir / "dsh"
        self.root.mkdir(parents=True, exist_ok=True)
        self.semaphore = asyncio.Semaphore(concurrency) if concurrency is not None else None
        self.locks: dict[str, asyncio.Lock] = {}
        self.processes: dict[str, asyncio.subprocess.Process] = {}
        self.runner, self.gateway = runner, None
        self.project_semaphores = {}
        self.provider_semaphores = {}

    def availability(self):
        return runtime_availability()

    def ensure_session(self, research_id, agent_id, direction_epoch=1, profile_version="1"):
        machine = agent_id == "organizer"
        key = digest(["machine" if machine else research_id, agent_id, 1 if machine else direction_epoch, profile_version])[:32]
        home = self.root / "homes" / key
        workspace = self.root / "workspaces" / key
        home.mkdir(parents=True, exist_ok=True)
        workspace.mkdir(parents=True, exist_ok=True)
        session = self.store.get("dsh_session", key)
        if session is None:
            session = self.store.put("dsh_session", key, {"session_id": "ar-" + key, "agent_id": agent_id,
                "home": str(home), "workspace": str(workspace), "has_history": False}, research_id)
        return session

    @asynccontextmanager
    async def _admission(self, research_id, generation, runtime_context, *gates):
        acquired = []
        announced = False
        wait_started = asyncio.get_running_loop().time()
        try:
            for gate in gates:
                waiting = asyncio.create_task(gate.acquire())
                try:
                    while not waiting.done():
                        self._assert_arbitration_open(runtime_context)
                        if not await asyncio.to_thread(self.control.allows_progress, research_id, generation):
                            raise ResearchStopped("等待模型容量期间研究已停止")
                        if (runtime_context.get("arbitration_id") and not announced and not waiting.done()
                                and asyncio.get_running_loop().time() - wait_started >= 0.25):
                            self.store.emit(research_id, "arbitration.capacity_wait", {
                                "arbitration_id": runtime_context["arbitration_id"], "execution_phase": "waiting_capacity"})
                            announced = True
                        await asyncio.wait({waiting}, timeout=0.2)
                    await waiting
                    acquired.append(gate)
                finally:
                    if not waiting.done():
                        waiting.cancel()
                        await asyncio.gather(waiting, return_exceptions=True)
                    elif gate not in acquired and not waiting.cancelled() and waiting.exception() is None:
                        gate.release()
            project = await asyncio.to_thread(self.store.project, research_id)
            self._assert_arbitration_open(runtime_context)
            if project["status"] in {"pausing", "paused", "blocked"}:
                raise RuntimeBlocked("research_paused", "研究暂停期间不启动新的模型回合")
            if runtime_context.get("arbitration_id") and announced:
                self.store.emit(research_id, "arbitration.capacity_acquired", {
                    "arbitration_id": runtime_context["arbitration_id"], "execution_phase": "judging"})
            yield
        finally:
            for gate in reversed(acquired):
                gate.release()

    def _assert_arbitration_open(self, runtime_context):
        aid = (runtime_context or {}).get("arbitration_id")
        if aid:
            request = self.store.get("arbitration_request", aid)
            if not request or request["status"] in {"cancelled", "stale", "exhausted", "needs_external_input"}:
                raise ResearchStopped("该 God 仲裁范围已经取消或失效")

    async def execute_turn(self, *, research_id, agent_id, role, task_id, turn_id,
                           prompt, schema, direction_epoch=1, model=None, context=None, runtime_context=None):
        runtime_context = runtime_context or {}
        private_score = bool(runtime_context.get("private_score", runtime_context.get("suppress_chat")
            and runtime_context.get("discussion_protocol") != 2))
        available = self.availability()
        if not available["available"]:
            raise RuntimeBlocked(available["code"], available["message"])
        project = self.store.project(research_id)
        generation = project["control_generation"]
        session = self.ensure_session(research_id, agent_id, direction_epoch)
        interrupted_history = any(e.get("session_id") == session["session_id"] and e.get("status") in {"interrupted", "unknown"}
                                  for e in self.store.list("execution", research_id))
        logical_key = digest([research_id, agent_id, task_id, turn_id])
        concurrency = project["config_snapshot"]["team"].get("model_concurrency", 4)
        project_semaphore = self.project_semaphores.setdefault(research_id, asyncio.Semaphore(concurrency))
        provider = self.store.get("provider", project["provider_config_id"]) or {}
        benchmark = provider.get("concurrency_benchmark") or {}
        measured_gates = []
        if (benchmark.get("recommended_limit") and benchmark.get("base_url") == provider.get("base_url")
                and benchmark.get("model") == provider.get("model")
                and benchmark.get("credential_version") == provider.get("credential_version")):
            key = digest([project["provider_config_id"], benchmark])
            measured_gates.append(self.provider_semaphores.setdefault(key, asyncio.Semaphore(int(benchmark["recommended_limit"]))))
        async with self._admission(research_id, generation, runtime_context or {},
                self.locks.setdefault(session["id"], asyncio.Lock()), project_semaphore,
                *measured_gates, *([self.semaphore] if self.semaphore is not None else [])):
            if not await asyncio.to_thread(self.control.allows_progress, research_id, generation):
                raise ResearchStopped("研究已停止或控制服务不可用")
            previous = self.store.get("execution", logical_key)
            if previous and previous["status"] == "completed":
                return previous["output"]
            if previous and previous["status"] in {"registered", "running", "unknown"}:
                pid = previous.get("pid")
                if not pid:
                    raise RuntimeBlocked("execution_unknown", "旧 DSH 启动身份未得到退出证明，拒绝重新调用")
                if pid and psutil.pid_exists(pid):
                    raise RuntimeBlocked("execution_unknown", "旧 DSH 执行仍存活，拒绝重复调用")
                self.store.patch("execution", logical_key, status="interrupted")
            secret = self.credentials.get_secret(project["provider_config_id"], version=project.get("credential_version"))
            if not secret:
                raise RuntimeBlocked("credential_missing", "请先在设置中保存 DeepSeek API Key")
            attempt = (previous or {}).get("attempt", 0) + 1
            execution_id = f"{logical_key}-{attempt}"
            token_limit = output_tokens(private_score, runtime_context.get("max_output_tokens"))
            record = {"status": "registered", "attempt": attempt, "execution_attempt_id": execution_id,
                "max_output_tokens": token_limit,
                "conversation_id": (runtime_context or {}).get("conversation_id"),
                "agent_id": agent_id, "task_id": task_id, "turn_id": turn_id, "session_id": session["session_id"],
                "control_generation": generation, "created_at": datetime.now(timezone.utc).isoformat(), **available}
            self.store.put("execution", logical_key, record, research_id)
            await asyncio.to_thread(self.control.register_process, research_id, execution_id, None, None, "agent",
                                    control_generation=generation)
            env = {k: v for k, v in os.environ.items() if not any(s in k.upper() for s in ("API_KEY", "TOKEN", "SECRET", "PASSWORD"))}
            env["PYTHONIOENCODING"] = "utf-8"
            kwargs = {"creationflags": 0x08000000} if os.name == "nt" else {"start_new_session": True}
            try:
                proc = await asyncio.create_subprocess_exec(sys.executable, "-m", "backend.adapters.sdk_child",
                    stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                    cwd=str(Path(__file__).resolve().parents[2]), env=env, **kwargs)
            except BaseException:
                await asyncio.to_thread(self.control.complete_execution, execution_id)
                self.store.patch("execution", logical_key, status="failed", error_code="runtime_owner_start_failed")
                raise
            self.processes[execution_id] = proc
            exit_confirmed = False
            capability = None
            turn_completed = False
            try:
                await asyncio.to_thread(self.control.register_process, research_id, execution_id, proc.pid,
                    psutil.Process(proc.pid).create_time(), "agent", control_generation=generation)
                self.store.patch("execution", logical_key, status="running", pid=proc.pid, started_at=datetime.now(timezone.utc).isoformat(),
                                 pid_create_time=psutil.Process(proc.pid).create_time())
                self.store.emit(research_id, "agent.started", {"mode": "live", "role": role, "execution_attempt_id": execution_id,
                    "attempt": attempt, "session_id": session["session_id"]}, agent_id=agent_id, execution_id=execution_id, task_id=task_id)
                deadline = datetime.fromisoformat(project["deadline_at"].replace("Z", "+00:00"))
                payload = {"api_key": secret, "home": session["home"], "workspace": session["workspace"],
                    "session_id": session["session_id"], "resume": session.get("has_history", False) or interrupted_history,
                    "prompt": prompt, "parent_pid": os.getpid(), "parent_create_time": psutil.Process().create_time(), "deadline_at": project["deadline_at"],
                    "timeout": max(1, (deadline - datetime.now(timezone.utc)).total_seconds()), "max_output_tokens": token_limit,
                    "model": (model or {}).get("model", (model or {}).get("model_id", "deepseek-flash")),
                    "base_url": messages_base_url((model or {}).get("base_url"))}
                payload["dsh_bin"] = available.get("dsh_bin")
                if self.runner is None:
                    raise RuntimeBlocked("tool_gateway_missing", "未绑定科研工具网关，拒绝启动真实 Harness")
                if self.gateway is None:
                    from backend.adapters.gateway import ToolGateway
                    self.gateway = ToolGateway(self.store, self.control, self.runner)
                def artifact_ids(value):
                    if isinstance(value, dict):
                        return ([value["artifact_id"]] if value.get("artifact_id") else []) + [a for v in value.values() for a in artifact_ids(v)]
                    if isinstance(value, list):
                        return [a for v in value for a in artifact_ids(v)]
                    return []
                capability = self.gateway.register(research_id=research_id, role=role, agent_id=agent_id,
                    execution_id=execution_id, control_generation=generation, task_id=task_id,
                    sdk_owner_pid=proc.pid, sdk_owner_create_time=psutil.Process(proc.pid).create_time(),
                    redact_secret=secret,
                    permitted_artifact_ids=artifact_ids(context or {}), **(runtime_context or {}))
                payload.update(gateway_url=self.gateway.url, gateway_token=capability, role=role,
                    auxiliary=private_score,
                    has_inputs=bool(project.get("workspace_path")),
                    plugin_entry=str(Path(__file__).resolve().parents[2] / "packages" / "dsh-plugin-research" / "index.js"))
                proc.stdin.write((json.dumps(payload, ensure_ascii=False) + "\n").encode())
                await proc.stdin.drain()
                proc.stdin.close()
                final = None
                offset = 0
                message_reader = RuntimeMessageReader(proc.stdout)
                stderr_task = asyncio.create_task(proc.stderr.read())
                while True:
                    try:
                        line = await asyncio.wait_for(message_reader.readline(), 0.4)
                    except asyncio.TimeoutError:
                        self._assert_arbitration_open(runtime_context)
                        if not await asyncio.to_thread(self.control.allows_progress, research_id, generation):
                            raise ResearchStopped("研究已终止")
                        continue
                    if not line:
                        break
                    self._assert_arbitration_open(runtime_context)
                    offset += 1
                    try:
                        from backend.services.credentials import redact
                        item = json.loads(redact(line.decode("utf-8").replace(capability, "[CAPABILITY]"), secret))
                    except json.JSONDecodeError:
                        continue
                    # Keep the SDK payload; committed tools also drive the live conversation.
                    from backend.persistence.event_views import tool_event_type
                    self.store.emit(research_id, tool_event_type(item), {"execution_attempt_id": execution_id,
                        "source_cursor": offset, "notification": item}, agent_id=agent_id,
                        conversation_id=(runtime_context or {}).get("conversation_id"), task_id=task_id)
                    if item.get("type") == "notification" and item.get("method") == "session.event":
                        event_payload = item.get("payload", {})
                        if event_payload.get("sessionId") == session["session_id"] and event_payload.get("event", {}).get("type") == "agent/inbox/spliced":
                            self.store.patch("dsh_session", session["id"], has_history=True)
                    if item.get("type") == "result":
                        final = item
                    if item.get("type") == "error":
                        if not await asyncio.to_thread(self.control.allows_progress, research_id, generation):
                            raise ResearchStopped("研究终止后到达的运行时错误只归档")
                        raise RuntimeBlocked(item.get("code", "dsh_error"), item.get("message", "Harness 执行失败"),
                            http_status=item.get("http_status"), provider_error_code=item.get("provider_error_code"))
                await proc.wait()
                exit_confirmed = True
                await stderr_task
                if not final or proc.returncode:
                    if not await asyncio.to_thread(self.control.allows_progress, research_id, generation):
                        raise ResearchStopped("研究已终止，Harness 进程已退出")
                    raise RuntimeBlocked("dsh_interrupted", "Harness 未产生完整的合法结果；已保留原始事件")
                if not await asyncio.to_thread(self.control.allows_progress, research_id, generation):
                    raise ResearchStopped("迟到结果只归档，不推进研究")
                self.store.patch("dsh_session", session["id"], has_history=True)
                if final.get("finish_reason") == "max-tokens":
                    failure = truncated_output_error()
                    raise RuntimeBlocked(failure["code"], failure["message"])
                result = parse_final_object(final["final_response"], schema,
                    finish_reason=final.get("finish_reason"),
                    record_repair=lambda audit: self.store.emit(research_id, "agent.json_repair", {
                        **audit, "execution_attempt_id": execution_id},
                        agent_id=agent_id, execution_id=execution_id, task_id=task_id))
                message_id = self.gateway.last_message(capability) if capability else None
                self.store.complete_model_turn(logical_key, result, session_id=session["id"],
                    fields={"finish_reason": final.get("finish_reason"), "message_id": message_id,
                        "actual_model": final.get("actual_model", "unknown")},
                    payload={"mode": "live", "execution_attempt_id": execution_id, "finish_reason": final.get("finish_reason")})
                turn_completed = True
                return result
            except BaseException as error:
                exit_confirmed = await asyncio.to_thread(kill_tree, proc.pid)
                cancelled = isinstance(error, (ResearchStopped, asyncio.CancelledError))
                safe_error = {"code": getattr(error, "code", type(error).__name__),
                    "message": str(error) if isinstance(error, RuntimeBlocked) else "研究已停止，执行已取消。" if cancelled else "DSH 运行失败，请检查本地运行时。",
                    "http_status": getattr(error, "http_status", None), "provider_error_code": getattr(error, "provider_error_code", None)}
                try:
                    self.store.patch("execution", logical_key, status="unknown" if not exit_confirmed else "interrupted" if isinstance(error, (ResearchStopped, asyncio.CancelledError)) else "failed",
                                     error_code=safe_error["code"], error_message=safe_error["message"], error=safe_error,
                                     http_status=safe_error["http_status"], provider_error_code=safe_error["provider_error_code"], completed_at=datetime.now(timezone.utc).isoformat())
                    self.store.emit(research_id, "agent.cancelled" if isinstance(error, (ResearchStopped, asyncio.CancelledError)) else "agent.failed",
                        {"mode": "live", "execution_attempt_id": execution_id, **safe_error,
                         "exit_confirmed": exit_confirmed}, agent_id=agent_id, execution_id=execution_id, task_id=task_id)
                except Exception as persistence_error:
                    if not is_storage_busy(persistence_error):
                        raise
                    # The independent exit receipt below remains writable while
                    # business SQLite is busy; recovery reconciles the old row.
                    print(json.dumps({"event": "agent.failure_persistence_deferred", "code": "storage_busy",
                        "execution_id": execution_id, "exit_confirmed": exit_confirmed}), flush=True)
                raise
            finally:
                self.processes.pop(execution_id, None)
                if capability and self.gateway:
                    try:
                        self.gateway.revoke(capability, interrupted=not turn_completed)
                    except Exception as cleanup_error:
                        if not is_storage_busy(cleanup_error):
                            raise
                if self.runner and hasattr(self.runner, "cancel_light_execution"):
                    try:
                        await asyncio.to_thread(self.runner.cancel_light_execution, execution_id)
                    except Exception:
                        pass
                try:
                    if exit_confirmed:
                        await asyncio.to_thread(self.control.complete_execution, execution_id)
                except Exception:
                    pass

    def get_execution(self, execution_id):
        return next((e for e in self.store.list("execution") if e.get("execution_attempt_id") == execution_id), None)

    def message_reference(self, research_id, agent_id, task_id, turn_id):
        record = self.store.get("execution", digest([research_id, agent_id, task_id, turn_id]))
        return record.get("message_id") if record else None

    async def cancel_execution(self, execution_id):
        proc = self.processes.get(execution_id)
        return {"execution_id": execution_id, "exit_confirmed": await asyncio.to_thread(kill_tree, proc.pid) if proc else False}

    def export_session(self, session_id):
        return {"session_id": session_id, "executions": [e for e in self.store.list("execution") if e.get("session_id") == session_id]}

    def reset_context(self, research_id, agent_id, direction_epoch):
        return self.ensure_session(research_id, agent_id, direction_epoch)

    async def close(self):
        for proc in list(self.processes.values()):
            await asyncio.to_thread(kill_tree, proc.pid)
        if self.gateway:
            await asyncio.to_thread(self.gateway.close)
