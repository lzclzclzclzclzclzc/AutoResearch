"""Capability-scoped research tools; model tools never receive service secrets."""
from __future__ import annotations

import json
import hashlib
import secrets
import threading
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from packages.contracts.research import digest
from backend.adapters.inputs import InputAccessError, ResearchInputs
from backend.persistence.resilience import is_storage_busy
from backend.runtime.light_cpu import validate_numeric_code


class ResearchToolArgumentError(ValueError):
    """Only deliberately public validation errors may be returned to a model."""


def visible_markdown(raw: str) -> str:
    """Decode only actually received characters of a JSON content_markdown value."""
    if not raw.lstrip().startswith("{"):
        return raw
    match = re.search(r'"content_markdown"\s*:\s*"', raw)
    if not match:
        return ""
    text, result, index = raw[match.end():], [], 0
    escapes = {'"': '"', "\\": "\\", "/": "/", "b": "\b", "f": "\f", "n": "\n", "r": "\r", "t": "\t"}
    while index < len(text):
        value = text[index]
        if value == '"':
            break
        if value != "\\":
            result.append(value)
            index += 1
            continue
        if index + 1 >= len(text):
            break
        escaped = text[index + 1]
        if escaped in escapes:
            result.append(escapes[escaped])
            index += 2
        elif escaped == "u":
            if index + 6 > len(text):
                break
            try:
                point = int(text[index + 2:index + 6], 16)
            except ValueError:
                break
            index += 6
            if 0xD800 <= point <= 0xDBFF:
                if index + 6 > len(text) or text[index:index+2] != "\\u":
                    break
                try:
                    low = int(text[index + 2:index + 6], 16)
                except ValueError:
                    break
                if not 0xDC00 <= low <= 0xDFFF:
                    break
                point = 0x10000 + (point - 0xD800) * 0x400 + low - 0xDC00
                index += 6
            result.append(chr(point) if not 0xDC00 <= point <= 0xDFFF else "\ufffd")
        else:
            break
    return "".join(result)


class ToolGateway:
    def __init__(self, store, control, runner):
        self.store, self.control, self.runner = store, control, runner
        self.capabilities = {}
        self.lock = threading.RLock()
        gateway = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_POST(self):
                try:
                    if self.path != "/tool":
                        raise ValueError("Unknown tool endpoint")
                    length = int(self.headers.get("Content-Length", "0"))
                    if length <= 0 or length > 1024 * 1024:
                        raise ValueError("Invalid tool payload")
                    capability = self.headers.get("Authorization", "").removeprefix("Bearer ")
                    payload = json.loads(self.rfile.read(length))
                    result = gateway.call(capability, payload)
                    body, status = json.dumps(result, ensure_ascii=False).encode(), 200
                except Exception as error:
                    from backend.runtime.control import ControlUnavailable
                    busy = is_storage_busy(error)
                    unavailable = isinstance(error, ControlUnavailable) and error.code == "control_unavailable"
                    status = 503 if busy or unavailable else 403 if isinstance(error, PermissionError) else 422
                    body = json.dumps({"code": "storage_busy" if busy else "local_service_unavailable" if unavailable else "research_input_unavailable" if isinstance(error, InputAccessError) else "tool_rejected",
                        "message": "研究记录暂时忙，使用同一请求重试" if busy else "本机控制服务暂时不可用，稍后重试" if unavailable else error.public_message if isinstance(error, InputAccessError) else str(error) if isinstance(error, ResearchToolArgumentError) else "科研工具请求未获授权或参数无效",
                        "retryable": busy or unavailable}, ensure_ascii=False).encode()
                try:
                    self.send_response(status)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.send_header("Content-Length", str(len(body)))
                    if status == 503:
                        self.send_header("Retry-After", "1")
                    self.end_headers()
                    self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError):
                    pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.url = f"http://127.0.0.1:{self.server.server_port}/tool"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True, name="research-tools")
        self.thread.start()

    def register(self, **context):
        token = secrets.token_urlsafe(32)
        with self.lock:
            self.capabilities[token] = context
        return token

    def revoke(self, token, interrupted=False):
        with self.lock:
            context = self.capabilities.pop(token, None)
        if context and interrupted:
            for stream in context.get("stream_records", {}).values():
                if stream.get("message_id") and not stream.get("finished"):
                    self.store.complete_stream_message(stream["message_id"], stream["visible"], status="interrupted")

    def last_message(self, token):
        with self.lock:
            context = self.capabilities.get(token, {})
            records = context.get("stream_records", {})
            return next((record.get("message_id") for record in reversed(list(records.values())) if record.get("message_id")), None)

    def call(self, token, payload):
        with self.lock:
            context = self.capabilities.get(token)
        if not context or not self.control.allows_progress(context["research_id"], context["control_generation"]):
            raise PermissionError("Expired execution capability")
        research_id, role, agent_id = context["research_id"], context["role"], context["agent_id"]
        name, args = payload.get("name"), payload.get("arguments", {})
        if name == "record_stream_event":
            return self.record_stream(context, args)
        if context.get("suppress_chat"):
            raise PermissionError("Private scheduling scores cannot use research tools")
        if role == "god":
            # God has a separate allowlist: ordinary role exclusions are unsafe
            # for a new role, especially evidence, memory and messaging tools.
            if name != "read_arbitration_context":
                raise PermissionError("God can only read its bound arbitration snapshot")
            request = self.store.get("arbitration_request", context.get("arbitration_id", ""))
            if not request or request["research_id"] != research_id or request["snapshot_hash"] != context.get("snapshot_hash"):
                raise PermissionError("Arbitration snapshot binding mismatch")
            snapshot = self.store.get("arbitration_snapshot", request["snapshot_id"])
            if not snapshot or snapshot["research_id"] != research_id:
                raise PermissionError("Missing arbitration snapshot")
            sections = snapshot.get("sections", snapshot.get("context", {}))
            section = args.get("section")
            if section not in sections:
                raise PermissionError("Section is outside the frozen arbitration context")
            offset = args.get("offset", 0)
            if type(offset) is not int or offset < 0:
                raise ValueError("Snapshot offset must be a nonnegative integer")
            value = sections[section]
            text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, sort_keys=True)
            page = text[offset:offset + 16384]
            return {"arbitration_id": request["id"], "snapshot_hash": request["snapshot_hash"],
                "section": section, "sha256": hashlib.sha256(text.encode()).hexdigest(), "offset": offset,
                "next_offset": offset + len(page), "total_length": len(text), "text": page}
        if name == "read_arbitration_context":
            raise PermissionError("Only God may read a bound arbitration snapshot")
        if name in {"list_research_inputs", "read_research_input"}:
            if role not in {"exploration", "critic", "implementation", "verify", "boss"}:
                raise PermissionError("This role cannot read mutable human inputs")
            inputs = ResearchInputs(self.store, research_id, context,
                lambda: self.control.allows_progress(research_id, context["control_generation"]))
            if name == "list_research_inputs":
                return inputs.list(args.get("path", ""), args.get("offset", 0))
            return inputs.read(args["relative_path"], args.get("offset", 0))
        if name == "read_research_evidence":
            artifact, path = self.store.artifact(args["artifact_id"])
            if artifact["research_id"] != research_id:
                raise PermissionError("Evidence belongs to another project")
            if role == "organizer":
                raise PermissionError("Organizer receives resource metadata only")
            line_id = context.get("line_id")
            if role in {"exploration", "critic", "implementation", "verify"}:
                if not line_id or artifact.get("line_id") != line_id:
                    raise PermissionError("Raw evidence from another research line is unavailable")
            if role == "paper" and args["artifact_id"] not in context.get("permitted_artifact_ids", []):
                raise PermissionError("Report only reads its frozen evidence snapshot")
            with path.open("rb") as handle:
                if hashlib.file_digest(handle, "sha256").hexdigest() != artifact["sha256"]:
                    raise PermissionError("Evidence integrity check failed")
            offset = max(0, int(args.get("offset", 0)))
            with path.open("rb") as handle:
                handle.seek(offset)
                content = handle.read(128 * 1024)
            return {"artifact_id": artifact["id"], "sha256": artifact["sha256"], "offset": offset,
                "next_offset": offset + len(content), "total_bytes": artifact["size_bytes"], "text": content.decode("utf-8", errors="replace")}
        if name == "read_shared_memory":
            if role == "organizer":
                raise PermissionError("Organizer receives scheduling metadata only")
            node_id = args.get("node_id", "")
            nodes = self.store.list("memory_node", research_id)
            selected = [n for n in nodes if n.get("node_id", "") == node_id or n.get("parent_id", "") == node_id]
            if not node_id and not selected:
                selected = [{k: n.get(k) for k in ("node_id", "parent_id", "title", "status", "memory_version")} for n in nodes]
            return {"memory_version": self.store.project(research_id)["memory_version"], "nodes": selected}
        if name == "run_light_cpu":
            if role not in {"exploration", "critic", "implementation", "verify"}:
                raise PermissionError("Role cannot run computations")
            try:
                validate_numeric_code(args["code"])
            except (SyntaxError, ValueError) as error:
                raise ResearchToolArgumentError("轻量计算 Python 语法错误：" + str(error)[:400]
                    + "。支持普通 Python，不限制导入、函数、属性或调用；文件操作须遵守工作区可写、用户数据目录只读的约束。请修正语法后重试。") from error
            return self.runner.run_light_cpu({"research_id": research_id, "agent_id": agent_id, "role": role,
                "line_id": context.get("line_id"), "code": args["code"], "evidence_ids": args.get("evidence_ids", []),
                "control_generation": context["control_generation"], "parent_execution_id": context["execution_id"],
                "parent_pid": context.get("sdk_owner_pid"), "parent_create_time": context.get("sdk_owner_create_time"),
                "execution_id": context["execution_id"] + ":light:" + str(payload.get("call_id", "unknown"))})
        if name == "send_research_message":
            if not isinstance(payload.get("call_id"), str) or not payload["call_id"]:
                raise ValueError("Communication needs a stable SDK call identity")
            recipients = args.get("recipient_ids", [])
            if not recipients or not isinstance(args.get("content_markdown"), str) or not args["content_markdown"].strip():
                raise ValueError("Recipients and message content are required")
            topology = self.store.get("topology", research_id)
            known = {n["id"]: n["type"] for n in topology["nodes"]}
            line_id = context.get("line_id")
            for recipient in recipients:
                if recipient not in known:
                    raise PermissionError("Unknown recipient")
                own_line = bool(line_id and recipient.startswith(line_id + "/"))
                if role not in {"boss", "paper"} and not (own_line or recipient in {"boss", "organizer"}):
                    raise PermissionError("Cross-line raw communication is not authorized")
                if role == "paper" and recipient != "boss":
                    raise PermissionError("Paper communicates with Boss")
            return self.store.deliver_agent_message(context, recipients, args["content_markdown"], payload["call_id"], self.control)
        raise PermissionError("Unregistered tool")

    def record_stream(self, context, args):
        from backend.services.credentials import redact
        # Scheduling scores are already retained once in the SDK result trace.
        if context.get("suppress_chat"):
            return {"accepted": True, "seq": 0}
        # A wire cursor is accepted once before any cumulative buffer mutation.
        # Replayed chunks cannot corrupt the next snapshot's prefix.
        with context.setdefault("stream_lock", threading.RLock()):
            chunk = json.loads(redact(json.dumps(args.get("chunk", {}), ensure_ascii=False), context.get("redact_secret")))
            stream_id, cursor = str(args.get("stream_id")), int(args.get("cursor", 0))
            if cursor < 1:
                raise ValueError("A positive stable stream cursor is required")
            records = context.setdefault("stream_records", {})
            previous = records.get(stream_id, {"raw": "", "visible": "", "finished": False, "seen": {}, "last_cursor": 0})
            fingerprint = digest(chunk)
            duplicate = previous["seen"].get(cursor)
            if duplicate:
                if duplicate["hash"] != fingerprint:
                    raise ValueError("Same stream cursor carried different content")
                return {"accepted": True, "seq": duplicate["seq"]}
            if cursor <= previous["last_cursor"] or previous["finished"]:
                raise ValueError("Out of order or already closed stream")
            stream = {**previous, "seen": dict(previous["seen"])}
            event_seq = previous.get("event_seq", 0)
            if chunk.get("type") == "text-delta":
                stream["raw"] = previous["raw"] + chunk.get("text", "")
                visible = visible_markdown(stream["raw"])
                if visible and visible != previous["visible"] and not context.get("suppress_chat"):
                    stream["message_id"] = "stream-" + digest([context["execution_id"], stream_id])[:32]
                    message = self.store.upsert_stream_message(context["research_id"], stream["message_id"], context["conversation_id"], context["agent_id"], visible,
                        source_event_id=context["execution_id"] + ":stream:" + stream_id,
                        chunk_id=str(cursor), content_version=1, execution_id=context["execution_id"],
                        task_id=context["task_id"], line_id=context.get("line_id"), kind="discussion")
                    event_seq = message.get("latest_event_seq", message.get("event_seq", 0))
                    stream["visible"] = visible
            elif chunk.get("type") == "finish":
                if stream.get("message_id"):
                    reason = chunk.get("reason", {}).get("kind")
                    self.store.complete_stream_message(stream["message_id"], stream["visible"],
                        status="interrupted" if reason in {"error", "aborted", "max-tokens", "max_tokens", "length"} else "complete")
                stream["finished"] = True
            if chunk.get("type") != "text-delta":
                event = self.store.emit(context["research_id"], "agent.stream_event",
                    {"execution_attempt_id": context["execution_id"], "stream_id": args.get("stream_id"), "source_cursor": cursor,
                     "chunk": chunk}, agent_id=context["agent_id"],
                    conversation_id=context["conversation_id"], task_id=context["task_id"])
                event_seq = event["seq"]
            stream["event_seq"] = event_seq
            stream["seen"][cursor] = {"hash": fingerprint, "seq": event_seq}
            stream["last_cursor"] = cursor
            records[stream_id] = stream
            return {"accepted": True, "seq": event_seq}

    def close(self):
        self.server.shutdown()
        self.server.server_close()
