from __future__ import annotations

import asyncio
import hashlib
import json
import os
import secrets
import sqlite3
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse

import httpx
from fastapi import FastAPI, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from backend.adapters.provider_endpoints import DEFAULT_MESSAGES_BASE_URL, messages_base_url, messages_url
from backend.adapters.token_limits import CONNECTION_OUTPUT_TOKENS
from backend.persistence import Store, StoreError, canonical, new_id, now
from backend.persistence.store import DEFAULT_TEAM, TERMINAL, build_topology
from backend.services.credentials import CredentialService, redact


class Input(BaseModel):
    model_config = ConfigDict(extra="forbid")
    client_request_id: str | None = Field(default=None, max_length=200)


class Team(Input):
    version: int | None = None
    n: int = Field(default=2, ge=1, le=16)
    m1: int = Field(default=2, ge=1, le=8)
    m2: int = Field(default=2, ge=1, le=8)
    k: int = Field(default=3, ge=1, le=20)
    boss_members: int = Field(default=3, ge=1, le=9)
    max_runtime_seconds: int = Field(default=3600, ge=1, le=31536000)
    model_concurrency: int = Field(default=4, ge=1, le=64)
    experiment_concurrency: int = Field(default=1, ge=1, le=16)


class Research(Input):
    title: str = Field(min_length=1, max_length=200)
    topic: str = Field(min_length=1, max_length=10000)
    description: str = Field(default="", max_length=50000)
    team: dict | None = None
    provider_config_id: str = "deepseek-default"
    resource_profile_id: str = "local-default"
    max_runtime_seconds: int | None = Field(default=None, ge=1, le=31536000)
    mode: Literal["live", "demo"] = "live"
    workspace_path: str | None = Field(default=None, max_length=4000)
    model_concurrency: int | None = Field(default=None, ge=1, le=64)
    experiment_concurrency: int | None = Field(default=None, ge=1, le=16)

    @field_validator("topic")
    @classmethod
    def meaningful_topic(cls, value):
        if not value.strip():
            raise ValueError("请输入研究题目")
        return value.strip()

    @field_validator("title")
    @classmethod
    def meaningful_title(cls, value):
        if not value.strip():
            raise ValueError("请填写任务名称")
        return value.strip()


class Provider(Input):
    version: int | None = None
    name: str = Field(default="DeepSeek", min_length=1, max_length=100)
    display_name: str = Field(default="DeepSeek V4.1 Flash", min_length=1, max_length=200)
    model: str = Field(default="deepseek-flash", min_length=1, max_length=200)
    base_url: str = DEFAULT_MESSAGES_BASE_URL
    api_key: str | None = Field(default=None, max_length=10000, repr=False)

    @field_validator("base_url")
    @classmethod
    def valid_url(cls, value):
        parsed = urlparse(value)
        if parsed.scheme not in {"https", "http"} or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("Base URL 必须是有效的 HTTP(S) 地址，且不包含凭据或查询参数")
        if parsed.scheme == "http" and parsed.hostname not in {"localhost", "127.0.0.1", "::1"}:
            raise ValueError("远程模型服务必须使用 HTTPS")
        return messages_base_url(value)


class Command(Input):
    kind: Literal["pause", "resume", "retry", "recover", "rebind_credential"]
    provider_config_id: str | None = None


class Export(Input):
    kind: Literal["all", "report", "submissions", "conversations"] = "all"


def _public(value):
    if isinstance(value, dict):
        return {k: _public(v) for k, v in value.items() if k not in {"api_key", "secret", "credential_ref", "authorization"}}
    if isinstance(value, list):
        return [_public(x) for x in value]
    return value


def create_app(data_dir: str | Path | None = None, control_client=None, *, testing=False):
    root = Path(data_dir or os.environ.get("AUTORESEARCH_DATA_DIR", "data")).resolve()
    app = FastAPI(title="AutoResearch Local API", version="0.1.0")
    app.state.data_dir = root
    app.state.store = None
    app.state.session_id = secrets.token_urlsafe(32)
    app.state.csrf_token = secrets.token_urlsafe(32)
    app.state.stop_commands = {}
    # Control requests never join the business SQLite executor or wait for its locks.
    app.state.control_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="independent-control")
    if control_client is None:
        from backend.runtime.control import ControlClient
        control_client = ControlClient()
    app.state.control = control_client

    def store() -> Store:
        if app.state.store is None:
            app.state.store = Store(root)
        return app.state.store

    async def control(method, *args, **kwargs):
        import functools
        return await asyncio.get_running_loop().run_in_executor(app.state.control_executor,
            functools.partial(getattr(app.state.control, method), *args, **kwargs))

    @app.middleware("http")
    async def local_session(request: Request, call_next):
        host = request.url.hostname
        if host not in {"localhost", "127.0.0.1", "::1"} and not (testing and host == "testserver"):
            return JSONResponse({"code": "invalid_host", "message": "仅允许本机访问"}, status_code=403)
        origin = request.headers.get("origin")
        if origin and origin != f"{request.url.scheme}://{request.url.netloc}":
            return JSONResponse({"code": "invalid_origin", "message": "请求来源不匹配"}, status_code=403)
        if request.headers.get("sec-fetch-site") == "cross-site":
            return JSONResponse({"code": "cross_site_request", "message": "拒绝跨站请求"}, status_code=403)
        if request.url.path.startswith("/api/") and request.url.path not in {"/api/session", "/api/health"}:
            cookie = request.cookies.get("autoresearch_session", "")
            if not secrets.compare_digest(cookie, app.state.session_id):
                return JSONResponse({"code": "local_session_required", "message": "请刷新页面建立本机会话"}, status_code=401)
            if request.method not in {"GET", "HEAD", "OPTIONS"} and not secrets.compare_digest(request.headers.get("x-csrf-token", ""), app.state.csrf_token):
                return JSONResponse({"code": "csrf_rejected", "message": "会话校验失败，请刷新页面"}, status_code=403)
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "same-origin"
        response.headers["Cache-Control"] = "no-store" if request.url.path.startswith("/api/") else "no-cache"
        return response

    @app.exception_handler(StoreError)
    async def business_error(request, exc):
        return JSONResponse({"code": exc.code, "message": exc.message}, status_code=exc.status)

    @app.exception_handler(RequestValidationError)
    async def validation_error(request, exc):
        # Pydantic errors contain input values; never echo a rejected API key.
        fields = [{"field": ".".join(str(x) for x in e["loc"]), "message": e["msg"]} for e in exc.errors()]
        return JSONResponse({"code": "validation_error", "message": "输入参数无效", "fields": fields}, status_code=422)

    @app.exception_handler(ValidationError)
    async def nested_validation_error(request, exc):
        fields = [{"field": ".".join(str(x) for x in e["loc"]), "message": e["msg"]} for e in exc.errors()]
        return JSONResponse({"code": "validation_error", "message": "团队配置无效", "fields": fields}, status_code=422)

    @app.exception_handler(sqlite3.Error)
    async def db_error(request, exc):
        return JSONResponse({"code": "storage_unavailable", "message": "业务存储暂不可用；立即终止与独立控制状态仍可使用"}, status_code=503)

    @app.exception_handler(OSError)
    async def filesystem_error(request, exc):
        return JSONResponse({"code": "filesystem_unavailable", "message": "本机文件存储不可用，请检查空间与目录权限；停止通道仍独立工作"}, status_code=503)

    @app.exception_handler(Exception)
    async def internal_error(request, exc):
        return JSONResponse({"code": "service_unavailable", "message": "服务暂不可用，请检查本机服务状态后重试"}, status_code=503)

    def request_id(request, payload):
        value = payload.client_request_id or request.headers.get("idempotency-key")
        if not value:
            raise StoreError("request_id_required", "写操作需要 client_request_id 或 Idempotency-Key", 422)
        return value

    @app.get("/api/session")
    async def session(response: Response):
        response.set_cookie("autoresearch_session", app.state.session_id, httponly=True, samesite="strict", max_age=86400, path="/")
        return {"csrf_token": app.state.csrf_token, "local_only": True}

    @app.get("/api/health")
    async def health():
        try:
            ctrl = await control("status")
            control_health = {"available": True, **ctrl}
        except Exception:
            control_health = {"available": False, "code": "control_unavailable", "message": "独立控制通道不可达，启动门禁关闭"}
        try:
            s = await asyncio.to_thread(store)
            worker = await asyncio.to_thread(s.get, "system", "worker")
            database = "available"
        except Exception:
            worker, database = None, "unavailable"
        from backend.adapters.dsh import runtime_availability
        runtime = await asyncio.to_thread(runtime_availability)
        return {"status": "ok" if database == "available" and control_health["available"] else "degraded",
            "database": database, "control": control_health, "worker": worker,
            "runtime": (worker or {}).get("dsh", runtime), "version": "0.1.0"}

    @app.get("/api/settings/team")
    def get_team():
        team = store().get("settings", "team")
        return {k:v for k,v in team.items() if k != "id"}

    @app.put("/api/settings/team")
    def save_team(body: Team, request: Request):
        s = store()
        values = body.model_dump(exclude={"client_request_id", "version"})
        def save(c):
            old = s._get(c, "settings", "team")
            if body.version is None or old["version"] != body.version:
                raise StoreError("version_conflict", "配置已变化，请重新读取后保存")
            return s._put(c, "settings", "team", values)
        return s.idempotent("team", request_id(request, body), values, save)

    @app.post("/api/settings/team/preview")
    def preview_team(body: Team):
        return build_topology(body.model_dump())

    @app.get("/api/settings/providers")
    def providers():
        s = store()
        credentials = CredentialService(root, s)
        return {"items": [credentials.public(p) for p in s.list("provider")]}

    @app.put("/api/settings/providers/{provider_id}")
    def save_provider(provider_id: str, body: Provider, request: Request):
        s = store()
        rid = request_id(request, body)
        # Hash inputs only; no plaintext key enters idempotency storage.
        payload = body.model_dump(exclude={"client_request_id", "api_key"})
        payload["credential_digest"] = hashlib.sha256((body.api_key or "").encode()).hexdigest()
        with s.connect() as c:
            previous = c.execute("SELECT request_hash,result FROM idempotency WHERE scope=? AND request_id=?", (f"provider:{provider_id}", rid)).fetchone()
            if previous:
                if previous["request_hash"] != hashlib.sha256(canonical(payload).encode()).hexdigest():
                    raise StoreError("idempotency_conflict", "同一请求 ID 的配置发生变化")
                return json.loads(previous["result"])
        service = CredentialService(root, s)
        prepared = service.prepare(body.api_key) if body.api_key else {}
        old_ref = None
        def save(c):
            nonlocal old_ref
            old = s._get(c, "provider", provider_id)
            if old and (body.version is None or old["version"] != body.version):
                raise StoreError("version_conflict", "模型配置已变化，请重新读取后保存")
            values = {**(old or {}), **body.model_dump(exclude={"client_request_id", "api_key", "version"}), **prepared, "test_status": "untested"}
            if prepared:
                values["credential_version"] = (old or {}).get("credential_version", 0)+1
                old_ref = (old or {}).get("credential_ref")
            return service.public(s._put(c, "provider", provider_id, values))
        try:
            result = s.idempotent(f"provider:{provider_id}", rid, payload, save)
        except Exception:
            service.discard(prepared.get("credential_ref"))
            raise
        service.discard(old_ref)
        return result

    @app.delete("/api/settings/providers/{provider_id}/credential")
    def delete_credential(provider_id: str, request: Request, client_request_id: str | None = None):
        rid = client_request_id or request.headers.get("idempotency-key")
        if not rid:
            raise StoreError("request_id_required", "删除凭据需要幂等请求 ID", 422)
        s = store()
        existing = s.get("credential_delete", rid)
        if existing:
            return existing
        result = CredentialService(root, s).delete(provider_id)
        result["impact"] = "受影响的研究已阻塞新增模型调用；已注入的环境变量凭据请在启动环境中移除。"
        s.put("credential_delete", rid, result)
        return result

    @app.post("/api/settings/providers/{provider_id}/test")
    async def test_provider(provider_id: str, body: Input, request: Request):
        rid = request_id(request, body)
        s = await asyncio.to_thread(store)
        previous = await asyncio.to_thread(s.get, "provider_test", rid)
        if previous:
            if previous.get("provider_id") != provider_id:
                raise StoreError("idempotency_conflict", "请求 ID 已用于其他模型配置")
            return previous
        p = await asyncio.to_thread(s.get, "provider", provider_id)
        if not p:
            raise StoreError("provider_not_found", "模型配置不存在", 404)
        secret = await asyncio.to_thread(CredentialService(root, s).get_secret, provider_id)
        if not secret:
            raise StoreError("credential_missing", "请先配置 API Key", 422)
        result = {"provider_id": provider_id, "credential_valid": False, "model_available": False, "streaming_contract_verified": False,
            "tools_contract_verified": False, "tested_at": now(), "test_kind": "messages_model_call"}
        try:
            async with httpx.AsyncClient(timeout=30, follow_redirects=False) as client:
                response = await client.post(messages_url(p.get("base_url")),
                    headers={"x-api-key": secret, "anthropic-version": "2023-06-01"},
                    json={"model": p["model"], "messages": [{"role": "user", "content": "Reply OK."}],
                        "max_tokens": CONNECTION_OUTPUT_TOKENS, "stream": False, "thinking": {"type": "disabled"}})
                result["http_status"] = response.status_code
                result["credential_valid"] = True if response.is_success else False if response.status_code in {401, 403} else None
                if response.is_success:
                    data = response.json()
                    valid_message = (isinstance(data, dict) and data.get("type") == "message"
                        and isinstance(data.get("content"), list) and any(isinstance(part, dict)
                            and part.get("type") == "text" and isinstance(part.get("text"), str) and part["text"].strip()
                            for part in data["content"]))
                    result["model_available"] = bool(valid_message)
                    if valid_message:
                        returned_model = data.get("model")
                        if isinstance(returned_model, str):
                            result["returned_model"] = redact(returned_model, secret)[:200]
                        result["message"] = "Messages 最小模型调用成功；工具与流式合同尚未验证"
                    elif isinstance(data, dict) and data.get("type") == "message" and data.get("stop_reason") == "max_tokens":
                        result["error_code"] = "model_response_truncated"
                        result["message"] = "服务已响应，但输出达到 token 上限前未生成有效文本；本次测试未通过，请增加测试输出额度后重试"
                    else:
                        result["message"] = "Messages 响应格式无效：需要 type=message 且包含非空 text 内容；连接测试未通过"
                elif response.status_code == 404:
                    result["message"] = "Messages 接口返回 HTTP 404；DeepSeek 官方地址应为 https://api.deepseek.com/anthropic，自定义服务需支持 /v1/messages"
                else:
                    result["message"] = f"Messages 服务返回 HTTP {response.status_code}，请检查凭据、模型名称及 Messages 地址"
        except Exception as exc:
            result["message"] = f"连接测试失败：{type(exc).__name__}"
        await asyncio.to_thread(s.patch, "provider", provider_id, test_status="passed" if result["model_available"] else "failed", last_test=result)
        return await asyncio.to_thread(s.put, "provider_test", rid, result)

    @app.get("/api/resources")
    async def resources():
        from backend.runtime.runner import RunnerClient, detect_resources
        try:
            result = await asyncio.to_thread(RunnerClient().resources)
            return {"available": True, **result}
        except Exception:
            return {"available": False, "capacity": await asyncio.to_thread(detect_resources), "allocations": [],
                    "message": "Runner 不可达，资源授权与实验启动已关闭"}

    @app.post("/api/research", status_code=201)
    def create_research(body: Research, request: Request):
        s = store()
        values = body.model_dump(exclude_none=True)
        values["client_request_id"] = request_id(request, body)
        values["_client_payload_hash"] = hashlib.sha256(canonical(values).encode()).hexdigest()
        # Validate nested team with the same strict field rules as the settings page.
        merged = {**DEFAULT_TEAM, **(s.get("settings", "team") or {}), **(body.team or {})}
        for field in ("model_concurrency", "experiment_concurrency"):
            if getattr(body, field) is not None:
                merged[field] = getattr(body, field)
        merged.pop("id", None)
        merged.pop("version", None)
        values["team"] = Team.model_validate(merged).model_dump(exclude={"client_request_id", "version"})
        if body.workspace_path:
            path = Path(body.workspace_path).expanduser().resolve()
            if not path.is_dir():
                raise StoreError("workspace_not_found", "指定的本机工作区目录不存在", 422)
            values["workspace_path"] = str(path)
        p = s.create_project(values)
        return {**_public(p), "topology": s.get("topology", p["id"])}

    @app.get("/api/research")
    def research_list(include_deleted: bool = False):
        return {"items": _public(store().projects(include_deleted=include_deleted))}

    @app.get("/api/research/{research_id}/configuration")
    def research_configuration(research_id: str):
        project = store().project(research_id)
        if project.get("deleted_at"):
            raise StoreError("research_deleted", "请先从已删除记录中恢复该任务", 404)
        team = project["config_snapshot"]["team"]
        return {"research_id": research_id, "title": project["title"], "topic": project["topic"],
            "description": project.get("description", ""), "workspace_path": project.get("workspace_path"),
            "mode": project["mode"], "team": {key: team[key] for key in DEFAULT_TEAM if key in team},
            "max_runtime_seconds": project["max_runtime_seconds"],
            "model_concurrency": team["model_concurrency"], "experiment_concurrency": team["experiment_concurrency"],
            "provider_config_id": project["provider_config_id"],
            "resource_profile_id": project["config_snapshot"].get("resource_profile_id", "local-default")}

    @app.delete("/api/research/{research_id}")
    def delete_research(research_id: str, request: Request, client_request_id: str | None = None):
        key = client_request_id or request.headers.get("idempotency-key")
        if not key:
            raise StoreError("request_id_required", "删除记录需要幂等请求 ID", 422)
        return store().set_project_deleted(research_id, True, key)

    @app.post("/api/research/{research_id}/restore")
    def restore_research(research_id: str, body: Input, request: Request):
        return store().set_project_deleted(research_id, False, request_id(request, body))

    @app.get("/api/research/{research_id}/snapshot")
    def snapshot(research_id: str):
        return _public(store().snapshot(research_id))

    @app.get("/api/research/{research_id}/timeline")
    def timeline(research_id: str, after: int = 0, limit: int = 200, detail: bool = False):
        s = store()
        s.project(research_id)
        events = s.events(research_id, after, limit, detail=detail)
        return {"items": events, "next_cursor": events[-1]["seq"] if events else after}

    @app.get("/api/research/{research_id}/tool-calls")
    def tool_calls(research_id: str, conversation_id: str | None = None, before: int | None = Query(default=None, ge=1),
                   limit: int = 50, q: str | None = Query(default=None, max_length=1000), at_seq: int | None = Query(default=None, ge=0)):
        s = store()
        s.project(research_id)
        if conversation_id:
            conversation = s.get("conversation", conversation_id)
            if not conversation or conversation["research_id"] != research_id:
                raise StoreError("conversation_not_found", "会话不属于当前研究", 404)
        return s.tool_calls(research_id, conversation_id, before, limit, q, at_seq)

    @app.get("/api/research/{research_id}/events")
    async def stream_events(research_id: str, request: Request, after: int = 0):
        s = await asyncio.to_thread(store)
        await asyncio.to_thread(s.project, research_id)
        try:
            cursor = max(after, int(request.headers.get("last-event-id", "0")))
        except ValueError:
            raise StoreError("invalid_cursor", "事件游标无效", 422)
        async def stream():
            nonlocal cursor
            idle = 0
            while not await request.is_disconnected():
                try:
                    batch = await asyncio.to_thread(s.events, research_id, cursor, 200)
                except sqlite3.Error:
                    yield "event: storage.unavailable\ndata: {\"code\":\"storage_unavailable\"}\n\n"
                    return
                if batch:
                    for event in batch:
                        cursor = event["seq"]
                        yield f"id: {cursor}\nevent: {event['type']}\ndata: {canonical(event)}\n\n"
                    idle = 0
                else:
                    idle += 1
                    if idle % 30 == 0:
                        yield ": keepalive\n\n"
                    await asyncio.sleep(.25)
        return StreamingResponse(stream(), media_type="text/event-stream", headers={"X-Accel-Buffering": "no", "Cache-Control": "no-store"})

    @app.get("/api/research/{research_id}/conversations")
    def conversations(research_id: str, agent_id: str | None = None, group_id: str | None = None, kind: str | None = None, line_id: str | None = None):
        s = store()
        s.project(research_id)
        items = s.list("conversation", research_id)
        if kind:
            items = [x for x in items if x.get("kind") == kind]
        if line_id:
            items = [x for x in items if x.get("line_id") == line_id]
        identity = agent_id or group_id
        if identity:
            with s.connect() as c:
                ids = {row[0] for row in c.execute("""
                    SELECT DISTINCT m.conversation_id FROM messages m WHERE m.research_id=:research AND (
                      json_extract(m.data,'$.sender.id')=:identity
                      OR substr(json_extract(m.data,'$.sender.id'),1,length(:identity)+1)=:identity||'/'
                      OR (json_extract(m.data,'$.sender.type')='group' AND
                          substr(:identity,1,length(json_extract(m.data,'$.sender.id'))+1)=json_extract(m.data,'$.sender.id')||'/')
                      OR EXISTS (SELECT 1 FROM json_each(m.data,'$.recipients') r WHERE
                          json_extract(r.value,'$.id')=:identity
                          OR substr(json_extract(r.value,'$.id'),1,length(:identity)+1)=:identity||'/'
                          OR (json_extract(r.value,'$.type')='group' AND
                              substr(:identity,1,length(json_extract(r.value,'$.id'))+1)=json_extract(r.value,'$.id')||'/'))
                    )""", {"research": research_id, "identity": identity})}
            items = [x for x in items if x["conversation_id"] in ids or identity in x.get("group_ids", [])
                     or any((p.get("id") if isinstance(p, dict) else p) == identity for p in x.get("participants", []))]
        for item in items:
            latest = s.messages(item["conversation_id"], limit=1)["items"]
            item["last_message"] = latest[-1] if latest else None
        return {"items": items}

    @app.get("/api/conversations/{conversation_id:path}/messages")
    def conversation_messages(conversation_id: str, before: int | None = None, limit: int = 50, q: str | None = Query(default=None, max_length=1000), at_seq: int | None = Query(default=None, ge=0)):
        if not store().get("conversation", conversation_id):
            raise StoreError("conversation_not_found", "会话不存在", 404)
        return store().messages(conversation_id, before, limit, q=q, at_seq=at_seq)

    @app.get("/api/research/{research_id}/messages/{message_id}")
    def referenced_message(research_id: str, message_id: str, at_seq: int | None = Query(default=None, ge=0)):
        s = store()
        s.project(research_id)
        with s.connect() as c:
            row = c.execute("SELECT cursor, conversation_id FROM messages WHERE research_id=? AND message_id=?",
                            (research_id, message_id)).fetchone()
        if row:
            page = s.messages(row["conversation_id"], before=row["cursor"] + 1, limit=1, at_seq=at_seq)
            if page["items"] and page["items"][0]["message_id"] == message_id:
                return page["items"][0]
        raise StoreError("message_not_found", "当前记录中找不到引用消息", 404)

    @app.get("/api/research/{research_id}/agents/{agent_id:path}/executions")
    def agent_executions(research_id: str, agent_id: str):
        s = store()
        s.project(research_id)
        return {"items": s.list("execution", research_id, agent_id=agent_id) + s.list("agent_execution", research_id, agent_id=agent_id)}

    @app.get("/api/research/{research_id}/edges/{edge_id:path}/messages")
    def edge_messages(research_id: str, edge_id: str, before: int | None = None, limit: int = 100, q: str | None = Query(default=None, max_length=1000), at_seq: int | None = Query(default=None, ge=0)):
        s = store()
        s.project(research_id)
        return s.edge_messages(research_id, edge_id, before, min(max(limit, 1), 200), q=q, at_seq=at_seq)

    @app.get("/api/research/{research_id}/submissions/{submission_id}")
    def submission(research_id: str, submission_id: str):
        s = store()
        item = s.get("submission", submission_id)
        if not item or item.get("research_id") != research_id:
            raise StoreError("submission_not_found", "该研究中找不到提交包", 404)
        spec = s.get("spec", item.get("spec_id", "")) or s.get("experiment_spec", item.get("spec_id", ""))
        return {**item, "spec": spec, "verifications": s.list("verification", research_id, submission_version_id=submission_id)}

    @app.get("/api/research/{research_id}/memory")
    def memory(research_id: str):
        s = store()
        p = s.project(research_id)
        return {"version": p.get("memory_version", 0), "items": s.list("memory_node", research_id), "history": s.list("memory_version", research_id)}

    @app.get("/api/artifacts/{artifact_id}")
    def artifact(artifact_id: str, download: bool = False):
        info, path = store().artifact(artifact_id)
        with path.open("rb") as file:
            actual = hashlib.file_digest(file, "sha256").hexdigest()
        if actual != info["sha256"]:
            raise StoreError("artifact_integrity_failed", "产物校验失败，文件可能已改变", 409)
        media_type = (info.get("media_type") or "application/octet-stream").split(";", 1)[0].lower()
        active = media_type in {"text/html", "application/xhtml+xml", "image/svg+xml", "application/javascript",
            "text/javascript", "application/ecmascript", "text/ecmascript", "text/xml", "application/xml"} or path.suffix.lower() in {
            ".html", ".htm", ".xhtml", ".svg", ".js", ".mjs", ".cjs", ".xml"}
        attachment = download or active
        return FileResponse(path, media_type=media_type,
            filename=info["name"] if attachment else None, content_disposition_type="attachment" if attachment else "inline",
            headers={"Content-Security-Policy": "sandbox; default-src 'none'; base-uri 'none'; form-action 'none'; frame-ancestors 'self'"})

    @app.post("/api/research/{research_id}/commands", status_code=202)
    def command(research_id: str, body: Command, request: Request):
        s = store()
        rid = request_id(request, body)
        existing = s.get("command", rid)
        if existing:
            if existing.get("research_id") != research_id or existing.get("kind") != body.kind:
                raise StoreError("idempotency_conflict", "请求 ID 已用于其他研究命令")
            return existing
        p = s.project(research_id)
        s.assert_progress(p)
        if p.get("started_at") and not app.state.control.allows_progress(research_id, p["control_generation"]):
            supervisor = s.get("task", f"{research_id}:supervisor")
            if (body.kind in {"retry", "recover"} and supervisor
                    and supervisor.get("status") in {"pending", "running", "failed", "blocked"}):
                # Only unfinished initial supervision can repair a missing
                # registration. Control retains all stop/restart tombstones.
                control_status = app.state.control.status(research_id)
                if control_status.get("state") == "unknown" and not control_status.get("stop_id"):
                    app.state.control.register_project(research_id, p["deadline_at"], p["control_generation"])
            if not app.state.control.allows_progress(research_id, p["control_generation"]):
                raise StoreError("control_denied", "独立控制服务不允许继续推进", 409)
        if body.kind == "pause":
            s.update_project(research_id, status="pausing")
        elif body.kind in {"resume", "retry", "recover"}:
            if body.kind == "resume" and p["status"] not in {"paused", "pausing"}:
                raise StoreError("not_paused", "当前研究未暂停")
            s.update_project(research_id, status="running" if p["started_at"] else "created", blocked_reason=None)
            for task in s.list("task", research_id):
                if task["status"] in {"failed", "blocked"}:
                    s.patch("task", task["id"], status="pending")
            s.emit(research_id, "research.resumed", {"deadline_at": p.get("deadline_at")})
        elif body.kind == "rebind_credential":
            provider_id = body.provider_config_id or p["provider_config_id"]
            provider = s.get("provider", provider_id)
            if not provider or not CredentialService(root, s).get_secret(provider_id):
                raise StoreError("credential_missing", "目标模型凭据不可用", 422)
            s.update_project(research_id, provider_config_id=provider_id, credential_version=provider["credential_version"])
            s.emit(research_id, "credential.rebound", {"provider_config_id": provider_id, "credential_version": provider["credential_version"]})
        result = s.put("command", rid, {"command_id": rid, "kind": body.kind, "status": "applied", "created_at": now(), "version": 1}, research_id)
        return result

    @app.post("/api/research/{research_id}/terminate", status_code=202)
    async def terminate(research_id: str, body: Input, request: Request):
        rid = request_id(request, body)
        # Deliberately no Store construction, project read or business dependency here.
        try:
            result = await control("terminate", research_id, reason="user_requested", stop_id=rid)
        except Exception:
            return JSONResponse({"code": "control_unavailable", "message": "独立停止通道不可达，停止尚未确认", "confirmed": False,
                "command_id": rid, "stop_journal_persisted": False, "db_synced": False}, status_code=503)
        app.state.stop_commands[rid] = research_id
        return {"command_id": rid, "db_synced": False, **result}

    @app.get("/api/research/{research_id}/control-status")
    async def control_status(research_id: str):
        try:
            return await control("status", research_id)
        except Exception:
            return JSONResponse({"code": "control_unavailable", "message": "独立控制状态不可达，执行停止状态未确认", "confirmed": False}, status_code=503)

    @app.get("/api/commands/{command_id}")
    async def command_status(command_id: str):
        if command_id in app.state.stop_commands:
            return {"command_id": command_id, **(await control("status", app.state.stop_commands[command_id]))}
        s = await asyncio.to_thread(store)
        item = await asyncio.to_thread(s.get, "command", command_id)
        if not item:
            raise StoreError("command_not_found", "命令不存在；停止命令请查询项目独立控制状态", 404)
        return item

    @app.post("/api/research/{research_id}/exports")
    def export(research_id: str, body: Export, request: Request):
        s = store()
        rid = request_id(request, body)
        previous = s.get("export", rid)
        if previous:
            if previous.get("research_id") != research_id:
                raise StoreError("idempotency_conflict", "请求 ID 已用于其他研究导出")
            return previous
        snapshot = s.snapshot(research_id)
        directory = root / "artifacts" / research_id
        directory.mkdir(parents=True, exist_ok=True)
        target = directory / f"export-{new_id()}.zip"
        temporary = target.with_suffix(".tmp")
        with zipfile.ZipFile(temporary, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("snapshot.json", canonical(_public(snapshot)))
            for kind in ("submission", "spec", "verification", "boss_decision", "report", "memory_node", "memory_version"):
                archive.writestr(f"{kind}.json", canonical(_public(s.list(kind, research_id))))
            with s.connect() as c:
                msgs = [json.loads(r[0]) for r in c.execute("SELECT data FROM messages WHERE research_id=? ORDER BY cursor", (research_id,))]
                events = [json.loads(r[0]) for r in c.execute("SELECT data FROM events WHERE research_id=? ORDER BY seq", (research_id,))]
            archive.writestr("conversations.json", canonical(_public(msgs)))
            archive.writestr("events.json", canonical(_public(events)))
            for info in s.list("artifact", research_id):
                if info.get("purpose") == "export":
                    continue
                _, source = s.artifact(info["artifact_id"])
                archive.write(source, f"artifacts/{info['artifact_id']}/{source.name}")
        os.replace(temporary, target)
        artifact = s.register_artifact(research_id, target, "application/zip", purpose="export")
        result = {"artifact_id": artifact["artifact_id"], "download_url": f"/api/artifacts/{artifact['artifact_id']}?download=true", "version": 1}
        s.put("export", rid, result, research_id)
        return result

    # Production serves the built UI from this origin. Vite proxies /api in development.
    web_root = Path(__file__).resolve().parents[1] / "web" / "dist"
    @app.get("/{file_path:path}", include_in_schema=False)
    def spa(file_path: str):
        if file_path.startswith("api/"):
            raise StoreError("endpoint_not_found", "接口不存在", 404)
        candidate = (web_root / file_path).resolve()
        if candidate.is_relative_to(web_root) and candidate.is_file():
            return FileResponse(candidate)
        index = web_root / "index.html"
        if index.is_file():
            return FileResponse(index)
        return JSONResponse({"message": "网页尚未构建，请先运行前端构建。API 已就绪。"}, status_code=503)
    return app


app = create_app()
