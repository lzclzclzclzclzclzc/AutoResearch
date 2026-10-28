"""OS-user-bound credential storage; the business DB contains metadata only."""
from __future__ import annotations

import ctypes
import os
import re
from ctypes import wintypes
from pathlib import Path

from backend.persistence import Store, StoreError, new_id, now


def redact(value: str, secret: str | None = None) -> str:
    if secret:
        value = value.replace(secret, "[REDACTED]")
    return re.sub(r"(?i)(authorization\s*[:=]\s*(?:bearer\s+)?|api[_-]?key\s*[:=]\s*)[^\s,;]+", r"\1[REDACTED]", value)


class DATA_BLOB(ctypes.Structure):
    _fields_ = [("cbData", wintypes.DWORD), ("pbData", ctypes.POINTER(ctypes.c_byte))]


def _dpapi(data: bytes, protect: bool) -> bytes:
    if os.name != "nt":
        raise StoreError("secure_storage_unavailable", "当前系统不能使用 Windows DPAPI；可通过 DEEPSEEK_API_KEY 环境变量注入凭据", 503)
    buf = ctypes.create_string_buffer(data)
    source = DATA_BLOB(len(data), ctypes.cast(buf, ctypes.POINTER(ctypes.c_byte)))
    output = DATA_BLOB()
    crypt = ctypes.WinDLL("crypt32", use_last_error=True)
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    # UI forbidden: service processes must never open an interactive credential dialog.
    if protect:
        success = crypt.CryptProtectData(ctypes.byref(source), "AutoResearch credential", None, None, None, 1, ctypes.byref(output))
    else:
        success = crypt.CryptUnprotectData(ctypes.byref(source), None, None, None, None, 1, ctypes.byref(output))
    if not success:
        raise StoreError("credential_protection_failed", "无法使用当前 Windows 用户保护或读取凭据，请重新配置", 503)
    try:
        return ctypes.string_at(output.pbData, output.cbData)
    finally:
        kernel.LocalFree(output.pbData)


class CredentialService:
    def __init__(self, data_dir: str | Path, store: Store | None = None):
        self.store = store or Store(data_dir)
        self.root = Path(data_dir).resolve() / "credentials"
        self.root.mkdir(parents=True, exist_ok=True)

    def prepare(self, secret: str):
        if not secret or len(secret) < 8 or "\n" in secret or "\r" in secret:
            raise StoreError("invalid_credential", "API Key 格式无效", 422)
        protected = _dpapi(secret.encode("utf-8"), True)
        ref = new_id("credential")
        temp = self.root / f"{ref}.tmp"
        temp.write_bytes(protected)
        os.replace(temp, self.root / f"{ref}.dpapi")
        return {"credential_ref": ref, "credential_mask": f"••••{secret[-4:]}",
                "configured": True, "credential_updated_at": now(), "credential_disabled": False}

    def discard(self, ref: str | None):
        if ref and re.fullmatch(r"credential-[a-f0-9]{32}", ref):
            (self.root / f"{ref}.dpapi").unlink(missing_ok=True)

    def save(self, provider_id: str, secret: str):
        prepared = self.prepare(secret)
        provider = self.store.get("provider", provider_id)
        if not provider:
            raise StoreError("provider_not_found", "模型配置不存在", 404)
        old = provider.get("credential_ref")
        updated = self.store.patch("provider", provider_id, **prepared,
            credential_version=provider.get("credential_version", 0)+1, test_status="untested")
        self.discard(old)
        return self.public(updated)

    def get_secret(self, provider_id="deepseek-default", version: int | None = None) -> str | None:
        provider = self.store.get("provider", provider_id)
        if not provider:
            return None
        if provider.get("credential_disabled"):
            return None
        if version is not None and version != provider.get("credential_version", 0):
            raise StoreError("credential_version_changed", "凭据版本已变化，请显式重新绑定研究凭据", 409)
        ref = provider.get("credential_ref")
        if ref:
            if not re.fullmatch(r"credential-[a-f0-9]{32}", ref):
                raise StoreError("invalid_credential_reference", "凭据引用无效", 503)
            path = self.root / f"{ref}.dpapi"
            if path.is_file():
                return _dpapi(path.read_bytes(), False).decode("utf-8")
        return os.environ.get("DEEPSEEK_API_KEY") if provider_id == "deepseek-default" else None

    def delete(self, provider_id):
        p = self.store.get("provider", provider_id)
        if not p:
            raise StoreError("provider_not_found", "模型配置不存在", 404)
        ref = p.get("credential_ref")
        if ref and re.fullmatch(r"credential-[a-f0-9]{32}", ref):
            (self.root / f"{ref}.dpapi").unlink(missing_ok=True)
        result = self.store.patch("provider", provider_id, credential_ref=None, configured=False,
            credential_mask=None, credential_disabled=True, credential_version=p.get("credential_version", 0)+1, test_status="credential_deleted")
        for project in self.store.projects():
            if project["provider_config_id"] == provider_id and project["status"] not in {"terminated", "completed", "terminating"}:
                self.store.update_project(project["id"], status="blocked", blocked_reason="credential_missing")
                self.store.emit(project["id"], "research.blocked", {"reason": "credential_missing"})
        return self.public(result)

    def public(self, provider):
        value = {k: v for k, v in provider.items() if k not in {"credential_ref", "api_key", "secret"}}
        if not value.get("configured") and not value.get("credential_disabled") and value.get("id") == "deepseek-default" and os.environ.get("DEEPSEEK_API_KEY"):
            value.update(configured=True, credential_mask="环境变量注入", credential_source="environment")
        return value
