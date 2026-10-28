"""Disposable, supervised owner of an official full-profile SDK runtime.

Only a stdin pipe contains the credential. stdout contains public SDK events.
No SDK private process internals or invented cancellation APIs are used.
"""
from __future__ import annotations

import json
import os
import sys
import threading
import time
from pathlib import Path

import psutil

from backend.adapters.model_errors import classify_model_failure


# Disable unsafe entry points in the full sdk tree, including paths which can
# create undeclared agents, discover user skills, or upload session logs.
DISABLED_ROWS = ["tool-bash", "tool-pwsh", "tool-jobs", "tool-fs", "tool-fs-search",
    "tool-working-directory", "tool-subagent", "tool-subagent-fork", "tool-subagent-control",
    "tool-subagent-list-agents", "tool-workflow", "workflow-ptc", "ptc-runtime",
    "tool-plugin-manager", "tool-skill", "skill-filesystem", "agent-instructions",
    "tool-goal", "goal-round-driver", "workspace-dependencies", "skill-office",
    "session-log-deepseek", "session-telemetry-otel"]


def policy_text():
    return "\n".join(f"- id: {row}\n  disabled: true" for row in DISABLED_ROWS) + "\n"


def emit(value):
    # The SDK may replace stdout's encoding. ASCII wire JSON roundtrips Unicode
    # independently of that process-wide setting, while the parent decodes JSON.
    print(json.dumps(value, ensure_ascii=True, default=str), flush=True)


def contain_windows_runtime():
    """Self-owned, non-inheritable Job closes if this owner is killed.

    The SDK starts only after assignment, so every future native runtime/tool
    descendant joins the Job and cannot survive the owner's abrupt death.
    """
    if os.name != "nt":
        return None
    import ctypes
    from ctypes import wintypes
    from backend.runtime.processes import EXTENDED_LIMIT, kernel
    kernel.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    kernel.AssignProcessToJobObject.restype = wintypes.BOOL
    job = kernel.CreateJobObjectW(None, None)
    limits = EXTENDED_LIMIT()
    limits.BasicLimitInformation.LimitFlags = 0x2000
    if not job or not kernel.SetInformationJobObject(job, 9, ctypes.byref(limits), ctypes.sizeof(limits)):
        raise RuntimeError("Cannot create runtime containment Job")
    if not kernel.AssignProcessToJobObject(job, ctypes.c_void_p(-1)):
        kernel.CloseHandle(job)
        raise RuntimeError("Cannot contain runtime owner in Windows Job")
    return job  # OS closes it on owner death. Never inherited by descendants.


def main():
    payload = json.loads(sys.stdin.readline())
    secret = payload.pop("api_key")
    patch = Path(payload["home"]) / "autoresearch-policy.patch.yml"
    content = policy_text()
    if payload.get("plugin_entry"):
        entry = Path(payload["plugin_entry"]).resolve().as_posix().replace("'", "''")
        content += f"- insert:\n    - id: autoresearch-tools\n      name: '{entry}'\n"
    patch.write_text(content, encoding="utf-8")
    stop = threading.Event()
    def watchdog():
        until = time.monotonic() + payload["timeout"]
        while not stop.wait(0.3):
            try:
                parent_alive = abs(psutil.Process(payload["parent_pid"]).create_time() - payload["parent_create_time"]) < 0.1
            except psutil.NoSuchProcess:
                parent_alive = False
            if not parent_alive or time.monotonic() >= until:
                for child in psutil.Process().children(recursive=True):
                    try:
                        child.kill()
                    except psutil.NoSuchProcess:
                        pass
                os._exit(75)
    threading.Thread(target=watchdog, daemon=True).start()
    try:
        job_handle = contain_windows_runtime()
        from deepseek_harness import DeepSeekHarness
        with DeepSeekHarness(profile="sdk", dsh_home=payload["home"], cwd=payload["workspace"],
                dsh_bin=payload.get("dsh_bin"),
                patches=(str(patch.resolve()),), provider="deepseek-official", model=payload["model"],
                base_url=payload["base_url"], api_key=secret, request_timeout_seconds=payload["timeout"],
                **({"reasoning_effort": "off", "max_tokens": 64} if payload.get("auxiliary") else {}),
                env={"AUTORESEARCH_TOOL_GATEWAY": payload.get("gateway_url", ""),
                     "AUTORESEARCH_TOOL_CAPABILITY": payload.get("gateway_token", ""), "AUTORESEARCH_ROLE": payload.get("role", ""),
                     "AUTORESEARCH_HAS_INPUTS": "1" if payload.get("has_inputs") else "0"},
                shutdown_timeout_seconds=1.0) as harness:
            session = harness.resume_session(payload["session_id"]) if payload["resume"] else harness.start_session(payload["session_id"])
            def notification(item):
                emit({"type": "notification", "method": item.method, "payload": item.payload})
            result = session.run(payload["prompt"], on_notification=notification)
            if result.finish_reason not in {"completed", "max-tokens"}:
                emit({"type": "error", **classify_model_failure(result.events)})
                return 1
            emit({"type": "result", "final_response": result.final_response,
                  "finish_reason": result.finish_reason, "actual_model": "unknown"})
    except Exception as error:
        # SDK exceptions may contain provider headers; never echo raw exception.
        if isinstance(error, TimeoutError):
            failure = classify_model_failure([{"type": "finish", "reason": {"kind": "error", "failure": {"code": "TIMEOUT"}}}])
        else:
            failure = {"code": "dsh_runtime_error", "message": "DSH 启动或执行失败，请检查本地运行时和模型配置。",
                "http_status": None, "provider_error_code": None}
        emit({"type": "error", **failure})
        return 1
    finally:
        stop.set()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
