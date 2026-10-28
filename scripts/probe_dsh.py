"""M0 inspection. Live calls require --live and a user-configured credential.

This reports each tested capability separately. A successful model response does
not claim SDK recovery, permission, cancellation, or arbitrary load tests passed.
"""
from __future__ import annotations

import argparse
import importlib.metadata
import inspect
import json
import os
import platform
import subprocess
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--live", action="store_true", help="Perform one actual minimal model request (billable)")
    parser.add_argument("--startup", action="store_true", help="Boot full SDK + role plugin with a dummy local credential; no model request")
    parser.add_argument("--dsh-bin", help="Explicit matching official DSH executable, supported by the public SDK")
    parser.add_argument("--local-fixture", action="store_true", help="Run official SDK against a local dummy model to verify tools, streaming, resume and stop; no live key")
    args = parser.parse_args()
    if args.dsh_bin:
        os.environ["AUTORESEARCH_DSH_BIN"] = args.dsh_bin
    report = {"schema_version": 1, "platform": platform.platform(), "profile": "sdk", "official_sources": [
        "https://deepseek-harness.github.io/deepseek-harness/en/guide/python-sdk",
        "https://github.com/deepseek-ai/deepseek-harness/blob/master/python/sdk/README.md"], "checks": {}}
    try:
        from backend.adapters.dsh import runtime_availability
        available = runtime_availability()
        report.update({k: v for k, v in available.items() if k not in {"message"}})
        report["checks"]["matching_versions"] = available["available"]
        if not available["available"]:
            raise RuntimeError("SDK or matching runtime unavailable")
        dsh_bin = available.get("dsh_bin")
        from deepseek_harness import DeepSeekHarness, DeepSeekHarnessConfig
        required = {"profile", "patches", "dsh_home", "request_timeout_seconds", "shutdown_timeout_seconds", "api_key"}
        fields = set(DeepSeekHarnessConfig.__dataclass_fields__)
        report["checks"]["public_contract"] = required <= fields and hasattr(DeepSeekHarness, "resume_session")
        from backend.adapters.sdk_child import DISABLED_ROWS, policy_text
        home = Path(args.data_dir).resolve() / "dsh" / "m0-home"
        home.mkdir(parents=True, exist_ok=True)
        patch = home / "autoresearch-policy.patch.yml"
        patch.write_text(policy_text(), encoding="utf-8")
        report["policy_disabled_rows"] = DISABLED_ROWS
        report["checks"]["permission_runtime_enforcement"] = "not_tested_without_live_runtime"
        if args.startup:
            entry = (Path(__file__).resolve().parents[1] / "packages" / "dsh-plugin-research" / "index.js").as_posix().replace("'", "''")
            patch.write_text(policy_text() + f"- insert:\n    - id: autoresearch-tools\n      name: '{entry}'\n", encoding="utf-8")
            workspace = home / "workspace"
            workspace.mkdir(exist_ok=True)
            with DeepSeekHarness(profile="sdk", dsh_home=str(home), cwd=str(workspace), patches=(str(patch),),
                    dsh_bin=dsh_bin,
                    model="deepseek-flash", api_key="local-startup-probe-dummy-key", base_url="http://127.0.0.1:1",
                    request_timeout_seconds=15, env={"AUTORESEARCH_TOOL_GATEWAY": "http://127.0.0.1:1/tool",
                    "AUTORESEARCH_TOOL_CAPABILITY": "local-startup-probe", "AUTORESEARCH_ROLE": "exploration"}) as harness:
                report["checks"]["full_sdk_and_plugin_startup"] = True
                report["checks"]["live_model"] = "not_requested"
            # Live probe below uses the base role policy without a dummy gateway.
            patch.write_text(policy_text(), encoding="utf-8")
        if args.live:
            from backend.services.credentials import CredentialService
            secret = CredentialService(args.data_dir).get_secret("deepseek-default")
            if not secret:
                report["checks"]["live_model"] = "blocked_missing_credential"
            else:
                workspace = home / "workspace"
                workspace.mkdir(exist_ok=True)
                notifications = []
                with DeepSeekHarness(profile="sdk", dsh_home=str(home), cwd=str(workspace),
                        dsh_bin=dsh_bin,
                        patches=(str(patch),), model="deepseek-flash", api_key=secret,
                        request_timeout_seconds=30, shutdown_timeout_seconds=1) as harness:
                    result = harness.run('Return exactly {"ok":true}', session_id="m0-contract",
                        on_notification=lambda n: notifications.append({"method": n.method, "payload": n.payload}))
                    report["checks"]["live_model"] = bool(json.loads(result.final_response).get("ok"))
                    report["checks"]["notifications"] = bool(notifications)
                    report["checks"]["root_events"] = bool(result.events)
                with DeepSeekHarness(profile="sdk", dsh_home=str(home), cwd=str(workspace),
                        dsh_bin=dsh_bin,
                        patches=(str(patch),), model="deepseek-flash", api_key=secret,
                        request_timeout_seconds=30) as harness:
                    harness.resume_session("m0-contract")
                    report["checks"]["resume_history"] = True
        else:
            report["checks"]["live_model"] = "not_requested"
        report["checks"]["hard_cancel_process_tree"] = "covered_by_runtime_tests_not_this_probe"
        report["checks"]["worker_crash_recovery"] = "requires_fault_injection"
        if args.local_fixture:
            repository = Path(__file__).resolve().parents[1]
            test_dir = Path(args.data_dir).resolve() / "dsh" / ("m0-fixtures-" + uuid.uuid4().hex[:8])
            run = subprocess.run([sys.executable, "-m", "pytest", "-q", "tests/integration/test_sdk_fixture.py",
                "tests/integration/test_dsh_lifecycle.py", "--basetemp", str(test_dir), "--disable-warnings"],
                cwd=repository, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=180)
            report["checks"]["local_transport_fixtures"] = "passed" if run.returncode == 0 else "failed"
            report["fixture_summary"] = (run.stdout + run.stderr)[-2000:]
            if run.returncode == 0:
                report["checks"].update(permission_runtime_enforcement="verified_against_local_fixture",
                    streaming_persistence="passed", session_resume="passed", execution_deduplication="passed",
                    hard_cancel_process_tree="passed", worker_owner_crash_containment="passed")
    except importlib.metadata.PackageNotFoundError:
        report["checks"]["sdk"] = "blocked_missing_sdk_or_native_runtime"
    except Exception as error:
        report["error_code"] = type(error).__name__
        report["checks"]["live_model"] = "failed_see_local_runtime_diagnostics" if args.live else "not_requested"
        if args.startup:
            report["checks"]["full_sdk_and_plugin_startup"] = False
    report["production_acceptance_complete"] = False
    output = Path(args.data_dir).resolve() / "dsh" / "m0-report.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    success = report.get("checks", {}).get("matching_versions") and not report.get("error_code")
    if args.local_fixture:
        success = success and report["checks"].get("local_transport_fixtures") == "passed"
    return 0 if success else 2


if __name__ == "__main__":
    raise SystemExit(main())
