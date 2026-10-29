"""Explicit, isolated live God acceptance against the current frozen provider.

Run with --live to permit three short real model requests. No existing research
is resumed; keys stay in CredentialService and never enter the verification data.
"""
import argparse
import asyncio
import json
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from backend.graphs.engine import ResearchEngine
from backend.persistence.store import Store, now
from backend.runtime.control import ControlSupervisor
from backend.runtime.runner import RunnerSupervisor
from backend.services.credentials import CredentialService


def execution_evidence(executions, pair):
    groups = [[e for e in executions if e.get("agent_id") == "god/" + d["arbitration_id"]] for d in pair]
    overlap = any(max(a["started_at"], b["started_at"]) < min(a["completed_at"], b["completed_at"])
        for a in groups[0] for b in groups[1] if a.get("completed_at") and b.get("completed_at"))
    sessions = [{e["session_id"] for e in group} for group in groups]
    return {"concurrent_overlap": overlap, "independent_sessions": bool(sessions[0] and sessions[1] and not sessions[0] & sessions[1]),
        "generation_attempts": len(executions)}


def check_existing(folder):
    root = Path(folder).resolve()
    report = json.loads((root / "report.json").read_text(encoding="utf-8"))
    store = Store(root)
    decisions = report["arbitrations"]
    evidence = execution_evidence(store.list("execution", report["research_id"]), decisions[1:])
    report.update(evidence, json_decisions=len(decisions), status="passed" if evidence["concurrent_overlap"] and evidence["independent_sessions"] else "failed")
    (root / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({**report, "report_path": str(root / "report.json")}, ensure_ascii=False))
    return 0 if report["status"] == "passed" else 1


async def verify(args):
    source = Store(args.source_data)
    provider = source.get("provider", "deepseek-default")
    root = Path(args.output).resolve() / ("god-live-" + uuid.uuid4().hex[:12])
    store = Store(root)
    # Copy configuration references, never DPAPI blobs or secret fields.
    store.put("provider", "deepseek-default", {k: v for k, v in provider.items()
        if k not in {"credential_ref", "api_key", "secret"}})
    project = store.start_project(store.create_project({"client_request_id": "god-live-acceptance",
        "topic": "God 仲裁短验收；不进行科研或实验", "mode": "live",
        "team": {"n": 1, "model_concurrency": 2}, "max_runtime_seconds": 300})["id"])
    control = ControlSupervisor(root / "control")
    control.register_project(project["id"], project["deadline_at"])
    runner = RunnerSupervisor(root, control=control)
    report = {"checked_at": now(), "mode": "live", "model": provider["model"], "research_id": project["id"]}
    try:
        async with AsyncSqliteSaver.from_conn_string(str(root / "checkpoints.sqlite")) as saver:
            engine = ResearchEngine(store, control, runner, CredentialService(args.source_data, source), saver)
            async def ask(scope):
                return await engine.arbitration.resolve(research_id=project["id"], source_task_id=scope,
                    scope_kind="discussion", scope_id=scope, reason_code="decision_blocked", origin_node="discussion.collect",
                    context={"topic": "短 JSON 决策验收", "role": "boss", "members": ["boss/member-01", "boss/member-02"],
                        "instruction": "两候选等价，已有真实1比1平票，按稳定ID选一项。只返回select_candidate完整JSON，不需要调用工具。",
                        "candidates": {"boss/member-01": {"method": "same"}, "boss/member-02": {"method": "same"}},
                        "votes": {"boss/member-01": "boss/member-01", "boss/member-02": "boss/member-02"},
                        "issues": [], "blockers": [], "scope": scope}, allowed_actions=["select_candidate"])
            try:
                single = await ask("single")
                await engine.arbitration.apply(single["arbitration_id"], single)
                pair = await asyncio.gather(ask("parallel-a"), ask("parallel-b"))
                for decision in pair:
                    await engine.arbitration.apply(decision["arbitration_id"], decision)
                executions = [e for e in store.list("execution", project["id"]) if e.get("agent_id", "").startswith("god/")]
                evidence = execution_evidence(executions, pair)
                report.update(status="passed" if evidence["concurrent_overlap"] and evidence["independent_sessions"] else "failed",
                    json_decisions=3, **evidence,
                    arbitrations=[{"arbitration_id": d["arbitration_id"], "action": d["action"], "selected_candidate_id": d["selected_candidate_id"]} for d in [single, *pair]])
            finally:
                await engine.close()
    except Exception as error:
        report.update(status="failed", code=getattr(error, "code", type(error).__name__))
    finally:
        runner.close()
        control.terminate(project["id"])
        control.close()
    (root / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({**report, "report_path": str(root / "report.json")}, ensure_ascii=False))
    return 0 if report.get("status") == "passed" else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--source-data", default="data")
    parser.add_argument("--output", default=".test-data")
    parser.add_argument("--check-existing", help="Recheck saved execution intervals without model requests")
    args = parser.parse_args()
    if args.check_existing:
        raise SystemExit(check_existing(args.check_existing))
    if not args.live:
        parser.error("--live is required for real model requests")
    raise SystemExit(asyncio.run(verify(args)))
