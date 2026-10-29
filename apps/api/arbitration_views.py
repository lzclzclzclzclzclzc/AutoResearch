"""Read-only, event-cursor projections for automatic arbitration."""
from __future__ import annotations

import json
import hashlib

REQUEST_FIELDS = (
    "id", "arbitration_id", "schema_version", "policy_version", "research_id", "source_task_id",
    "scope_kind", "scope_id", "origin_node", "origin_checkpoint", "reason_code", "failure_fingerprint",
    "scope_revision", "direction_epoch", "control_generation", "snapshot_id", "snapshot_hash",
    "source_event_seq", "target_schema_name", "target_schema_hash", "allowed_actions", "resume_point",
    "provider_config_id", "credential_version", "model", "recovery_chain_id", "created_at",
    "judge_deadline_at", "expires_at", "requested_event_seq", "line_id",
)

TRANSITION_STATUSES = {
    "arbitration.requested": "requested", "arbitration.started": "running",
    "arbitration.retry_wait": "retry_wait", "arbitration.decided": "decided",
    "arbitration.applying": "applying", "arbitration.applied": "applied",
    "arbitration.stale": "stale", "arbitration.cancelled": "cancelled",
    "arbitration.needs_external_input": "needs_external_input", "arbitration.exhausted": "exhausted",
}
CAPACITY_PHASES = {"arbitration.capacity_wait": "waiting_capacity", "arbitration.capacity_acquired": "judging"}


def arbitration_events(store, research_id, at_seq=None):
    with store.connect() as c:
        return [json.loads(row[0]) for row in c.execute(
            "SELECT data FROM events WHERE research_id=? AND seq<=? "
            "AND (json_extract(data,'$.type') LIKE 'arbitration.%' OR json_extract(data,'$.type') LIKE 'dependency.%' OR json_extract(data,'$.type') IN "
            "('task.waiting_recovery','task.recovery_ready','task.decomposed','task.reassembled')) ORDER BY seq",
            (research_id, at_seq if at_seq is not None else 9223372036854775807))]


def project_request(request, events, at_seq=None):
    identity = request.get("arbitration_id", request["id"])
    refs = [e for e in events if e["type"].startswith("arbitration.") and e.get("payload", {}).get("arbitration_id") == identity]
    transitions = [e for e in refs if e["type"] in TRANSITION_STATUSES]
    history = [h for h in request.get("history", []) if h.get("event_seq", 0) and
               (at_seq is None or h["event_seq"] <= at_seq)]
    if at_seq is not None and not transitions and not history:
        return None
    result = {key: request[key] for key in REQUEST_FIELDS if key in request}
    result["arbitration_id"] = identity
    result["conversation_id"] = f"{request['research_id']}:god:{identity}"
    result["history"] = history
    last = transitions[-1] if transitions else None
    result["status"] = (TRANSITION_STATUSES[last["type"]] if last else
                        history[-1]["status"] if history else request.get("status", "requested"))
    if last and last.get("payload", {}).get("reason"):
        result["diagnostic"] = last["payload"]["reason"]
    if at_seq is None:
        for field in ("status", "retry_not_before", "generation_attempts", "error", "diagnostic", "resolution_status"):
            if field in request:
                result[field] = request[field]
    if result["status"] == "running":
        phase = next((event for event in reversed(refs) if event["type"] in CAPACITY_PHASES
                      and event["seq"] > (last["seq"] if last else 0)), None)
        result["execution_phase"] = CAPACITY_PHASES[phase["type"]] if phase else "judging"
    result["latest_event_seq"] = refs[-1]["seq"] if refs else history[-1]["event_seq"] if history else 0
    result["attempts"] = [{"type": e["type"], "event_seq": e["seq"], "occurred_at": e["occurred_at"],
                           **{k: v for k, v in e.get("payload", {}).items() if k in {
                               "attempt", "generation_attempts", "lease_epoch", "agent_id", "execution_id", "reason",
                               "error", "retry_not_before"}}}
                          for e in refs if e["type"] in {"arbitration.started", "arbitration.retry_wait"}]
    for status in ("decided", "applied"):
        transition = next((e for e in reversed(refs) if e["type"] == f"arbitration.{status}"), None)
        if transition:
            result[f"{status}_event_seq"] = transition["seq"]
            for field in ("decision_id", "decision_hash", "effect_id", "applied_action"):
                if field in transition.get("payload", {}):
                    result[field] = transition["payload"][field]
        elif at_seq is None and request.get(f"{status}_event_seq"):
            result[f"{status}_event_seq"] = request[f"{status}_event_seq"]
            for field in ("decision_id", "decision_hash", "effect_id"):
                if field in request:
                    result[field] = request[field]
    recovery = [event for event in events if event["type"].startswith(("task.", "dependency.")) and
                (event.get("payload", {}).get("arbitration_id") == identity or
                 event.get("payload", {}).get("task_id") == request.get("source_task_id"))]
    if recovery:
        result["resolution_status"] = "cancelled" if recovery[-1]["type"] == "dependency.cancelled" else "waiting_dependencies" if recovery[-1]["type"] in {
            "dependency.waiting", "dependency.coordination_requested"} else "waiting_children" if recovery[-1]["type"] in {
            "task.waiting_recovery", "task.decomposed"} else "fulfilled"
    return result


def request_list(store, research_id, at_seq=None):
    events = arbitration_events(store, research_id, at_seq)
    views = [view for request in store.list("arbitration_request", research_id)
             if (view := project_request(request, events, at_seq)) is not None]
    for view in views:
        if view.get("decided_event_seq") and view.get("decision_id"):
            decision = store.get("arbitration_decision", view["decision_id"])
            if decision and decision.get("research_id") == research_id and decision.get("arbitration_id") == view["arbitration_id"]:
                view["applied_action"] = decision.get("decision", {}).get("action") if view.get("applied_event_seq") else None
    return views


def request_detail(store, research_id, request, at_seq=None):
    view = project_request(request, arbitration_events(store, research_id, at_seq), at_seq)
    if view is None:
        return None
    snapshot = store.get("arbitration_snapshot", request.get("snapshot_id", "")) or {}
    if snapshot.get("research_id") == research_id:
        sections = snapshot.get("sections", {})
        view["snapshot_manifest"] = [{"section": name, "length": len(text),
                                      "sha256": hashlib.sha256(text.encode()).hexdigest()}
            for name, value in sections.items()
            for text in [value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, sort_keys=True)]]
    else:
        view["snapshot_manifest"] = []
    if view.get("decided_event_seq"):
        decisions = store.list("arbitration_decision", research_id, arbitration_id=view["arbitration_id"])
        decision = next((d for d in decisions if d.get("id") == view.get("decision_id")), None)
        if decision is None and view.get("decision_hash"):
            decision = next((d for d in decisions if d.get("decision_hash") == view["decision_hash"]), None)
        if decision and decision.get("decision_hash") == view.get("decision_hash"):
            view["decision"] = decision.get("decision")
            view["validation"] = decision.get("validation", {"valid": True})
            view["source_execution_id"] = decision.get("source_execution_id")
            if view.get("applied_event_seq"):
                view["applied_action"] = decision.get("decision", {}).get("action")
    if view.get("applied_event_seq"):
        view["effects"] = [effect for effect in store.list("arbitration_effect", research_id,
            arbitration_id=view["arbitration_id"]) if effect.get("decision_hash") == view.get("decision_hash")]
    return view
