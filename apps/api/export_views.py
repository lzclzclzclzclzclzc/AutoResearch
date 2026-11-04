"""Select archive records and their direct attachments for a requested export."""
from __future__ import annotations

import json
import re
from urllib.parse import unquote

from backend.persistence.event_views import tool_notification


ALL_DOCUMENTS = ("submission", "spec", "verification", "boss_decision", "report", "memory_node", "memory_version")
PROJECT_METADATA = ("id", "research_id", "title", "mode", "created_at", "started_at", "completed_at")


def export_records(store, research_id, kind, snapshot):
    """Partial exports carry selected records, never the full workspace snapshot.

    Report snapshot IDs and other business-document citations remain references.
    Following them would import unselected discussions, inputs, or submissions.
    """
    with store.connect() as connection:
        events = [json.loads(row[0]) for row in connection.execute(
            "SELECT data FROM events WHERE research_id=? ORDER BY seq", (research_id,))]
        messages = ([json.loads(row[0]) for row in connection.execute(
            "SELECT data FROM messages WHERE research_id=? ORDER BY cursor", (research_id,))]
            if kind in {"all", "conversations"} else [])

    if kind == "all":
        return {"snapshot.json": snapshot,
            **{name + ".json": store.list(name, research_id) for name in ALL_DOCUMENTS},
            "conversations.json": messages, "events.json": events}

    selected_snapshot = {
        "project": {key: snapshot["project"][key] for key in PROJECT_METADATA if key in snapshot["project"]},
        "snapshot_seq": snapshot["snapshot_seq"], "export_kind": kind,
    }
    records = {"snapshot.json": selected_snapshot}
    if kind == "report":
        reports = store.list("report", research_id)
        report_ids = {report["id"] for report in reports}
        records["report.json"] = reports
        selected_snapshot["reports"] = [report for report in snapshot["reports"] if report["id"] in report_ids]
        events = [event for event in events if event["type"] == "report.completed"
            and event.get("payload", {}).get("report_id") in report_ids]
    elif kind == "submissions":
        submissions = store.list("submission", research_id)
        submission_ids = {submission["id"] for submission in submissions}
        spec_ids = {submission.get("spec_id") for submission in submissions}
        run_ids = {run_id for submission in submissions for run_id in submission.get("run_ids", [])}
        specs = {spec["id"]: spec for name in ("experiment_spec", "spec")
            for spec in store.list(name, research_id) if spec["id"] in spec_ids}
        verifications = [report for report in store.list("verification", research_id)
            if report.get("submission_version_id") in submission_ids]
        records.update({"submission.json": submissions, "spec.json": list(specs.values()),
            "verification.json": verifications})
        selected_snapshot.update({
            "submissions": [submission for submission in snapshot["submissions"] if submission["id"] in submission_ids],
            "verification_reports": verifications,
            "runs": [run for run in snapshot["runs"] if run["run_id"] in run_ids],
        })
        events = [event for event in events if _submission_event(event, submission_ids, run_ids)]
    elif kind == "conversations":
        conversations = store.list("conversation", research_id)
        conversation_ids = {conversation["conversation_id"] for conversation in conversations}
        message_ids = {message["message_id"] for message in messages}
        records["conversations.json"] = messages
        selected_snapshot["conversations"] = conversations
        events = [event for event in events if _conversation_event(event, conversation_ids, message_ids)]
    else:
        raise ValueError("Unknown export kind")
    records["events.json"] = events
    return records


def _submission_event(event, submission_ids, run_ids):
    payload, event_type = event.get("payload", {}), event["type"]
    if event_type == "submission.created" or event_type.startswith("verification."):
        return payload.get("submission_id") in submission_ids or payload.get("submission_version_id") in submission_ids
    return event_type.startswith("experiment.") and any(
        payload.get(key) in run_ids for key in ("run_id", "experiment_run_id", "from_run_id"))


def _conversation_event(event, conversation_ids, message_ids):
    event_type, payload = event["type"], event.get("payload", {})
    if event_type.startswith("conversation."):
        return event.get("conversation_id") in conversation_ids
    if event_type.startswith("message."):
        return payload.get("message_id") in message_ids
    # Tool results are part of the displayed conversation transcript. Legacy SDK
    # traces may lack conversation IDs, but remain scoped to this research project.
    return event_type in {"tool.started", "tool.completed", "tool.failed"} or (
        event_type == "agent.runtime_event" and tool_notification(payload.get("notification") or {}) is not None)


def export_artifacts(store, research_id, kind, public_records):
    """Attach only same-project files referenced by already-redacted records.

    Never follow an attachment into another export archive: it could contain
    categories omitted from this request. Attachment bytes remain unchanged.
    """
    available = {artifact["artifact_id"]: artifact for artifact in store.list("artifact", research_id)
        if artifact.get("purpose") != "export"}
    if kind == "all":
        return list(available.values())
    references = set()

    def collect(value):
        if isinstance(value, dict):
            for item in value.values():
                collect(item)
        elif isinstance(value, list):
            for item in value:
                collect(item)
        elif isinstance(value, str):
            if value in available:
                references.add(value)
            references.update(re.findall(r"\bartifact-[a-zA-Z0-9_-]+", value))
            references.update(unquote(identity) for identity in re.findall(
                r"/api/artifacts/([^/?#\s)<>\"']+)", value))

    collect(public_records)
    return [artifact for identity, artifact in available.items() if identity in references]
