import json
import zipfile
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from apps.api.main import create_app


SECRET = "SYNTHETIC-SECRET-MUST-NOT-BE-EXPORTED"


@pytest.fixture
def export_data(tmp_path):
    app = create_app(tmp_path, control_client=object(), testing=True)
    with TestClient(app) as client:
        client.headers["X-CSRF-Token"] = client.get("/api/session").json()["csrf_token"]
        response = client.post("/api/research", json={
            "client_request_id": "export-scopes", "title": "Scoped export fixture",
            "topic": "PROJECT_PRIVATE_TOPIC", "description": "PROJECT_PRIVATE_DESCRIPTION", "mode": "demo",
        })
        assert response.status_code == 201, response.text
        research_id, store = response.json()["id"], app.state.store
        foreign_id = store.create_project({"client_request_id": "foreign", "topic": "Foreign project"})["id"]

        def attachment(name, marker, owner=research_id, **fields):
            source = tmp_path / name
            source.write_text(marker, encoding="utf-8")
            return store.register_artifact(owner, source, "text/plain", **fields)

        artifacts = {
            "report": attachment("report.md", "REPORT_ATTACHMENT"),
            "report_evidence": attachment("reported-chart.csv", "REPORT_EVIDENCE_ATTACHMENT"),
            "submission": attachment("experiment.log", "SUBMISSION_ATTACHMENT"),
            "spec": attachment("plan-input.txt", "SPEC_ATTACHMENT"),
            "verify": attachment("verification.txt", "VERIFY_ATTACHMENT"),
            "chat": attachment("chat-note.txt", "CHAT_ATTACHMENT"),
            "tool": attachment("tool-result.csv", "TOOL_ATTACHMENT"),
            "shared": attachment("shared-note.txt", "DIRECTLY_LINKED_ATTACHMENT"),
            "unrelated": attachment("private-unrelated.txt", "UNRELATED_PRIVATE_ATTACHMENT"),
            "foreign": attachment("foreign-private.txt", "FOREIGN_PRIVATE_ATTACHMENT", foreign_id),
            "prior_export": attachment("prior-full-export.zip", "PRIOR_FULL_EXPORT_PRIVATE_DATA", purpose="export"),
        }
        store.put("spec", "published-spec", {"question": "SUBMISSION_PLAN_RECORD",
            "data": {"artifact_id": artifacts["spec"]["id"]}}, research_id)
        store.put("spec", "draft-spec", {"question": "UNSUBMITTED_DRAFT_PLAN"}, research_id)
        store.put("run", "published-run", {"state": "completed", "metrics": {"label": "SUBMISSION_RUN_RECORD"}}, research_id)
        store.put("run", "unsubmitted-run", {"state": "completed", "metrics": {"label": "UNSUBMITTED_RUN_RECORD"}}, research_id)
        submission = store.put("submission", "published-submission", {"cycle_id": "cycle",
            "spec_id": "published-spec", "run_ids": ["published-run"], "notes": "SUBMISSION_RECORD",
            "artifacts": [artifacts["submission"]]}, research_id)
        store.put("verification", "published-verification", {"submission_version_id": submission["id"],
            "passed": False, "content_markdown": "VERIFICATION_RECORD",
            "evidence": [{"source_id": artifacts["verify"]["id"]}], "secret": SECRET}, research_id)
        store.put("verification", "unrelated-verification", {"submission_version_id": "not-submitted",
            "passed": False, "content_markdown": "UNRELATED_VERIFICATION_RECORD"}, research_id)
        store.put("report_snapshot", "frozen-report-context", {"snapshot": {
            "submission": submission, "private_notes": "FROZEN_CONTEXT_PRIVATE_CHAT",
            "artifact_id": artifacts["unrelated"]["id"]}}, research_id)
        store.put("report", "published-report", {"title": "Stage report", "status": "completed",
            "content_markdown": "REPORT_RECORD [linked note](/api/artifacts/" + artifacts["shared"]["id"] + "?download=true)",
            "artifact_id": artifacts["report"]["id"], "snapshot_id": "frozen-report-context",
            "evidence_ids": [artifacts["report_evidence"]["id"], artifacts["foreign"]["id"],
                artifacts["prior_export"]["id"], submission["id"]],
            "receipt": {"api_key": SECRET, "authorization": SECRET},
            # A redacted field must not pull its attachment into the ZIP.
            "secret": artifacts["unrelated"]["id"]}, research_id)
        store.put("memory_node", "memory", {"content": "MEMORY_ONLY_RECORD", "credential_ref": SECRET}, research_id)
        store.put("memory_version", "memory:1", {"content": "MEMORY_HISTORY_ONLY_RECORD"}, research_id)
        store.put("boss_decision", "decision", {"content_markdown": "BOSS_ONLY_RECORD"}, research_id)
        store.create_conversation(research_id, "chat", title="CHAT_METADATA")
        store.create_message(research_id, "chat", "boss/member-01", "CHAT_RECORD api_key=" + SECRET,
            artifact_refs=[artifacts["chat"]["id"], {"artifact_id": artifacts["shared"]["id"]}],
            capability=SECRET)
        tool_event = {"type": "notification", "method": "session.event", "payload": {"event": {
            "type": "tool/result", "data": {"message": {"toolCallId": "call", "content": [
                {"type": "text", "text": json.dumps({"artifact_id": artifacts["tool"]["id"], "label": "CHAT_TOOL_EVENT"})}]}}}}}
        store.emit(research_id, "tool.completed", {"notification": tool_event}, conversation_id="chat")
        store.emit(research_id, "report.completed", {"report_id": "published-report",
            "artifact_id": artifacts["report"]["id"], "label": "REPORT_EVENT", "api_key": SECRET})
        store.emit(research_id, "verification.completed", {"submission_version_id": submission["id"], "label": "VERIFY_EVENT"})
        store.emit(research_id, "experiment.metric", {"run_id": "published-run", "metrics": {"label": "SUBMISSION_RUN_EVENT"}})
        store.emit(research_id, "experiment.metric", {"run_id": "unsubmitted-run", "metrics": {"label": "UNSUBMITTED_RUN_EVENT"}})
        store.emit(research_id, "research.blocked", {"label": "OTHER_EVENT_ONLY_RECORD"})
        credentials = tmp_path / "credentials"
        credentials.mkdir(exist_ok=True)
        (credentials / "synthetic.dpapi").write_text(SECRET, encoding="utf-8")
        yield SimpleNamespace(client=client, store=store, research_id=research_id, artifacts=artifacts)
    app.state.control_executor.shutdown(wait=True)


def export_files(data, kind, request_id=None):
    response = data.client.post(f"/api/research/{data.research_id}/exports", json={
        "kind": kind, "client_request_id": request_id or "export-" + kind})
    assert response.status_code == 200, response.text
    assert response.json()["kind"] == kind
    _, path = data.store.artifact(response.json()["artifact_id"])
    with zipfile.ZipFile(path) as archive:
        files = {name: archive.read(name) for name in archive.namelist()}
    return files, response.json()


def attached_ids(files):
    return {name.split("/")[1] for name in files if name.startswith("artifacts/")}


def assert_no_cross_scope_data(files, markers):
    content = b"\n".join(files.values())
    for marker in [SECRET, "FOREIGN_PRIVATE_ATTACHMENT", "PRIOR_FULL_EXPORT_PRIVATE_DATA", *markers]:
        assert marker.encode() not in content, marker


def test_report_export_contains_only_reports_events_and_direct_report_attachments(export_data):
    data = export_data
    files, _ = export_files(data, "report")
    assert {name for name in files if not name.startswith("artifacts/")} == {"snapshot.json", "report.json", "events.json"}
    snapshot = json.loads(files["snapshot.json"])
    assert set(snapshot) == {"project", "snapshot_seq", "export_kind", "reports"}
    assert snapshot["reports"][0]["id"] == "published-report"
    assert [event["type"] for event in json.loads(files["events.json"])] == ["report.completed"]
    assert attached_ids(files) == {data.artifacts[name]["id"] for name in ("report", "report_evidence", "shared")}
    assert_no_cross_scope_data(files, ["CHAT_RECORD", "CHAT_TOOL_EVENT", "CHAT_METADATA", "CHAT_ATTACHMENT",
        "SUBMISSION_RECORD", "SUBMISSION_ATTACHMENT", "VERIFICATION_RECORD", "MEMORY_ONLY_RECORD",
        "BOSS_ONLY_RECORD", "OTHER_EVENT_ONLY_RECORD", "FROZEN_CONTEXT_PRIVATE_CHAT",
        "UNRELATED_PRIVATE_ATTACHMENT", "PROJECT_PRIVATE_TOPIC", "PROJECT_PRIVATE_DESCRIPTION"])


def test_submission_export_includes_only_submitted_plans_verifications_runs_and_their_files(export_data):
    data = export_data
    files, _ = export_files(data, "submissions")
    assert {name for name in files if not name.startswith("artifacts/")} == {
        "snapshot.json", "submission.json", "spec.json", "verification.json", "events.json"}
    assert [record["id"] for record in json.loads(files["spec.json"])] == ["published-spec"]
    assert [record["id"] for record in json.loads(files["verification.json"])] == ["published-verification"]
    snapshot = json.loads(files["snapshot.json"])
    assert [run["run_id"] for run in snapshot["runs"]] == ["published-run"]
    assert {event["type"] for event in json.loads(files["events.json"])} == {
        "submission.created", "verification.completed", "experiment.metric"}
    assert attached_ids(files) == {data.artifacts[name]["id"] for name in ("submission", "spec", "verify")}
    assert_no_cross_scope_data(files, ["CHAT_RECORD", "CHAT_TOOL_EVENT", "REPORT_RECORD", "REPORT_ATTACHMENT",
        "MEMORY_ONLY_RECORD", "BOSS_ONLY_RECORD", "OTHER_EVENT_ONLY_RECORD", "UNRELATED_PRIVATE_ATTACHMENT",
        "UNSUBMITTED_DRAFT_PLAN", "UNSUBMITTED_RUN_RECORD", "UNSUBMITTED_RUN_EVENT",
        "UNRELATED_VERIFICATION_RECORD", "PROJECT_PRIVATE_TOPIC", "PROJECT_PRIVATE_DESCRIPTION"])


def test_conversation_export_contains_transcripts_tool_events_and_referenced_files(export_data):
    data = export_data
    files, _ = export_files(data, "conversations")
    assert {name for name in files if not name.startswith("artifacts/")} == {"snapshot.json", "conversations.json", "events.json"}
    messages = json.loads(files["conversations.json"])
    assert len(messages) == 1 and "CHAT_RECORD" in messages[0]["content_markdown"]
    assert {event["type"] for event in json.loads(files["events.json"])} == {
        "conversation.created", "message.completed", "tool.completed"}
    assert attached_ids(files) == {data.artifacts[name]["id"] for name in ("chat", "tool", "shared")}
    assert_no_cross_scope_data(files, ["REPORT_RECORD", "REPORT_ATTACHMENT", "SUBMISSION_RECORD",
        "SUBMISSION_ATTACHMENT", "VERIFICATION_RECORD", "MEMORY_ONLY_RECORD", "BOSS_ONLY_RECORD",
        "OTHER_EVENT_ONLY_RECORD", "UNRELATED_PRIVATE_ATTACHMENT", "PROJECT_PRIVATE_TOPIC", "PROJECT_PRIVATE_DESCRIPTION"])


def test_all_export_preserves_existing_file_layout_and_full_project_evidence(export_data):
    data = export_data
    files, _ = export_files(data, "all")
    assert {name for name in files if not name.startswith("artifacts/")} == {
        "snapshot.json", "submission.json", "spec.json", "verification.json", "boss_decision.json",
        "report.json", "memory_node.json", "memory_version.json", "conversations.json", "events.json"}
    content = b"\n".join(files.values())
    for marker in ["CHAT_RECORD", "REPORT_RECORD", "SUBMISSION_RECORD", "MEMORY_ONLY_RECORD",
            "BOSS_ONLY_RECORD", "OTHER_EVENT_ONLY_RECORD", "UNRELATED_PRIVATE_ATTACHMENT",
            "UNSUBMITTED_DRAFT_PLAN", "PROJECT_PRIVATE_DESCRIPTION"]:
        assert marker.encode() in content, marker
    assert attached_ids(files) == {artifact["id"] for name, artifact in data.artifacts.items()
        if name not in {"foreign", "prior_export"}}
    assert_no_cross_scope_data(files, [])


@pytest.mark.parametrize("legacy_cache", [False, True])
def test_report_request_cannot_reuse_an_all_export_request_id(export_data, legacy_cache):
    data = export_data
    _, first = export_files(data, "all", request_id="same-request")
    if legacy_cache:
        record = data.store.get("export", "same-request")
        data.store.put("export", "same-request", {key: value for key, value in record.items() if key != "kind"}, data.research_id)
    original_artifacts = len(data.store.list("artifact", data.research_id))
    response = data.client.post(f"/api/research/{data.research_id}/exports", json={
        "kind": "report", "client_request_id": "same-request"})
    assert response.status_code == 409, response.text
    assert response.json()["code"] == "idempotency_conflict"
    repeated = data.client.post(f"/api/research/{data.research_id}/exports", json={
        "kind": "all", "client_request_id": "same-request"})
    assert repeated.status_code == 200
    assert repeated.json()["artifact_id"] == first["artifact_id"]
    assert len(data.store.list("artifact", data.research_id)) == original_artifacts
