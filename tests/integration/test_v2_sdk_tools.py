"""The production SDK against a local transport fixture: no external model call."""
import asyncio
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from backend.adapters.dsh import DshRuntimeAdapter, runtime_availability
from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor
from packages.contracts.research import DiscussionFinal, DiscussionTurn, Willingness, digest
from backend.runtime.watchdog import WatchdogDecision


class LocalCredential:
    def get_secret(self, *args, **kwargs):
        return "local-only-discussion-fixture-secret"


@pytest.mark.skipif(not runtime_availability().get("available"), reason="Optional locked official SDK/runtime unavailable")
@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["member", "god", "private-score", "watchdog"])
async def test_suppressed_control_json_retains_authorized_tools_but_private_scores_cannot_use_them(tmp_path, mode):
    requests = []
    material = "BOUND-EVIDENCE-FOR-" + mode
    store = Store(tmp_path / "managed")
    project = store.start_project(store.create_project({"client_request_id": mode, "topic": "LOCAL SDK TOOL FIXTURE",
        "mode": "live", "team": {"n": 1}, "max_runtime_seconds": 90})["id"])
    rid = project["id"]
    path = store.data_dir / "evidence.txt"
    path.write_text(material, encoding="utf-8")
    artifact = store.register_artifact(rid, path, "text/plain", line_id="line-01")
    if mode == "god":
        tool = "read_arbitration_context"
        arguments = {"section": "discussion", "offset": 0}
        output = {"next_step": "critic", "final_output": {"content_markdown": "LOCAL GOD FINAL"}}
        schema, role, agent = DiscussionFinal, "god", "god/local-finalization"
    elif mode == "watchdog":
        tool = "pwsh"
        source = str(path).replace("'", "''")
        repaired = store.data_dir / "native-repair.txt"
        destination = str(repaired).replace("'", "''")
        arguments = {"command": f"Get-Content -LiteralPath '{source}'; Set-Content -LiteralPath '{destination}' -Value 'native-repaired'",
            "description": "Read the local fixture and write its repair receipt"}
        output = {"content_markdown": "Read local evidence and repaired with native tool", "actions": []}
        schema, role, agent = WatchdogDecision, "watchdog", "watchdog"
    else:
        tool, arguments = "read_research_evidence", {"artifact_id": artifact["id"]}
        output = {"score": 50} if mode == "private-score" else {"content_markdown": "LOCAL DISCUSSION TURN"}
        schema, role, agent = (Willingness if mode == "private-score" else DiscussionTurn), "exploration", "line-01/exploration/member-01"

    class Fixture(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            requests.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            index = len(requests)
            if index == 1:
                block = {"type": "tool_use", "id": "local-tool-call", "name": tool, "input": {}}
                delta = {"type": "input_json_delta", "partial_json": json.dumps(arguments)}
                finish = "tool_use"
            else:
                block = {"type": "text", "text": ""}
                delta = {"type": "text_delta", "text": json.dumps(output)}
                finish = "end_turn"
            chunks = [
                {"type": "message_start", "message": {"id": "local-" + str(index), "type": "message", "role": "assistant",
                    "model": "deepseek-flash", "content": [], "stop_reason": None, "stop_sequence": None,
                    "usage": {"input_tokens": 10, "output_tokens": 0}}},
                {"type": "content_block_start", "index": 0, "content_block": block},
                {"type": "content_block_delta", "index": 0, "delta": delta},
                {"type": "content_block_stop", "index": 0},
                {"type": "message_delta", "delta": {"stop_reason": finish, "stop_sequence": None}, "usage": {"output_tokens": 10}},
                {"type": "message_stop"},
            ]
            response = "".join("event: " + chunk["type"] + "\ndata: " + json.dumps(chunk) + "\n\n" for chunk in chunks).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Content-Length", str(len(response)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(response)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Fixture)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(rid, project["deadline_at"])
    # The fixture only uses local evidence and temporary files.
    adapter = DshRuntimeAdapter(store, control, LocalCredential(), runner=object())
    cid = "suppressed-" + mode
    store.create_conversation(rid, cid, title="Local SDK tool fixture")
    runtime_context = {"conversation_id": cid, "line_id": "line-01", "suppress_chat": True,
        "private_score": mode == "private-score", "discussion_protocol": 1 if mode == "private-score" else 2}
    if mode == "god":
        sections = {"discussion": material}
        snapshot_hash = digest(sections)
        store.put("discussion_finalization_snapshot", "local-finalization", {
            "sections": sections, "snapshot_hash": snapshot_hash}, rid)
        runtime_context.update(finalization_id="local-finalization", snapshot_hash=snapshot_hash)
    if mode == "watchdog":
        store.update_project(rid, status="blocked", error={"code": "discussion_output_exhausted"})
        store.put("watchdog_event", "local-watchdog", {"line_id": "line-01", "direction_epoch": 1,
            "cycle_id": None, "status": "diagnosing", "lease_token": "local-lease"}, rid)
        runtime_context.update(watchdog_event_id="local-watchdog", watchdog_lease_token="local-lease")
    try:
        result = await asyncio.wait_for(adapter.execute_turn(research_id=rid, agent_id=agent, role=role,
            task_id="sdk-tool-fixture", turn_id="turn", prompt="Run only the local tool transport fixture.",
            schema=schema, model={"model": "deepseek-flash", "base_url": f"http://127.0.0.1:{server.server_port}"},
            context={"artifact_id": artifact["id"]}, runtime_context=runtime_context), timeout=65)
        assert result == schema.model_validate(output).model_dump()
        assert len(requests) == 2
        tool_result = json.dumps(requests[1])
        if mode == "private-score":
            assert material not in tool_result
            results = [block for message in requests[1]["messages"] for block in message.get("content", [])
                if isinstance(block, dict) and block.get("type") == "tool_result"]
            assert results and any(block.get("is_error") for block in results), results
        else:
            assert material in tool_result, [block for message in requests[1]["messages"]
                for block in message.get("content", []) if isinstance(block, dict) and block.get("type") == "tool_result"]
            assert tool in {item["name"] for item in requests[0].get("tools", [])}
        if mode == "watchdog":
            assert repaired.read_text(encoding="utf-8").strip() == "native-repaired"
        assert not store.messages(cid)["items"]  # Typed discussion messages are committed by the state machine.
    finally:
        await adapter.close()
        control.close()
        server.shutdown()
        server.server_close()
