"""Concurrent Watch Dog diagnostics share a persisted SDK session safely."""
import asyncio
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

import pytest

from backend.adapters.dsh import DshRuntimeAdapter, runtime_availability
from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor
from backend.runtime.watchdog import WatchdogService


@pytest.mark.skipif(not runtime_availability().get("available"), reason="Optional matching official SDK/runtime unavailable")
@pytest.mark.asyncio
async def test_concurrent_watchdog_diagnostics_resume_the_first_completed_turn(tmp_path):
    requests = []

    class Fixture(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            requests.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            index = len(requests)
            output = json.dumps({"content_markdown": f"LOCAL WATCHDOG FIXTURE {index}", "actions": []})
            chunks = [
                {"type": "message_start", "message": {"id": f"local-{index}", "type": "message",
                    "role": "assistant", "model": "deepseek-flash", "content": [],
                    "stop_reason": None, "stop_sequence": None, "usage": {"input_tokens": 10, "output_tokens": 0}}},
                {"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}},
                {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": output}},
                {"type": "content_block_stop", "index": 0},
                {"type": "message_delta", "delta": {"stop_reason": "end_turn", "stop_sequence": None},
                    "usage": {"output_tokens": 10}},
                {"type": "message_stop"},
            ]
            response = "".join("event: " + chunk["type"] + "\ndata: " + json.dumps(chunk) + "\n\n"
                for chunk in chunks).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Content-Length", str(len(response)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(response)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Fixture)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    store = Store(tmp_path / "managed")
    store.patch("provider", "deepseek-default", base_url=f"http://127.0.0.1:{server.server_port}")
    project = store.start_project(store.create_project({"client_request_id": "watchdog-session-queue",
        "topic": "LOCAL SDK SESSION FIXTURE", "mode": "live", "max_runtime_seconds": 180,
        "team": {"n": 2}})["id"])
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(project["id"], project["deadline_at"])
    runner = SimpleNamespace(status=lambda: {"runs": []}, resources=lambda: {"allocations": []},
        cancel_light_execution=lambda *_: None)
    adapter = DshRuntimeAdapter(store, control,
        SimpleNamespace(get_secret=lambda *args, **kwargs: "local-watchdog-fixture-dummy-key"), runner=runner)
    service = WatchdogService(SimpleNamespace(store=store, control=control, runner=runner, live=adapter))
    events = [store.put("watchdog_event", f"diagnose-{line['line_id']}", {
        "line_id": line["line_id"], "direction_epoch": line["direction_epoch"],
        "cycle_id": line.get("cycle_id"), "status": "diagnosing", "action_ids": [],
        "diagnosis_round": 0, "lease_token": line["line_id"]}, project["id"])
        for line in store.lines(project["id"])]
    try:
        await asyncio.wait_for(asyncio.gather(*(service.diagnose(event) for event in events)), timeout=75)
        assert len(requests) == 2
        assert "LOCAL WATCHDOG FIXTURE 1" in json.dumps(requests[1]["messages"])
        assert len(requests[1]["messages"]) > len(requests[0]["messages"])
        assert all(store.get("watchdog_event", event["id"])["status"] == "awaiting_progress" for event in events)
        executions = store.list("execution", project["id"])
        assert len(executions) == 2 and all(execution["status"] == "completed" for execution in executions)
        assert len({execution["session_id"] for execution in executions}) == 1
        assert not control.status(project["id"])["unfinished"]
    finally:
        await adapter.close()
        control.close()
        server.shutdown()
        server.server_close()
