"""Native SDK HTTP failures use only a local server and an inert dummy key."""
import asyncio
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from backend.adapters.dsh import DshRuntimeAdapter, RuntimeBlocked, runtime_availability
from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor
from backend.runtime.runner import RunnerSupervisor
from packages.contracts.research import Proposal


@pytest.mark.skipif(not runtime_availability().get("available"), reason="Optional matching official SDK/runtime unavailable")
@pytest.mark.asyncio
async def test_native_sdk_http_404_is_a_readable_persisted_endpoint_failure(tmp_path):
    dummy_secret = "native-local-http-error-dummy-secret"
    requests = []
    class Credentials:
        def get_secret(self, *args, **kwargs):
            return dummy_secret
    class Fixture(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass
        def do_POST(self):
            self.rfile.read(int(self.headers["Content-Length"]))
            requests.append(self.path)
            body = json.dumps({"type": "error", "error": {"type": "not_found_error",
                "message": "RAW_UPSTREAM_DETAIL " + dummy_secret}}).encode("utf-8")
            self.send_response(404)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(body)
    server = ThreadingHTTPServer(("127.0.0.1", 0), Fixture)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    store = Store(tmp_path)
    project = store.start_project(store.create_project({"client_request_id": "native-404-regression", "topic": "LOCAL HTTP ERROR FIXTURE",
        "mode": "live", "max_runtime_seconds": 90, "team": {"n": 1}})["id"])
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(project["id"], project["deadline_at"])
    runner = RunnerSupervisor(tmp_path, control=control)
    adapter = DshRuntimeAdapter(store, control, Credentials(), runner=runner)
    try:
        with pytest.raises(RuntimeBlocked) as caught:
            await asyncio.wait_for(adapter.execute_turn(research_id=project["id"], agent_id="boss/member-01", role="boss",
                task_id="http-error-fixture", turn_id="first", prompt="LOCAL DUMMY HTTP 404 FIXTURE", schema=Proposal,
                model={"model": "deepseek-flash", "base_url": f"http://127.0.0.1:{server.server_port}"},
                runtime_context={"conversation_id": "http-error-fixture"}), timeout=40)
        failure = caught.value
        assert failure.code == "model_endpoint_error" and failure.http_status == 404
        assert failure.provider_error_code == "HTTP_404" and "模型接口地址" in str(failure)
        assert "404" in str(failure) and "RAW_UPSTREAM_DETAIL" not in str(failure) and dummy_secret not in str(failure)
        assert len(requests) == 1  # Invalid endpoint failure does not create a retry storm.
        execution = store.list("execution", project["id"])[0]
        assert execution["status"] == "failed" and execution["error_code"] == failure.code
        assert execution["error_message"] == str(failure) and execution["http_status"] == 404
        assert execution["provider_error_code"] == "HTTP_404"
        events = store.events(project["id"], limit=2000)
        failed = next(event["payload"] for event in events if event["type"] == "agent.failed")
        assert all(failed[key] == value for key, value in execution["error"].items())
        assert "RAW_UPSTREAM_DETAIL" not in json.dumps(failed)
        root_errors = [event["payload"]["notification"]["payload"]["event"]["data"]["reason"]["error"]
            for event in events if event["type"] == "agent.runtime_event"
            and event["payload"]["notification"].get("method") == "session.event"
            and event["payload"]["notification"].get("payload", {}).get("event", {}).get("type") == "turn/end"]
        assert root_errors and root_errors[-1]["code"] == "HTTP_404" and root_errors[-1]["status"] == 404
        assert dummy_secret not in json.dumps(events)
        assert not control.status(project["id"])["unfinished"]
        assert not store.list("verification", project["id"])
    finally:
        await adapter.close()
        runner.close()
        control.close()
        server.shutdown()
        server.server_close()
