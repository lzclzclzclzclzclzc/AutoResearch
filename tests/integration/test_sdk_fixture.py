"""Real SDK/runtime against a local transport fixture, never a real model.

This covers the public SDK DeepSeek Messages wire contract, custom tools, guards, streaming,
session persistence and execution deduplication without a user's API key.
"""
import asyncio
import importlib.util
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from backend.adapters.dsh import DshRuntimeAdapter,runtime_availability
from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor
from backend.runtime.runner import RunnerSupervisor
from packages.contracts.research import Proposal


def verified_sdk_present():
    return runtime_availability().get("available",False)


SDK_PRESENT=verified_sdk_present()


class DummyCredential:
    def get_secret(self, *args, **kwargs):
        return "local-transport-fixture-dummy-secret"


@pytest.mark.skipif(not SDK_PRESENT, reason="Optional official SDK/native runtime not installed")
@pytest.mark.asyncio
async def test_official_full_sdk_tools_stream_resume_and_dedup(tmp_path):
    requests = []
    class Fixture(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            requests.append(body)
            index = len(requests)
            if index <= 4:
                name, arguments = {
                    1: ("pwsh", {"command": "Write-Output forbidden"}),
                    2: ("run_light_cpu", {"code": "result=sum([1,2,3])", "evidence_ids": []}),
                    3: ("list_research_inputs", {"path": "", "offset": 0}),
                    4: ("read_research_input", {"relative_path": "input.csv", "offset": 0}),
                }[index]
                block = {"type": "tool_use", "id": f"fixture-call-{index}", "name": name, "input": {}}
                delta = {"type": "input_json_delta", "partial_json": json.dumps(arguments)}
                finish = "tool_use"
            else:
                block = {"type": "text", "text": ""}
                delta = {"type": "text_delta", "text": json.dumps({"content_markdown": "LOCAL TRANSPORT FIXTURE ONLY", "candidate": {"value": 6}, "evidence": []})}
                finish = "end_turn"
            chunks = [
                {"type": "message_start", "message": {"id": f"local-{index}", "type": "message", "role": "assistant", "model": "deepseek-flash", "content": [], "stop_reason": None, "stop_sequence": None, "usage": {"input_tokens": 10, "output_tokens": 0}}},
                {"type": "content_block_start", "index": 0, "content_block": block},
                {"type": "content_block_delta", "index": 0, "delta": delta},
                {"type": "content_block_stop", "index": 0},
                {"type": "message_delta", "delta": {"stop_reason": finish, "stop_sequence": None}, "usage": {"output_tokens": 10}},
                {"type": "message_stop"},
            ]
            if delta["type"] == "text_delta":
                first, rest = delta["text"][:35], delta["text"][35:]
                chunks[2]["delta"] = {"type": "text_delta", "text": first}
                chunks.insert(3, {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": rest}})
            response = "".join("event: " + chunk["type"] + "\ndata: " + json.dumps(chunk) + "\n\n" for chunk in chunks).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Content-Length", str(len(response)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(response)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Fixture)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    inputs = tmp_path / "human-inputs"
    inputs.mkdir()
    (inputs / "input.csv").write_text("x,y\n1,2\n", encoding="utf-8")
    store = Store(tmp_path / "managed")
    project = store.create_project({"client_request_id": "native-sdk-local-fixture", "topic": "LOCAL TRANSPORT FIXTURE", "mode": "live",
        "workspace_path": str(inputs), "team": {"n": 1}, "max_runtime_seconds": 120})
    project = store.start_project(project["id"])
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(project["id"], project["deadline_at"])
    runner = RunnerSupervisor(store.data_dir, control=control)
    adapter = DshRuntimeAdapter(store, control, DummyCredential(), runner=runner)
    conversation_id = "local-sdk-conversation"
    store.create_conversation(project["id"], conversation_id, title="Local transport fixture")
    arguments = dict(research_id=project["id"], agent_id="line-01/exploration/member-01", role="exploration",
        task_id="fixture-task", turn_id="fixture-turn", prompt="Complete the local SDK transport fixture.", schema=Proposal,
        model={"model": "deepseek-flash", "base_url": f"http://127.0.0.1:{server.server_port}"},
        runtime_context={"conversation_id": conversation_id, "line_id": "line-01"})
    try:
        result = await asyncio.wait_for(adapter.execute_turn(**arguments), timeout=75)
        assert result["candidate"]["value"] == 6
        assert len(requests) == 5
        names = {tool["name"] for tool in requests[0].get("tools", [])}
        assert {"read_research_evidence", "read_shared_memory", "send_research_message", "run_light_cpu", "list_research_inputs", "read_research_input"} <= names
        assert not ({"bash", "pwsh", "subagent", "subagent_fork", "run_code"} & names)
        assert "UNKNOWN_TOOL" in json.dumps(requests[1]) or "outside" in json.dumps(requests[1]) or "unknown tool" in json.dumps(requests[1]).lower()
        assert "6" in json.dumps(requests[2])
        assert "input.csv" in json.dumps(requests[3]) and "source_sha256" in json.dumps(requests[4])
        assert any(artifact.get("source_kind") == "human_input" for artifact in store.list("artifact", project["id"]))
        assert not any(event["type"] == "agent.output_delta" for event in store.events(project["id"], limit=2000))
        calls = store.tool_calls(project["id"], conversation_id)["items"]
        assert {call["tool_name"] for call in calls} >= {"run_light_cpu", "list_research_inputs", "read_research_input"}
        assert all("result" in call for call in calls)
        deltas = [event for event in store.events(project["id"], limit=2000) if event["type"] == "message.delta"]
        assert len(deltas) >= 2 and len({event["payload"]["message_id"] for event in deltas}) == 1
        messages = store.messages(conversation_id)["items"]
        assert len(messages) == 1 and messages[0]["content_markdown"] == "LOCAL TRANSPORT FIXTURE ONLY"
        assert messages[0]["status"] == "complete"
        assert DummyCredential().get_secret() not in json.dumps(store.events(project["id"], limit=2000))
        again = await adapter.execute_turn(**arguments)
        assert again == result and len(requests) == 5
        resumed = await adapter.execute_turn(**{**arguments, "turn_id": "resume-turn"})
        assert resumed["candidate"]["value"] == 6 and len(requests) == 6
        assert len(requests[5]["messages"]) > len(requests[0]["messages"])
        jobs = store.list("light_cpu", project["id"])
        assert jobs and all(job["state"] == "completed" for job in jobs)
    finally:
        await adapter.close()
        runner.close()
        control.close()
        server.shutdown()
        server.server_close()


@pytest.mark.skipif(not SDK_PRESENT, reason="Optional verified official SDK/runtime unavailable")
@pytest.mark.asyncio
async def test_stop_during_native_model_stream_keeps_partial_message_and_kills_runtime(tmp_path):
    from backend.adapters.dsh import ResearchStopped
    release = threading.Event()
    class StreamingFixture(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass
        def do_POST(self):
            self.rfile.read(int(self.headers["Content-Length"]))
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Connection", "close")
            self.end_headers()
            events = [
                {"type": "message_start", "message": {"id": "local-cancel", "type": "message", "role": "assistant", "model": "deepseek-flash", "content": [], "usage": {"input_tokens": 10, "output_tokens": 0}}},
                {"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}},
                {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": '{"content_markdown":"actual partial output'}},
            ]
            try:
                self.wfile.write("".join("event: " + event["type"] + "\ndata: " + json.dumps(event) + "\n\n" for event in events).encode())
                self.wfile.flush()
                release.wait(30)
            except (BrokenPipeError, ConnectionResetError):
                pass
    server = ThreadingHTTPServer(("127.0.0.1", 0), StreamingFixture)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    store = Store(tmp_path)
    project = store.start_project(store.create_project({"client_request_id": "native-cancel", "topic": "LOCAL CANCEL FIXTURE", "mode": "live", "max_runtime_seconds": 90})["id"])
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(project["id"], project["deadline_at"])
    runner = RunnerSupervisor(tmp_path, control=control)
    adapter = DshRuntimeAdapter(store, control, DummyCredential(), runner=runner)
    store.create_conversation(project["id"], "stream-cancel", title="Cancellation transport fixture")
    task = asyncio.create_task(adapter.execute_turn(research_id=project["id"], agent_id="line-01/exploration/member-01", role="exploration", task_id="cancel", turn_id="cancel", prompt="Local cancellation fixture", schema=Proposal,
        model={"model": "deepseek-flash", "base_url": f"http://127.0.0.1:{server.server_port}"}, runtime_context={"conversation_id": "stream-cancel", "line_id": "line-01"}))
    try:
        until = time.monotonic() + 25
        while not store.messages("stream-cancel")["items"] and time.monotonic() < until:
            await asyncio.sleep(0.05)
        partial = store.messages("stream-cancel")["items"]
        assert partial and partial[0]["content_markdown"] == "actual partial output"
        assert partial[0]["status"] == "streaming"
        control.terminate(project["id"], "human_terminate")
        with pytest.raises(ResearchStopped):
            await asyncio.wait_for(task, timeout=10)
        final = store.messages("stream-cancel")["items"]
        assert final[0]["content_markdown"] == "actual partial output" and final[0]["status"] == "interrupted"
        assert not adapter.processes
        assert not control.status(project["id"])["unfinished"]
        assert not store.list("verification", project["id"])
    finally:
        release.set()
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await adapter.close()
        runner.close()
        control.close()
        server.shutdown()
        server.server_close()


@pytest.mark.skipif(not SDK_PRESENT, reason="Optional verified official SDK/runtime unavailable")
@pytest.mark.asyncio
async def test_official_sdk_message_tool_reaches_receiver_context_and_delivers_reply(tmp_path):
    from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
    from backend.graphs.engine import ResearchEngine
    from backend.graphs.tasks import CommunicationGraph
    requests = []
    class MessageFixture(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            requests.append(body)
            index = len(requests)
            if index == 1:
                block = {"type": "tool_use", "id": "native-send-call", "name": "send_research_message", "input": {}}
                delta = {"type": "input_json_delta", "partial_json": json.dumps({"recipient_ids": ["boss"], "content_markdown": "LOCAL_PROTOCOL_QUESTION: which validation split?"})}
                finish = "tool_use"
            else:
                output = {2: {"content_markdown": "LOCAL SENDER FIXTURE", "candidate": {"sent": True}, "evidence": []},
                    3: {"content_markdown": "LOCAL RECEIVER FIXTURE: fix the split before fitting."}}[index]
                block, delta, finish = {"type": "text", "text": ""}, {"type": "text_delta", "text": json.dumps(output)}, "end_turn"
            chunks = [
                {"type": "message_start", "message": {"id": f"native-mail-{index}", "type": "message", "role": "assistant", "model": "deepseek-flash", "content": [], "stop_reason": None, "usage": {"input_tokens": 10, "output_tokens": 0}}},
                {"type": "content_block_start", "index": 0, "content_block": block},
                {"type": "content_block_delta", "index": 0, "delta": delta},
                {"type": "content_block_stop", "index": 0},
                {"type": "message_delta", "delta": {"stop_reason": finish, "stop_sequence": None}, "usage": {"output_tokens": 10}},
                {"type": "message_stop"},
            ]
            data = "".join("event: " + c["type"] + "\ndata: " + json.dumps(c) + "\n\n" for c in chunks).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(data)
    server = ThreadingHTTPServer(("127.0.0.1", 0), MessageFixture)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    store = Store(tmp_path)
    store.patch("provider", "deepseek-default", base_url=f"http://127.0.0.1:{server.server_port}")
    p = store.start_project(store.create_project({"client_request_id": "native-message", "topic": "LOCAL TRANSPORT FIXTURE ONLY",
        "mode": "live", "team": {"boss_members": 1}, "max_runtime_seconds": 120})["id"])
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(p["id"], p["deadline_at"])
    runner = RunnerSupervisor(tmp_path, control)
    try:
        async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "checkpoints.sqlite")) as saver:
            engine = ResearchEngine(store, control, runner, DummyCredential(), saver)
            sender = "line-01/exploration/member-01"
            result = await asyncio.wait_for(engine.turn(p["id"], sender, "exploration", "native-send", "send", Proposal,
                {"instruction": "Local protocol fixture."}, line_id="line-01", conversation_id="native-mail"), 60)
            assert result["candidate"]["sent"]
            jobs = [t for t in store.pending_tasks() if t["kind"] == "communication"]
            assert len(jobs) == 1 and len(store.list("inbox", p["id"])) == 1
            graph = CommunicationGraph(engine.communication, saver)
            reply = await asyncio.wait_for(graph.run(jobs[0]), 60)
            assert "LOCAL RECEIVER FIXTURE" in reply["content_markdown"]
            wire = json.dumps(requests[2], ensure_ascii=False)
            assert "received_messages" in wire and "LOCAL_PROTOCOL_QUESTION" in wire
            assert requests[0]["max_tokens"] == requests[2]["max_tokens"] == 65536
            inbox = store.consume_agent_inbox(p["id"], sender, "receive-native-reply", control, 1)
            assert inbox[0]["message"]["content_markdown"] == reply["content_markdown"]
            await graph.run(jobs[0])
            assert len(requests) == 3
            assert not store.list("cycle", p["id"]) and not store.list("run", p["id"])
            await engine.close()
    finally:
        runner.close()
        control.close()
        server.shutdown()
        server.server_close()
