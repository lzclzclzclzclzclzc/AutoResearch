import json
import os
import subprocess
import threading

import pytest

from backend.adapters.gateway import ToolGateway
from backend.adapters.inputs import InputAccessError
from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor


@pytest.fixture
def inputs(tmp_path):
    root = tmp_path / "human-inputs"
    root.mkdir()
    store = Store(tmp_path / "managed")
    project = store.create_project({"client_request_id": "input-tools", "topic": "Input policy fixture", "mode": "demo", "workspace_path": str(root)})
    project = store.start_project(project["id"])
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(project["id"], project["deadline_at"])
    gateway = ToolGateway.__new__(ToolGateway)
    gateway.store, gateway.control, gateway.runner = store, control, None
    gateway.lock, gateway.capabilities = threading.RLock(), {}
    token = gateway.register(research_id=project["id"], role="exploration", agent_id="line-01/exploration/1",
        line_id="line-01", control_generation=1, task_id="input-task", execution_id="input-execution", conversation_id="inputs")
    yield gateway, token, root, project
    control.close()


def call(inputs, name, **arguments):
    gateway, token, *_ = inputs
    return gateway.call(token, {"name": name, "arguments": arguments})


def test_authorized_csv_and_python_are_read_only_immutable_evidence(inputs):
    gateway, _, root, project = inputs
    (root / "data.csv").write_text("x,y\n1,2\n3,4\n", encoding="utf-8")
    (root / "analysis.py").write_text("# 输入源码\nresult = 2 + 3\n", encoding="utf-8")
    assert {entry["name"] for entry in call(inputs, "list_research_inputs")["entries"]} == {"analysis.py", "data.csv"}
    csv = call(inputs, "read_research_input", relative_path="data.csv")
    python = call(inputs, "read_research_input", relative_path="analysis.py")
    assert csv["text"].splitlines() == ["x,y", "1,2", "3,4"] and "输入源码" in python["text"]
    metadata, backing = gateway.store.artifact(csv["artifact_id"])
    assert metadata["line_id"] == "line-01" and metadata["source_kind"] == "human_input"
    assert metadata["sha256"] == csv["source_sha256"]
    (root / "data.csv").write_text("changed", encoding="utf-8")
    assert backing.read_text(encoding="utf-8") == "x,y\n1,2\n3,4\n"
    assert len([event for event in gateway.store.events(project["id"]) if event["type"] == "research.input_read"]) == 2


@pytest.mark.parametrize("path", ["../outside.py", "..\\outside.py", "C:\\Windows\\win.ini", "/Windows/win.ini", "data.csv:secret", ".env", ".ssh/id_rsa", "my_api_key.txt"])
def test_traversal_and_credential_paths_are_denied(inputs, path):
    with pytest.raises((InputAccessError, PermissionError)):
        call(inputs, "read_research_input", relative_path=path)


def test_input_listing_is_paginated_and_hides_credentials(inputs):
    _, _, root, _ = inputs
    (root / ".env").write_text("secret", encoding="utf-8")
    for index in range(105):
        (root / f"item-{index:03d}.py").write_text("pass", encoding="utf-8")
    first = call(inputs, "list_research_inputs")
    second = call(inputs, "list_research_inputs", offset=first["next_offset"])
    assert len(first["entries"]) == 100 and len(second["entries"]) == 5
    assert first["next_offset"] == 100 and second["next_offset"] is None
    assert all(entry["name"] != ".env" for entry in first["entries"] + second["entries"])


def test_input_chunk_transport_is_bounded_and_advances(inputs):
    _, _, root, _ = inputs
    content = "\t" * 100000
    (root / "large.txt").write_text(content, encoding="utf-8")
    offset, text = 0, ""
    while True:
        chunk = call(inputs, "read_research_input", relative_path="large.txt", offset=offset)
        assert len(json.dumps(chunk, ensure_ascii=False).encode("utf-8")) <= 64 * 1024
        text += chunk["text"]
        offset = chunk["next_offset"]
        if chunk["eof"]:
            break
    assert text == content


def test_managed_storage_missing_input_and_frozen_paper_are_denied(inputs):
    gateway, _, root, project = inputs
    paper = gateway.register(research_id=project["id"], role="paper", agent_id="paper", line_id=None,
        control_generation=1, task_id="paper-task", execution_id="paper", conversation_id="paper")
    with pytest.raises(PermissionError):
        gateway.call(paper, {"name": "list_research_inputs", "arguments": {}})
    gateway.store.update_project(project["id"], workspace_path=str(gateway.store.data_dir))
    with pytest.raises(InputAccessError):
        call(inputs, "list_research_inputs")
    gateway.store.update_project(project["id"], workspace_path=str(root.parent))
    assert "managed" not in {entry["name"] for entry in call(inputs, "list_research_inputs")["entries"]}
    gateway.store.update_project(project["id"], workspace_path=None)
    with pytest.raises(InputAccessError, match="No human-selected"):
        call(inputs, "list_research_inputs")


def test_reparse_or_symlink_to_outside_input_is_denied(inputs):
    _, _, root, _ = inputs
    outside = root.parent / "outside.py"
    outside.write_text("outside = True", encoding="utf-8")
    relative = "escape.py"
    try:
        os.symlink(outside, root / "escape.py")
    except OSError:
        if os.name != "nt":
            pytest.skip("Creating symlinks is unavailable")
        target = root.parent / "outside-junction"
        target.mkdir()
        (target / "outside.py").write_text("outside = True", encoding="utf-8")
        junction = root / "escape-junction"
        created = subprocess.run(["cmd", "/c", "mklink", "/J", str(junction), str(target)],
            capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
        if created.returncode:
            pytest.skip("Neither symlink nor owned test directory junction creation is available")
        relative = "escape-junction/outside.py"
    with pytest.raises(InputAccessError):
        call(inputs, "read_research_input", relative_path=relative)
    assert not call(inputs, "list_research_inputs")["entries"]
