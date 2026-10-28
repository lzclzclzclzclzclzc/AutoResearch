import os
import subprocess
import sys
import time

import psutil
import pytest
from backend.runtime.processes import identity_alive,terminate_identity


@pytest.mark.skipif(os.name != "nt", reason="Windows production containment")
def test_sdk_owner_death_closes_job_and_kills_descendants():
    code = ("from backend.adapters.sdk_child import contain_windows_runtime; import subprocess,sys,time; "
        "job=contain_windows_runtime(); child=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)']); "
        "print(child.pid,flush=True);time.sleep(60)")
    owner = subprocess.Popen([sys.executable, "-c", code], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             text=True, creationflags=0x08000000)
    child_pid = None
    try:
        line = owner.stdout.readline().strip()
        assert line, owner.stderr.read()
        child_pid = int(line)
        child = psutil.Process(child_pid)
        identity = child.create_time()
        owner.kill()
        owner.wait(timeout=5)
        until = time.monotonic() + 5
        while time.monotonic() < until:
            if not psutil.pid_exists(child_pid):
                break
            time.sleep(0.05)
        assert not psutil.pid_exists(child_pid) or psutil.Process(child_pid).create_time() != identity
    finally:
        if owner.poll() is None:
            owner.kill()
            owner.wait(timeout=5)
        if child_pid and psutil.pid_exists(child_pid):
            psutil.Process(child_pid).kill()


def test_sdk_child_exits_before_startup_when_owner_pipe_closes():
    owner = subprocess.Popen([sys.executable, "-m", "backend.adapters.sdk_child"], stdin=subprocess.PIPE,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, creationflags=0x08000000 if os.name == "nt" else 0)
    owner.stdin.close()
    owner.wait(timeout=5)
    assert owner.returncode != 0


def test_worker_crash_before_payload_closes_owner_stdin_without_orphan_runtime():
    helper="""import subprocess,sys,time,json,psutil
owner=subprocess.Popen([sys.executable,'-m','backend.adapters.sdk_child'],stdin=subprocess.PIPE,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
print(json.dumps([owner.pid,psutil.Process(owner.pid).create_time()]),flush=True)
time.sleep(60)
"""
    worker=subprocess.Popen([sys.executable,"-c",helper],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,
        creationflags=0x08000000 if os.name=="nt" else 0)
    owner_pid=created=None
    try:
        import json
        line=worker.stdout.readline()
        assert line,worker.stderr.read()
        owner_pid,created=json.loads(line)
        descendants=psutil.Process(owner_pid).children(recursive=True)
        # Windows virtualenv launchers may have an interpreter child; no native
        # Harness/tool runtime can exist before the payload has been delivered.
        assert all(child.name().lower().startswith("python") for child in descendants)
        identities=[(owner_pid,created)]+[(child.pid,child.create_time()) for child in descendants]
        worker.kill();worker.wait(timeout=3)
        until=time.monotonic()+5
        while any(identity_alive(pid,born) for pid,born in identities) and time.monotonic()<until:time.sleep(.05)
        assert not any(identity_alive(pid,born) for pid,born in identities)
    finally:
        if worker.poll() is None:worker.kill();worker.wait(timeout=3)
        if owner_pid and identity_alive(owner_pid,created):terminate_identity(owner_pid,created)
