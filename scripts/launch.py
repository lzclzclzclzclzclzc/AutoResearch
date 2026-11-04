"""Start the independently supervised local services, with control available first."""
from __future__ import annotations

import argparse
import contextlib
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import sys
import time
import urllib.request
import webbrowser

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


class WorkerWatchdog:
    """Start a missing/exited Worker; its exclusive lock and recovery fence admission."""
    def __init__(self):
        self.retry_at = None
        self.delay = 1
        self.started_at = time.monotonic()

    def check(self, process, restart, current_time=None, *, ready=None):
        current_time = time.monotonic() if current_time is None else current_time
        if process is not None and process.poll() is None:
            if current_time - self.started_at >= 60:
                self.delay = 1
            return False
        if self.retry_at is None:
            self.retry_at = current_time + self.delay
            self.delay = min(30, self.delay * 2)
        if current_time < self.retry_at:
            return False
        if ready is not None and not ready():
            self.retry_at = current_time + self.delay
            self.delay = min(30, self.delay * 2)
            return False
        restart()
        self.retry_at = None
        self.started_at = current_time
        return True


def request(url, token, body=None, timeout=1.5):
    req = urllib.request.Request(url, data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json", "X-AutoResearch-Token": token})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.load(response)


@contextlib.contextmanager
def lock(directory):
    path = directory / "launcher.lock"
    with path.open("a+b") as handle:
        handle.seek(0); handle.write(b"0"); handle.flush(); handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise SystemExit("此数据目录已有启动器。请打开 http://127.0.0.1:8000 或先关闭原启动器。")
        yield


def main():
    parser = argparse.ArgumentParser(description="AutoResearch 本机启动器")
    parser.add_argument("--data-dir", default=str(ROOT / "data"))
    parser.add_argument("--port", default=8000, type=int)
    parser.add_argument("--control-port", default=8766, type=int)
    parser.add_argument("--runner-port", default=8767, type=int)
    parser.add_argument("--no-browser", action="store_true")
    args = parser.parse_args()
    directory = Path(args.data_dir).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    if len(set([args.port, args.control_port, args.runner_port])) != 3:
        raise SystemExit("三个服务需要不同端口。")
    for port in (args.port, args.control_port, args.runner_port):
        with socket.socket() as test:
            try:
                test.bind(("127.0.0.1", port))
            except OSError:
                raise SystemExit(f"端口 {port} 已被占用。为避免影响现有进程，启动器已退出。")
    control_url = f"http://127.0.0.1:{args.control_port}"
    runner_url = f"http://127.0.0.1:{args.runner_port}"
    api_url = f"http://127.0.0.1:{args.port}"
    token = secrets.token_urlsafe(48)
    env = {**os.environ, "PYTHONPATH": str(ROOT) + os.pathsep + os.environ.get("PYTHONPATH", ""), "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8",
        "AUTORESEARCH_SESSION_TOKEN": token, "AUTORESEARCH_DATA_DIR": str(directory),
        "AUTORESEARCH_CONTROL_URL": control_url, "AUTORESEARCH_RUNNER_URL": runner_url,
        "LANGCHAIN_TRACING_V2": "false", "LANGSMITH_TRACING": "false"}
    children, logs = {}, {}
    worker_watchdog = WorkerWatchdog()
    shutdown = False
    def signal_stop(*_):
        nonlocal shutdown
        shutdown = True
    signal.signal(signal.SIGINT, signal_stop)
    signal.signal(signal.SIGTERM, signal_stop)

    def spawn(name, argv):
        if name in logs:
            logs[name].close()
        log = (directory / f"{name}.log").open("ab", buffering=0)
        logs[name] = log
        children[name] = subprocess.Popen([sys.executable, *argv], cwd=ROOT, env=env,
            stdin=subprocess.DEVNULL, stdout=log, stderr=log,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
        return children[name]

    def spawn_worker():
        return spawn("worker", ["-m", "backend.runtime.worker", "--data-dir", str(directory),
            "--control-url", control_url, "--runner-url", runner_url])

    def runner_ready():
        try:
            return request(f"{runner_url}/health", token, timeout=.5).get("status") == "ok"
        except Exception:
            return False

    def ready(name, url):
        until = time.monotonic() + 25
        while time.monotonic() < until:
            if children[name].poll() is not None:
                raise RuntimeError(f"{name} 启动失败；查看 {directory / (name + '.log')}")
            try:
                request(url, token)
                return
            except Exception:
                time.sleep(.15)
        raise RuntimeError(f"{name} 未能就绪；查看本机日志。")

    with lock(directory):
        try:
            spawn("control", ["-m", "backend.runtime.control", "--data-dir", str(directory / "control"), "--port", str(args.control_port)])
            ready("control", f"{control_url}/control-status")
            print("独立停止通道已就绪。", flush=True)
            spawn("api", ["-m", "uvicorn", "apps.api.main:app", "--host", "127.0.0.1", "--port", str(args.port), "--log-level", "warning"])
            ready("api", f"{api_url}/api/session")
            # API remains available for control and diagnostics if later startup fails.
            try:
                spawn("runner", ["-m", "backend.runtime.runner", "--data-dir", str(directory), "--port", str(args.runner_port), "--control-url", control_url])
                ready("runner", f"{runner_url}/health")
                spawn_worker()
            except RuntimeError as error:
                print(f"运行组件未就绪：{error}；停止与诊断入口继续提供服务。", flush=True)
            print(f"AutoResearch: {api_url}\n按 Ctrl+C 停止服务与在途研究。", flush=True)
            if not args.no_browser:
                webbrowser.open(api_url)
            while not shutdown:
                if children["control"].poll() is not None:
                    print("控制服务退出，启动门禁关闭，正在清理本启动器管理的进程。", flush=True)
                    break
                if children["api"].poll() is not None:
                    print("API 退出；独立控制仍可清理研究。正在关闭本次服务。", flush=True)
                    break
                # Runner may finish historical recovery after the initial ready()
                # timeout. Keep probing without replacing that live Runner, then
                # start the missing Worker under its ordinary exclusive lock.
                if worker_watchdog.check(children.get("worker"), spawn_worker, ready=runner_ready):
                    print("Worker 已启动，正在恢复执行状态并重新调度；详情见 worker.log。", flush=True)
                time.sleep(.5)
        finally:
            # This only uses control's in-memory registry, never business SQLite.
            try:
                state = request(f"{control_url}/control-status", token)
                for rid, project in state.get("projects", {}).items():
                    if not project.get("completed"):
                        request(f"{control_url}/terminate", token, {"research_id": rid, "reason": "launcher_shutdown"})
            except Exception:
                pass
            from backend.runtime.processes import terminate_identity
            import psutil
            # Stop worker, then Runner, and retain control until both are gone.
            for name in ("worker", "runner", "api", "control"):
                process = children.get(name)
                if process and process.poll() is None:
                    try:
                        terminate_identity(process.pid, psutil.Process(process.pid).create_time())
                    except (OSError, psutil.Error):
                        pass
            for log in logs.values():
                log.close()


if __name__ == "__main__":
    main()
