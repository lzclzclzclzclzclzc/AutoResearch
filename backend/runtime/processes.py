"""Process identity checks and Windows create-in-job containment.

Windows uses STARTUPINFOEX JOB_LIST at CreateProcess time, never the unsafe
create-then-assign sequence. Unsupported hosts fail closed. POSIX is a development
backend using an isolated session/process group.
"""
from __future__ import annotations

import ctypes
import os
import shutil
import signal
import subprocess
import threading
import time
from pathlib import Path
from typing import Any


def process_identity(pid: int) -> float | None:
    try:
        import psutil
        return psutil.Process(pid).create_time()
    except (ImportError, OSError, Exception):
        return None


def identity_alive(pid: int | None, create_time: float | None) -> bool:
    if not pid or create_time is None:
        return False
    try:
        import psutil
        proc = psutil.Process(pid)
        return abs(proc.create_time() - create_time) < 0.02 and proc.is_running() and proc.status() != psutil.STATUS_ZOMBIE
    except (ImportError, OSError, Exception):
        return False


def terminate_identity(pid: int, create_time: float | None, timeout: float = 2) -> bool:
    """Never terminate a recycled PID. Unknown identity cannot prove cleanup."""
    if create_time is None:
        return False
    try:
        import psutil
        try:
            parent = psutil.Process(pid)
        except psutil.NoSuchProcess:
            return True
        if abs(parent.create_time() - create_time) >= 0.02:
            return True
        children = parent.children(recursive=True)
        for proc in [*reversed(children), parent]:
            try:
                proc.kill()
            except psutil.NoSuchProcess:
                pass
        _, alive = psutil.wait_procs([*children, parent], timeout=timeout)
        return not alive
    except Exception:
        return False


def sanitized_environment(overrides: dict | None = None, gpu_uuids: list[str] | None = None) -> dict[str, str]:
    # Explicit baseline: credentials from the supervisor are never inherited by experiments.
    names = ("PATH", "SYSTEMROOT", "WINDIR", "COMSPEC", "PATHEXT", "TEMP", "TMP", "HOME", "USERPROFILE",
        "APPDATA", "LOCALAPPDATA", "PROGRAMFILES", "PROGRAMFILES(X86)", "PROGRAMDATA", "LANG", "LC_ALL")
    safe = {key: value for key, value in os.environ.items() if key.upper() in names}
    for key, value in (overrides or {}).items():
        upper = key.upper()
        if any(part in upper for part in ("KEY", "TOKEN", "PASSWORD", "SECRET", "AUTHORIZATION", "CREDENTIAL")):
            raise ValueError("Credential-like environment variables are forbidden in experiments")
        safe[str(key)] = str(value)
    safe["PYTHONUNBUFFERED"] = "1"
    safe["CUDA_VISIBLE_DEVICES"] = ",".join(gpu_uuids or [])
    return safe


if os.name == "nt":
    from ctypes import wintypes as w
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    SIZE_T = ctypes.c_size_t
    ULONG_PTR = ctypes.c_size_t

    class STARTUPINFO(ctypes.Structure):
        _fields_ = [("cb", w.DWORD), ("lpReserved", w.LPWSTR), ("lpDesktop", w.LPWSTR),
            ("lpTitle", w.LPWSTR), ("dwX", w.DWORD), ("dwY", w.DWORD), ("dwXSize", w.DWORD),
            ("dwYSize", w.DWORD), ("dwXCountChars", w.DWORD), ("dwYCountChars", w.DWORD),
            ("dwFillAttribute", w.DWORD), ("dwFlags", w.DWORD), ("wShowWindow", w.WORD),
            ("cbReserved2", w.WORD), ("lpReserved2", ctypes.POINTER(w.BYTE)),
            ("hStdInput", w.HANDLE), ("hStdOutput", w.HANDLE), ("hStdError", w.HANDLE)]

    class STARTUPINFOEX(ctypes.Structure):
        _fields_ = [("StartupInfo", STARTUPINFO), ("lpAttributeList", w.LPVOID)]

    class PROCESS_INFORMATION(ctypes.Structure):
        _fields_ = [("hProcess", w.HANDLE), ("hThread", w.HANDLE), ("dwProcessId", w.DWORD), ("dwThreadId", w.DWORD)]

    class SECURITY_ATTRIBUTES(ctypes.Structure):
        _fields_ = [("nLength", w.DWORD), ("lpSecurityDescriptor", w.LPVOID), ("bInheritHandle", w.BOOL)]

    class BASIC_LIMIT(ctypes.Structure):
        _fields_ = [("PerProcessUserTimeLimit", ctypes.c_int64), ("PerJobUserTimeLimit", ctypes.c_int64),
            ("LimitFlags", w.DWORD), ("MinimumWorkingSetSize", SIZE_T), ("MaximumWorkingSetSize", SIZE_T),
            ("ActiveProcessLimit", w.DWORD), ("Affinity", ULONG_PTR), ("PriorityClass", w.DWORD), ("SchedulingClass", w.DWORD)]

    class IO_COUNTERS(ctypes.Structure):
        _fields_ = [(name, ctypes.c_uint64) for name in ("ReadOperationCount", "WriteOperationCount", "OtherOperationCount",
            "ReadTransferCount", "WriteTransferCount", "OtherTransferCount")]

    class EXTENDED_LIMIT(ctypes.Structure):
        _fields_ = [("BasicLimitInformation", BASIC_LIMIT), ("IoInfo", IO_COUNTERS),
            ("ProcessMemoryLimit", SIZE_T), ("JobMemoryLimit", SIZE_T),
            ("PeakProcessMemoryUsed", SIZE_T), ("PeakJobMemoryUsed", SIZE_T)]

    class CPU_RATE(ctypes.Structure):
        _fields_ = [("ControlFlags", w.DWORD), ("CpuRate", w.DWORD)]

    class BASIC_ACCOUNTING(ctypes.Structure):
        _fields_ = [(name, ctypes.c_int64) for name in ("TotalUserTime", "TotalKernelTime", "PeriodUserTime", "PeriodKernelTime")] + [
            (name, w.DWORD) for name in ("PageFaults", "TotalProcesses", "ActiveProcesses", "TerminatedProcesses")]

    kernel.CreateJobObjectW.argtypes = [ctypes.POINTER(SECURITY_ATTRIBUTES), w.LPCWSTR]
    kernel.CreateJobObjectW.restype = w.HANDLE
    kernel.SetInformationJobObject.argtypes = [w.HANDLE, ctypes.c_int, w.LPVOID, w.DWORD]
    kernel.SetInformationJobObject.restype = w.BOOL
    kernel.QueryInformationJobObject.argtypes = [w.HANDLE, ctypes.c_int, w.LPVOID, w.DWORD, ctypes.POINTER(w.DWORD)]
    kernel.QueryInformationJobObject.restype = w.BOOL
    kernel.InitializeProcThreadAttributeList.argtypes = [w.LPVOID, w.DWORD, w.DWORD, ctypes.POINTER(SIZE_T)]
    kernel.InitializeProcThreadAttributeList.restype = w.BOOL
    kernel.UpdateProcThreadAttribute.argtypes = [w.LPVOID, w.DWORD, SIZE_T, w.LPVOID, SIZE_T, w.LPVOID, w.LPVOID]
    kernel.UpdateProcThreadAttribute.restype = w.BOOL
    kernel.DeleteProcThreadAttributeList.argtypes = [w.LPVOID]
    kernel.CreateProcessW.argtypes = [w.LPCWSTR, w.LPWSTR, w.LPVOID, w.LPVOID, w.BOOL, w.DWORD,
        w.LPVOID, w.LPCWSTR, ctypes.POINTER(STARTUPINFOEX), ctypes.POINTER(PROCESS_INFORMATION)]
    kernel.CreateProcessW.restype = w.BOOL
    kernel.CreateFileW.argtypes = [w.LPCWSTR, w.DWORD, w.DWORD, ctypes.POINTER(SECURITY_ATTRIBUTES), w.DWORD, w.DWORD, w.HANDLE]
    kernel.CreateFileW.restype = w.HANDLE
    kernel.CloseHandle.argtypes = [w.HANDLE]
    kernel.CloseHandle.restype = w.BOOL
    kernel.ResumeThread.argtypes = [w.HANDLE]
    kernel.ResumeThread.restype = w.DWORD
    kernel.TerminateJobObject.argtypes = [w.HANDLE, w.UINT]
    kernel.TerminateJobObject.restype = w.BOOL
    kernel.GetExitCodeProcess.argtypes = [w.HANDLE, ctypes.POINTER(w.DWORD)]
    kernel.GetExitCodeProcess.restype = w.BOOL
    kernel.WaitForSingleObject.argtypes = [w.HANDLE, w.DWORD]
    kernel.WaitForSingleObject.restype = w.DWORD

    def _check(result: Any, operation: str) -> Any:
        if not result:
            raise OSError(ctypes.get_last_error(), f"Windows process containment failed: {operation}")
        return result


class ManagedProcess:
    """The supervisor exclusively owns the Job handle. Child code starts on resume()."""

    def __init__(self, command: list[str], cwd: str | Path, log_path: str | Path,
                 env: dict | None = None, cpu_cores: int | None = None, memory_bytes: int | None = None,
                 defer_creation: bool = False):
        self._handle_lock = threading.RLock()
        self.closed = False
        self.exit_code = None
        if not command or not all(isinstance(v, str) and "\x00" not in v for v in command):
            raise ValueError("Command must be a nonempty argument list")
        self.job = self.handle = self.thread = None
        self.proc = None
        self.closed = False
        self.suspended = True
        self.pid = 0
        self.create_time = None
        cwd = str(Path(cwd).resolve())
        self._command, self._cwd, self._env = command, cwd, env or {}
        Path(log_path).parent.mkdir(parents=True, exist_ok=True)
        if os.name == "nt":
            self._windows_prepare(command, cwd, str(log_path), env or {}, cpu_cores, memory_bytes)
        else:
            self._log = open(log_path, "ab", buffering=0)
        if not defer_creation:
            self.create_suspended()
            self.capture_identity()

    def create_suspended(self) -> None:
        if self.pid:
            raise RuntimeError("Process operation has already created its process")
        if os.name == "nt":
            try:
                pi = PROCESS_INFORMATION()
                _check(kernel.CreateProcessW(self._executable, self._command_line, None, None, True,
                    0x00080000 | 0x00000004 | 0x00000400 | 0x08000000, self._env_block, self._cwd,
                    ctypes.byref(self._startup_info), ctypes.byref(pi)), "CreateProcessW in Job")
                self.handle, self.thread, self.pid = pi.hProcess, pi.hThread, pi.dwProcessId
            finally:
                self._cleanup_prepared_handles()
        else:
            # Development backend starts stopped via a tiny POSIX wrapper, before exec of the command.
            import sys
            wrapper = "import os,signal,sys;os.kill(os.getpid(),signal.SIGSTOP);os.execvpe(sys.argv[1],sys.argv[1:],os.environ)"
            self.proc = subprocess.Popen([sys.executable, "-c", wrapper, *self._command], cwd=self._cwd,
                stdout=self._log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, env=self._env, start_new_session=True)
            self.pid = self.proc.pid

    def capture_identity(self) -> None:
        for _ in range(30):
            self.create_time = process_identity(self.pid)
            if self.create_time is not None:
                break
            time.sleep(0.01)
        if self.create_time is None:
            self.terminate()
            raise RuntimeError("Could not establish stable process identity")

    def _windows_prepare(self, command, cwd, log_path, env, cpu_cores, memory_bytes):
        self.job = _check(kernel.CreateJobObjectW(None, None), "CreateJobObjectW")
        self._attributes = None
        self._inheritable = []
        try:
            limits = EXTENDED_LIMIT()
            limits.BasicLimitInformation.LimitFlags = 0x2000  # KILL_ON_JOB_CLOSE; no breakaway.
            if memory_bytes:
                limits.BasicLimitInformation.LimitFlags |= 0x200  # JOB_MEMORY
                limits.JobMemoryLimit = int(memory_bytes)
            _check(kernel.SetInformationJobObject(self.job, 9, ctypes.byref(limits), ctypes.sizeof(limits)), "Job limits")
            if cpu_cores:
                rate = CPU_RATE(1 | 4, max(1, min(10000, int(cpu_cores / (os.cpu_count() or 1) * 10000))))
                _check(kernel.SetInformationJobObject(self.job, 15, ctypes.byref(rate), ctypes.sizeof(rate)), "CPU rate")
            sa = SECURITY_ATTRIBUTES(ctypes.sizeof(SECURITY_ATTRIBUTES), None, True)
            for path, access, disposition in (("NUL", 0x80000000, 3), (log_path, 0x40000000, 2)):
                handle = kernel.CreateFileW(path, access, 1 | 2, ctypes.byref(sa), disposition, 0x80, None)
                if handle in (None, ctypes.c_void_p(-1).value):
                    _check(False, "CreateFileW")
                self._inheritable.append(handle)
            size = SIZE_T(0)
            kernel.InitializeProcThreadAttributeList(None, 2, 0, ctypes.byref(size))
            self._attributes = ctypes.create_string_buffer(size.value)
            _check(kernel.InitializeProcThreadAttributeList(self._attributes, 2, 0, ctypes.byref(size)), "Attribute list")
            self._jobs = (w.HANDLE * 1)(self.job)
            self._handles = (w.HANDLE * 2)(*self._inheritable)
            _check(kernel.UpdateProcThreadAttribute(self._attributes, 0, 0x0002000D, self._jobs, ctypes.sizeof(self._jobs), None, None), "JOB_LIST unsupported")
            _check(kernel.UpdateProcThreadAttribute(self._attributes, 0, 0x00020002, self._handles, ctypes.sizeof(self._handles), None, None), "HANDLE_LIST")
            info = STARTUPINFOEX()
            info.StartupInfo.cb = ctypes.sizeof(info)
            info.StartupInfo.dwFlags = 0x100
            info.StartupInfo.hStdInput, info.StartupInfo.hStdOutput = self._inheritable
            info.StartupInfo.hStdError = self._inheritable[1]
            info.lpAttributeList = ctypes.cast(self._attributes, w.LPVOID)
            self._startup_info = info
            self._executable = shutil.which(command[0]) or command[0]
            self._command_line = ctypes.create_unicode_buffer(subprocess.list2cmdline([self._executable, *command[1:]]))
            self._env_block = ctypes.create_unicode_buffer("\x00".join(f"{k}={v}" for k, v in sorted(env.items(), key=lambda item: item[0].upper())) + "\x00\x00")
        except Exception:
            self._cleanup_prepared_handles()
            if self.job:
                kernel.CloseHandle(self.job)
                self.job = None
            raise

    def _cleanup_prepared_handles(self):
        if getattr(self, "_attributes", None) is not None:
            kernel.DeleteProcThreadAttributeList(self._attributes)
            self._attributes = None
        for handle in getattr(self, "_inheritable", []):
            kernel.CloseHandle(handle)
        self._inheritable = []

    def resume(self) -> None:
        with self._handle_lock:
            return self._resume()

    def _resume(self) -> None:
        if self.closed:
            raise RuntimeError("The process has already been closed")
        if not self.suspended:
            return
        if os.name == "nt":
            if kernel.ResumeThread(self.thread) == 0xFFFFFFFF:
                raise OSError(ctypes.get_last_error(), "ResumeThread failed")
            kernel.CloseHandle(self.thread)
            self.thread = None
        else:
            os.kill(self.pid, signal.SIGCONT)
        self.suspended = False

    def poll(self) -> int | None:
        with self._handle_lock:
            if self.closed:
                return self.exit_code
            return self._poll()

    def _poll(self) -> int | None:
        if os.name == "nt":
            code = w.DWORD()
            if not self.handle or not kernel.GetExitCodeProcess(self.handle, ctypes.byref(code)):
                return None
            self.exit_code = None if code.value == 259 else int(code.value)
            return self.exit_code
        return self.proc.poll()

    def tree_alive(self) -> bool:
        with self._handle_lock:
            if self.closed:
                return False
            return self._tree_alive()

    def _tree_alive(self) -> bool:
        if os.name == "nt":
            if not self.job:
                return False
            accounting = BASIC_ACCOUNTING()
            _check(kernel.QueryInformationJobObject(self.job, 1, ctypes.byref(accounting), ctypes.sizeof(accounting), None), "Query Job accounting")
            return accounting.ActiveProcesses > 0
        try:
            os.killpg(self.pid, 0)
            return True
        except ProcessLookupError:
            return False

    def terminate(self) -> bool:
        with self._handle_lock:
            if self.closed:
                return True
            return self._terminate()

    def _terminate(self) -> bool:
        if os.name == "nt":
            if self.job:
                kernel.TerminateJobObject(self.job, 1)
            if self.handle:
                kernel.WaitForSingleObject(self.handle, 2000)
            try:
                until = time.monotonic() + 2
                while self.tree_alive() and time.monotonic() < until:
                    time.sleep(0.01)
                return not self.tree_alive()
            except OSError:
                return False
        try:
            os.killpg(self.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        if self.proc:
            try:
                self.proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                return False
        return not identity_alive(self.pid, self.create_time)

    def close(self) -> None:
        lock = getattr(self, "_handle_lock", None)
        if lock is None:
            return
        with lock:
            if self.closed:
                return
            self.closed = True
            if os.name == "nt":
                self._cleanup_prepared_handles()
                for handle in (getattr(self, "job", None), getattr(self, "thread", None), getattr(self, "handle", None)):
                    if handle:
                        kernel.CloseHandle(handle)
                self.job = self.thread = self.handle = None
            else:
                if getattr(self, "_log", None):
                    self._log.close()

    def __del__(self):
        self.close()


def detect_resources(workspace: str | Path = ".") -> dict:
    import psutil
    memory = psutil.virtual_memory()
    disk = shutil.disk_usage(workspace)
    gpus = []
    try:
        result = subprocess.run(["nvidia-smi", "--query-gpu=uuid,name,memory.total,memory.free", "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=3, creationflags=0x08000000 if os.name == "nt" else 0)
        if result.returncode == 0:
            for line in result.stdout.splitlines():
                uuid, name, total, free = [value.strip() for value in line.split(",", 3)]
                gpus.append({"uuid": uuid, "name": name, "memory_total_bytes": int(total) * 1048576,
                    "memory_free_bytes": int(free) * 1048576})
    except (OSError, subprocess.TimeoutExpired, ValueError):
        pass
    if gpus:
        occupancy = {gpu["uuid"]: [] for gpu in gpus}
        known = False
        try:
            result = subprocess.run(["nvidia-smi", "--query-compute-apps=gpu_uuid,pid", "--format=csv,noheader,nounits"],
                capture_output=True, text=True, timeout=3, creationflags=0x08000000 if os.name == "nt" else 0)
            known = result.returncode == 0
            if known:
                for line in result.stdout.splitlines():
                    gpu_uuid, pid = [value.strip() for value in line.split(",", 1)]
                    if gpu_uuid in occupancy:
                        occupancy[gpu_uuid].append(int(pid))
        except (OSError, subprocess.TimeoutExpired, ValueError):
            pass
        for gpu in gpus:
            gpu.update(process_ids=occupancy[gpu["uuid"]], occupancy_known=known)
    return {"cpu_cores": psutil.cpu_count(logical=True) or 1, "physical_cpu_cores": psutil.cpu_count(logical=False),
        "memory_total_bytes": memory.total, "memory_available_bytes": memory.available,
        "disk_total_bytes": disk.total, "disk_free_bytes": disk.free, "gpus": gpus,
        "gpu_message": "无可用 GPU" if not gpus else f"{len(gpus)} 块 GPU",
        "platform": os.name, "process_backend": "windows_create_in_job" if os.name == "nt" else "posix_process_group"}
