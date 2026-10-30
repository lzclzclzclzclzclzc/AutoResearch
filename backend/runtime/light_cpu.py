"""Registered numeric CPU analysis over immutable evidence.

Ordinary Python runs in a supervised child with resource and stop controls.
Workspace/data access rules are separate from code capabilities. This process
supervision is not a filesystem security boundary.
"""
from __future__ import annotations

import ast
import hashlib
import json
import os
import shutil
import stat
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from .control import ControlUnavailable,_canonical,iso_now,timestamp
from .processes import ManagedProcess,identity_alive,terminate_identity,sanitized_environment
from .scientific_json import loads as scientific_loads,numeric_status

ROLES={"exploration","critic","implementation","verify"}
# Include the import footprint of NumPy/pandas in the supervised process tree.
LIGHT_CPU_MEMORY_BYTES = 512 * 1024 * 1024


def validate_numeric_code(code: str) -> tuple[ast.Module, dict, dict]:
    """Validate Python syntax without rewriting code or restricting capabilities.

    The return shape is retained for existing callers; imports execute normally.
    Workspace rules are specified separately from Python language capabilities.
    """
    tree = ast.parse(code, "<light-cpu>", "exec")
    compile(tree, "<light-cpu>", "exec")
    return tree, {}, {}


class LightCpuService:
    def __init__(self,data_dir:Path,control:Any,resources:Any,instance_id:str,generation:str):
        self.data_dir=Path(data_dir).resolve()
        self.directory=self.data_dir/"light_cpu"
        self.directory.mkdir(parents=True,exist_ok=True)
        self.control,self.resources=control,resources
        self.instance_id,self.generation=instance_id,generation
        self.jobs:dict[str,dict]={}
        self.processes:dict[str,ManagedProcess]={}
        self.stopped:set[str]=set()
        self.cancelled_parents:set[str]=set()
        self.closed=threading.Event()
        self.lock=threading.RLock()
        self.creation_lock=threading.RLock()
        for job in resources.light_jobs():
            if job["state"] in ("completed","execution_failed","cancelled","interrupted"):
                self.jobs[job["execution_id"]]=job
                continue
            # Parent Runner has acquired the exclusive old-instance lock; old Job owner is gone.
            exited=not job.get("pid") or not identity_alive(job["pid"],job.get("process_create_time"))
            if not exited:
                exited=terminate_identity(job["pid"],job.get("process_create_time"))
            if exited:
                job.update(state="interrupted",ended_at=iso_now(),process_tree_exited=True)
                resources.update_light(job,terminal_proof=True)
            self.jobs[job["execution_id"]]=job
        self.maintenance=threading.Thread(target=self._reconcile_pending,name="light-cpu-reconcile",daemon=True)
        self.maintenance.start()

    @staticmethod
    def _parent_alive(context:dict)->bool:
        pid,created=context.get("parent_pid"),context.get("parent_create_time")
        if pid is None and created is None:
            return True
        return bool(pid and created is not None and identity_alive(pid,created))

    def execute(self,payload:dict)->dict:
        from backend.persistence.store import Store
        rid,agent,role=payload["research_id"],payload["agent_id"],payload["role"]
        if role not in ROLES:
            raise ValueError("该角色无轻量 CPU 执行权限")
        ids=list(payload.get("evidence_ids",[]))
        if role=="verify" and not ids:
            raise ValueError("Verify 只能分析已有证据，至少提供一个 evidence_id")
        code=str(payload.get("code",""))
        validate_numeric_code(code)
        generation=int(payload.get("control_generation",1))
        parent=payload.get("parent_execution_id")
        if (payload.get("parent_pid") is not None or payload.get("parent_create_time") is not None) and not parent:
            raise ValueError("父进程监督身份必须绑定父执行 ID")
        if rid in self.stopped or parent in self.cancelled_parents or self.closed.is_set() or not self._parent_alive(payload) or not self.control.allows_progress(rid,generation):
            raise ControlUnavailable("研究停止、到期或控制服务不可用")
        store=Store(self.data_dir)
        identity=store.get("agent",f"{rid}:{agent}")
        if not identity or identity.get("role") not in (role,f"{role}_member"):
            raise ValueError("Agent 身份与研究角色不匹配")
        project=store.project(rid)
        control_state=self.control.status(rid)
        execution_id=str(payload.get("execution_id") or f"light-{uuid.uuid4().hex}")
        content_hash=hashlib.sha256(_canonical({"research_id":rid,"agent_id":agent,"role":role,"code":code,"evidence_ids":ids})).hexdigest()
        with self.lock:
            old=self.jobs.get(execution_id)
            if old:
                if old["content_hash"]!=content_hash:raise ValueError("执行 ID 不能用于不同内容")
                if old.get("state") == "completed":
                    self._artifacts(store, old, payload.get("line_id"))
                    self.resources.update_light(old, terminal_proof=True)
                return old
            job={"execution_id":execution_id,"research_id":rid,"agent_id":agent,"role":role,"content_hash":content_hash,
                "line_id":payload.get("line_id"),"state":"starting","light_cpu":True,"cpu_cores":1,"memory_bytes":LIGHT_CPU_MEMORY_BYTES,"gpu_uuids":[],
                "control_generation":generation,"control_epoch":control_state["control_epoch"],"deadline_at":timestamp(project["deadline_at"]),
                "parent_execution_id":parent,
                "parent_pid":payload.get("parent_pid"),"parent_create_time":payload.get("parent_create_time"),
                "runner_instance_id":self.instance_id,"runner_generation":self.generation,"started_at":iso_now(),"pid":None}
            self.jobs[execution_id]=job
        admitted=False
        process=None
        try:
            saved=self.resources.reserve_light(job)
            if saved is not job:
                with self.lock:self.jobs[execution_id]=saved
                return saved
            admitted=True
            directory=self.directory/hashlib.sha256(execution_id.encode()).hexdigest()[:24]
            directory.mkdir(exist_ok=True)
            inputs=directory/"evidence"
            inputs.mkdir(exist_ok=True)
            # Per-call context; evidence loading happens before any process is created.
            evidence={}
            for eid in ids:
                metadata,path=store.artifact(eid)
                if metadata.get("research_id")!=rid:raise ValueError("证据不属于当前研究")
                with path.open("rb") as stream:actual=hashlib.file_digest(stream,"sha256").hexdigest()
                if actual!=metadata["sha256"]:raise ValueError("证据完整性检查失败")
                copied=inputs/(hashlib.sha256(eid.encode()).hexdigest()[:24]+path.suffix)
                shutil.copyfile(path,copied);copied.chmod(stat.S_IREAD)
                evidence[eid]={"name":metadata["name"],"sha256":actual,"path":str(copied)}
            data_dir = str(Path(project["workspace_path"]).expanduser().resolve()) if project.get("workspace_path") else None
            context={"evidence":evidence,"workspace_dir":str(directory),"data_dir":data_dir}
            job.update(workspace_dir=str(directory),data_dir=data_dir)
            (directory/"inputs.json").write_text(json.dumps(context,ensure_ascii=False),encoding="utf-8")
            (directory/"analysis.py").write_text(code,encoding="utf-8")
            script=directory/"run_numeric.py"
            shutil.copyfile(Path(__file__).with_name("light_cpu_child.py"), script)
            job.update(log_path=str(directory/"stdout.log"),result_path=str(directory/"result.json"),evidence_ids=ids)
            self.control.register_process(rid,execution_id,kind="light_cpu",control_epoch=job["control_epoch"],control_generation=generation,supervisor_id=self.instance_id)
            # Keep BLAS from allocating a machine-wide worker pool for a 1-core job.
            environment = sanitized_environment({"OPENBLAS_NUM_THREADS": "1", "OMP_NUM_THREADS": "1", "MKL_NUM_THREADS": "1"})
            environment["TMP"] = environment["TEMP"] = str(directory)
            process=ManagedProcess([sys.executable,str(script),str(directory/"inputs.json"),job["result_path"]],directory,job["log_path"],environment,1,job["memory_bytes"],defer_creation=True)
            if not self._parent_alive(job):raise ControlUnavailable("父 Agent 执行已退出，禁止创建计算")
            if not self.control.allows_progress(rid,generation,job["control_epoch"]):raise ControlUnavailable("控制门禁已关闭")
            with self.creation_lock:
                if rid in self.stopped or parent in self.cancelled_parents or self.closed.is_set() or time.time()>=job["deadline_at"]:raise ControlUnavailable("本地启动门禁已关闭")
                process.create_suspended()
                self.processes[execution_id]=process
                job["pid"]=process.pid
            process.capture_identity();job["process_create_time"]=process.create_time
            self.resources.update_light(job)
            self.control.register_process(rid,execution_id,pid=process.pid,create_time=process.create_time,kind="light_cpu",control_epoch=job["control_epoch"],control_generation=generation,supervisor_id=self.instance_id)
            if not self._parent_alive(job):raise ControlUnavailable("父 Agent 执行已退出，禁止恢复计算")
            if not self.control.allows_progress(rid,generation,job["control_epoch"]):raise ControlUnavailable("恢复执行前控制门禁已关闭")
            with self.creation_lock:
                if rid in self.stopped or parent in self.cancelled_parents or self.closed.is_set() or time.time()>=job["deadline_at"]:raise ControlUnavailable("本地启动门禁已关闭")
                process.resume();job["state"]="running"
            self.resources.update_light(job)
            while process.poll() is None:
                if self.closed.wait(.1) or rid in self.stopped or parent in self.cancelled_parents or not self._parent_alive(job) or time.time()>=job["deadline_at"]:
                    raise ControlUnavailable("研究停止或总时间到期")
                if not self.control.allows_progress(rid,generation,job["control_epoch"]):
                    raise ControlUnavailable("控制服务失联或执行许可失效")
            if rid in self.stopped or parent in self.cancelled_parents or not self._parent_alive(job) or not self.control.allows_progress(rid,generation,job["control_epoch"]):
                raise ControlUnavailable("结果返回前研究已停止")
            job.update(state="completed" if process.poll()==0 else "execution_failed",exit_code=process.poll())
            if job["state"]=="completed":
                if Path(job["result_path"]).exists():
                    job["result"]=scientific_loads(Path(job["result_path"]).read_text(encoding="utf-8"))["result"]
                    job["result_numeric_status"]=numeric_status(job["result"])
                preview = directory/"result.txt"
                if preview.exists():
                    job["result_text"]=preview.read_text(encoding="utf-8")
        except Exception as error:
            job.update(state="cancelled" if isinstance(error,ControlUnavailable) else "execution_failed",error=str(error))
            if not admitted:
                with self.lock:
                    self.jobs.pop(execution_id, None)
                raise
        finally:
            exited=process.terminate() if process else True
            if admitted:
                job.update(ended_at=iso_now(),process_tree_exited=exited)
                if exited:
                    try:
                        self._artifacts(store, job)
                        self.resources.update_light(job,terminal_proof=True)
                        self.control.complete_execution(execution_id)
                    except Exception:
                        job["reconciliation_pending"]=True
                else:job.update(state="stopping",reconciliation_pending=True)
            if process:
                process.close()
            self.processes.pop(execution_id,None)
        return job

    def _artifacts(self, store, job, line_id=None):
        line_id = job.get("line_id") or line_id or job["agent_id"].split("/")[0]
        job["line_id"] = line_id
        if job.get("log_path") and Path(job["log_path"]).exists():
            with Path(job["log_path"]).open("rb") as stream:
                stream.seek(max(0,Path(job["log_path"]).stat().st_size-65536))
                job["logs"]=stream.read().decode("utf-8",errors="replace")
            job["log_artifact"]=store.register_artifact(job["research_id"],job["log_path"],"text/plain",kind="light_cpu_log",line_id=line_id)
        if job.get("state")=="completed":
            directory = Path(job["workspace_dir"])
            internal = {"analysis.py", "run_numeric.py", "inputs.json", "stdout.log"}
            outputs = []
            for path in sorted(directory.rglob("*")):
                if (not path.is_file() or not path.resolve().is_relative_to(directory.resolve())
                        or path.relative_to(directory).parts[0] in {"evidence", "__pycache__"}
                        or path.relative_to(directory).as_posix() in internal):
                    continue
                import mimetypes
                outputs.append(store.register_artifact(job["research_id"], path,
                    mimetypes.guess_type(path.name)[0] or "application/octet-stream",
                    kind="light_cpu_result",line_id=line_id,source_evidence_ids=job.get("evidence_ids",[])))
            job["output_artifacts"] = outputs
            if outputs:
                job["artifact"] = next((a for a in outputs if a["name"] == "result.json"), outputs[0])

    def expire_due(self):
        with self.lock:
            active=[job for job in self.jobs.values() if job.get("state") in ("starting","running","stopping","reconciling")]
            expired={job["research_id"] for job in active if job["deadline_at"]<=time.time()}
        for rid in expired:
            if rid not in self.stopped:self.stop_project(rid,"time_limit_reached")
        for job in active:
            if not self._parent_alive(job) and job.get("parent_execution_id") not in self.cancelled_parents:
                self.cancel_parent(job["parent_execution_id"])

    def _reconcile_pending(self):
        while not self.closed.wait(.5):
            with self.lock:
                pending=[job for job in self.jobs.values() if job.get("reconciliation_pending") and job.get("process_tree_exited")]
            for job in pending:
                try:
                    from backend.persistence.store import Store
                    self._artifacts(Store(self.data_dir), job)
                    self.resources.update_light(job,terminal_proof=True)
                    self.control.complete_execution(job["execution_id"])
                    job["reconciliation_pending"]=False
                except Exception:pass

    def cancel_parent(self,parent_execution_id:str)->dict:
        self.cancelled_parents.add(parent_execution_id)
        with self.creation_lock:
            targets=[(eid,p) for eid,p in self.processes.items() if self.jobs[eid].get("parent_execution_id")==parent_execution_id]
        for eid,process in targets:
            threading.Thread(target=process.terminate,daemon=True).start()
        return {"parent_execution_id":parent_execution_id,"state":"stopping","execution_ids":[eid for eid,p in targets]}

    def stop_project(self,research_id:str,reason:str="user_requested"):
        self.stopped.add(research_id)
        with self.creation_lock:
            processes=[p for eid,p in self.processes.items() if self.jobs[eid]["research_id"]==research_id]
        for process in processes:
            threading.Thread(target=process.terminate,daemon=True).start()

    def close(self):
        self.closed.set()
        for process in list(self.processes.values()):
            process.terminate()
            process.close()
