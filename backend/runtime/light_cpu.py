"""Registered numeric CPU analysis over immutable evidence.

This is a deliberately restricted Python language, not a general Python shell.
The Windows Job/resource ledger supplies containment; AST validation narrows tool
capabilities. It is not presented as a hostile-code security boundary.
"""
from __future__ import annotations

import ast
import csv
import hashlib
import importlib
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
MODULES={
 "math": {"sqrt","log","log2","log10","exp","pow","sin","cos","tan","floor","ceil","fabs","fsum","isfinite","isnan","isclose","factorial","comb","prod","dist","hypot","pi","e","inf","nan"},
 "statistics": {"mean","fmean","geometric_mean","harmonic_mean","median","median_low","median_high","mode","multimode","pstdev","pvariance","stdev","variance","quantiles","correlation","covariance","linear_regression"},
 "json": {"loads","dumps"},
 "csv": {"reader","DictReader"},
 "collections": {"Counter","defaultdict","deque"},
 "itertools": {"chain","combinations","combinations_with_replacement","permutations","product","groupby","accumulate","islice","zip_longest","repeat","count","takewhile","dropwhile","filterfalse"},
 "random": {"Random","seed","random","uniform","randint","randrange","choice","choices","sample","shuffle","gauss","normalvariate"},
}
BUILTINS={"abs","all","any","bool","dict","enumerate","filter","float","int","len","list","map","max","min","pow","print","range","reversed","round","set","sorted","str","sum","tuple","zip","Exception","ValueError","TypeError","ZeroDivisionError"}
FORBIDDEN=(ast.ClassDef,ast.AsyncFunctionDef,ast.Await,ast.With,ast.AsyncWith,ast.Global,ast.Nonlocal,ast.Delete)


def validate_numeric_code(code: str) -> tuple[ast.Module,dict[str,str],dict[str,tuple[str,str]]]:
    tree=ast.parse(code,"<light-cpu>","exec")
    aliases:dict[str,str]={}
    members:dict[str,tuple[str,str]]={}
    for node in ast.walk(tree):
        if isinstance(node,FORBIDDEN):
            raise ValueError("轻量 CPU 工具只允许数值分析，禁止该 Python 语法")
        if isinstance(node,ast.Name) and node.id.startswith("_"):
            raise ValueError("禁止访问内部名称")
        if isinstance(node,ast.Import):
            for imported in node.names:
                if imported.name not in MODULES or (imported.asname or imported.name).startswith("_"):
                    raise ValueError("轻量 CPU 禁止导入文件、进程、网络、GPU 或训练库")
                aliases[imported.asname or imported.name]=imported.name
        if isinstance(node,ast.ImportFrom):
            if node.level or node.module not in MODULES:
                raise ValueError("禁止该模块")
            for imported in node.names:
                if imported.name not in MODULES[node.module] or (imported.asname or imported.name).startswith("_"):
                    raise ValueError("禁止该模块成员")
                members[imported.asname or imported.name]=(node.module,imported.name)
    functions={node.name for node in ast.walk(tree) if isinstance(node,ast.FunctionDef)}
    for node in ast.walk(tree):
        if isinstance(node,ast.Attribute):
            if not isinstance(node.ctx,ast.Load) or not isinstance(node.value,ast.Name) or node.value.id not in aliases or node.attr not in MODULES[aliases[node.value.id]]:
                raise ValueError("只允许白名单数值模块成员；禁止对象属性遍历")
        if isinstance(node,ast.Call) and isinstance(node.func,ast.Name):
            if node.func.id not in BUILTINS|members.keys()|functions:
                raise ValueError("禁止调用未经授权的函数")
        if isinstance(node,ast.Call) and not isinstance(node.func,(ast.Name,ast.Attribute)):
            raise ValueError("禁止动态函数调用")
    # Imports are provided by trusted setup, without granting user code __import__.
    class RemoveImports(ast.NodeTransformer):
        def visit_Import(self,node):return ast.Pass()
        def visit_ImportFrom(self,node):return ast.Pass()
    tree=RemoveImports().visit(tree)
    ast.fix_missing_locations(tree)
    return tree,aliases,members


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
        tree,aliases,members=validate_numeric_code(code)
        normalized=ast.unparse(tree)
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
                return old
            job={"execution_id":execution_id,"research_id":rid,"agent_id":agent,"role":role,"content_hash":content_hash,
                "state":"starting","light_cpu":True,"cpu_cores":1,"memory_bytes":268435456,"gpu_uuids":[],
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
                text=copied.read_text(encoding="utf-8")
                content=scientific_loads(text) if path.suffix.lower()==".json" else list(csv.DictReader(text.splitlines(),delimiter="\t" if path.suffix.lower()==".tsv" else ",")) if path.suffix.lower() in (".csv",".tsv") else text
                evidence[eid]={"name":metadata["name"],"sha256":actual,"data":content}
            context={"code":normalized,"aliases":aliases,"members":members,"evidence":evidence,"builtins":sorted(BUILTINS)}
            (directory/"inputs.json").write_text(json.dumps(context,ensure_ascii=False),encoding="utf-8")
            wrapper="""import json,builtins,importlib,sys
from pathlib import Path
context=json.loads(Path(sys.argv[1]).read_text(encoding='utf-8'))
scope={'__builtins__':{name:getattr(builtins,name) for name in context['builtins']},'evidence':context['evidence'],'result':None}
for alias,module in context['aliases'].items():scope[alias]=importlib.import_module(module)
for alias,pair in context['members'].items():scope[alias]=getattr(importlib.import_module(pair[0]),pair[1])
exec(compile(context['code'],'<light-cpu>','exec'),scope,scope)
Path(sys.argv[2]).write_text(json.dumps({'result':scope.get('result')},ensure_ascii=False),encoding='utf-8')
"""
            script=directory/"run_numeric.py"
            script.write_text(wrapper,encoding="utf-8")
            job.update(log_path=str(directory/"stdout.log"),result_path=str(directory/"result.json"),evidence_ids=ids)
            self.control.register_process(rid,execution_id,kind="light_cpu",control_epoch=job["control_epoch"],control_generation=generation,supervisor_id=self.instance_id)
            process=ManagedProcess([sys.executable,"-I",str(script),str(directory/"inputs.json"),job["result_path"]],directory,job["log_path"],sanitized_environment(),1,job["memory_bytes"],defer_creation=True)
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
                job["result"]=scientific_loads(Path(job["result_path"]).read_text(encoding="utf-8"))["result"]
                job["result_numeric_status"]=numeric_status(job["result"])
        except Exception as error:
            job.update(state="cancelled" if isinstance(error,ControlUnavailable) else "execution_failed",error=str(error))
            if not admitted:raise
        finally:
            exited=process.terminate() if process else True
            if admitted:
                job.update(ended_at=iso_now(),process_tree_exited=exited)
                if exited:
                    try:
                        self.resources.update_light(job,terminal_proof=True)
                        self.control.complete_execution(execution_id)
                    except Exception:
                        job["reconciliation_pending"]=True
                else:job.update(state="stopping",reconciliation_pending=True)
            if process:
                process.close()
            self.processes.pop(execution_id,None)
        if job.get("log_path") and Path(job["log_path"]).exists():
            with Path(job["log_path"]).open("rb") as stream:
                stream.seek(max(0,Path(job["log_path"]).stat().st_size-65536))
                job["logs"]=stream.read().decode("utf-8",errors="replace")
            job["log_artifact"]=store.register_artifact(rid,job["log_path"],"text/plain",kind="light_cpu_log")
        if job.get("state")=="completed":
            job["artifact"]=store.register_artifact(rid,job["result_path"],"application/json",kind="light_cpu_result",source_evidence_ids=ids)
        return job

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
