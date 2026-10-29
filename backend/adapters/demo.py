"""Explicit fixture mode. No fixture text is labelled as real model output."""
from __future__ import annotations

import asyncio

from packages.contracts.research import digest


DEMO_CODE = '''import json, math, random, time
random.seed(42)
points = [(i/100, 2*(i/100)+1+random.gauss(0,0.02)) for i in range(200)]
w, b = 0.0, 0.0
baseline = sum(y*y for x,y in points)/len(points)
for epoch in range(120):
    dw = sum(2*x*(w*x+b-y) for x,y in points)/len(points)
    db = sum(2*(w*x+b-y) for x,y in points)/len(points)
    w -= 0.08*dw; b -= 0.08*db
    loss = sum((w*x+b-y)**2 for x,y in points)/len(points)
    if epoch % 10 == 0:
        print(json.dumps({"epoch":epoch,"mse":loss}), flush=True)
    time.sleep(0.025)
result={"baseline_mse":baseline,"final_mse":loss,"weight":w,"bias":b,"seed":42,"samples":200,"epochs":120}
with open("metrics.json","w",encoding="utf-8") as f: json.dump(result,f,indent=2)
print(json.dumps({"result":result}),flush=True)
'''


class DemoRuntimeAdapter:
    def __init__(self, store, control, *args, **kwargs):
        self.store, self.control = store, control

    def availability(self):
        return {"available": True, "mode": "demo", "real_model": False}

    async def execute_turn(self, **arguments):
        from backend.persistence.store import now
        research_id, agent_id = arguments["research_id"], arguments["agent_id"]
        if self.store.project(research_id)["mode"] != "demo":
            raise RuntimeError("Fixture adapter requires an explicitly selected demo project")
        key = "demo-" + digest([research_id, agent_id, arguments["task_id"], arguments["turn_id"]])
        existing = self.store.get("execution", key)
        if existing and existing.get("status") == "completed":
            return existing["output"]
        self.store.put("execution", key, {"status": "running", "mode": "demo", "real_model": False,
            "execution_attempt_id": key, "agent_id": agent_id, "role": arguments["role"], "task_id": arguments["task_id"],
            "turn_id": arguments["turn_id"], "attempt": 1, "created_at": now(), "started_at": now()}, research_id)
        self.store.emit(research_id, "agent.started", {"mode": "demo", "real_model": False, "execution_attempt_id": key},
            agent_id=agent_id, execution_id=key, task_id=arguments["task_id"])
        try:
            output = await self._produce_turn(**arguments)
            self.store.complete_model_turn(key, output, payload={"mode": "demo", "real_model": False, "execution_attempt_id": key})
            return output
        except BaseException as error:
            from backend.adapters.dsh import ResearchStopped
            stopped = isinstance(error, (ResearchStopped, asyncio.CancelledError))
            self.store.patch("execution", key, status="interrupted" if stopped else "failed", completed_at=now())
            self.store.emit(research_id, "agent.cancelled" if stopped else "agent.failed", {"mode": "demo", "real_model": False,
                "execution_attempt_id": key, "code": type(error).__name__}, agent_id=agent_id, execution_id=key, task_id=arguments["task_id"])
            raise

    async def _produce_turn(self, *, research_id, agent_id, role, task_id, turn_id,
                           prompt, schema, direction_epoch=1, model=None, context=None, runtime_context=None):
        if self.store.project(research_id)["mode"] != "demo":
            raise RuntimeError("Fixture adapter requires an explicitly selected demo project")
        if not await asyncio.to_thread(self.control.allows_progress, research_id):
            from backend.adapters.dsh import ResearchStopped
            raise ResearchStopped("研究已停止")
        await asyncio.sleep(0.05)
        context = context or {}
        content = f"**演示 · 固定脚本，非模型生成**\n\n{agent_id}：{context.get('instruction', '执行演示任务')}"
        name = schema.__name__
        def boss_decision():
            initial = context.get("initial", False)
            return {"content_markdown": content, "action": "assign" if initial else "finish_line", "assignments": [{"line_id": line["line_id"], "objective": "演示 CPU 线性回归完整工作流", "priority": 0} for line in context.get("lines", [])] if initial else [], "memory_updates": [] if initial else [{"node_id": "research/demo/"+context.get("line_id", "line"), "parent_id": "research/demo", "title": "CPU 工作流演示", "content_markdown": "真实 CPU 演示流程已执行；非用户题目的科研结论", "status": "passed", "source_ids": [context.get("submission_id", "")], "conditions": "仅合成数据演示"}], "request_report": not initial, "rationale": "完成演示生命周期"}
        if name == "GodDecision":
            request, snapshot = context["arbitration_request"], context["snapshot"]
            value = {"arbitration_id": request["arbitration_id"], "snapshot_hash": request["snapshot_hash"],
                "content_markdown": content, "rationale": "演示固定脚本：只处理当前冻结现场，不生成成员投票或实验事实。",
                "evidence_refs": [], "limitations": ["固定脚本演示，非真实模型仲裁"], "unresolved_issue_ids": []}
            actions = request["allowed_actions"]
            if "select_candidate" in actions and snapshot.get("candidates") and not snapshot.get("blockers"):
                value.update(action="select_candidate", selected_candidate_id=sorted(snapshot["candidates"])[0], minority_views=[])
            elif "repair_output" in actions and snapshot.get("winner") and snapshot.get("candidates"):
                winner = snapshot["winner"]
                value.update(action="repair_output", target_schema_hash=request["target_schema_hash"], source_fields=["/winner", "/candidates"],
                    output={"content_markdown": content, "selected_candidate_id": winner,
                        "decision": snapshot["candidates"][winner] if snapshot.get("role") == "boss" else {},
                        "minority_views": [], "issues": snapshot.get("issues", [])})
            else:
                missing = [i.get("issue_id", "未知阻断项") + ": " + i.get("description", "") for i in snapshot.get("blockers", [])]
                value.update(action="needs_external_input", missing_items=missing or ["演示脚本不包含此原合同的真实修复结果"],
                    why_unavailable="固定演示不能捏造证据或代替真实模型修复任意任务", operation="切换到真实研究并检查原任务所需资料")
                value["content_markdown"] += "\n缺少条件：" + "；".join(value["missing_items"]) + "\n操作：" + value["operation"]
        elif name == "Willingness":
            value = {"score": int(digest([agent_id, task_id, turn_id])[:8], 16) % 101}
        elif name == "MessageReply":
            value = {"content_markdown": content + "\n已收到实际投递的消息；此回复仅用于通信演示。"}
        elif name == "BossProposal":
            value = {"content_markdown": content, "candidate": boss_decision(), "evidence": []}
        elif name == "Proposal":
            value = {"content_markdown": content + "\n采用合成线性回归 CPU 示例检验完整工作流。", "candidate": {"method": "seeded_cpu_linear_regression"}, "evidence": []}
        elif name in {"Review", "BossReview"}:
            value = {"content_markdown": content + "\n固定 seed、样本数与梯度步数，结果必须由真实进程提供。", "ready_for_vote": True, "new_evidence": [], "issues": []}
        elif name == "Vote":
            candidate = next(iter(context["proposals"]))
            value = {"content_markdown": content, "candidate_id": candidate, "rationale": "演示中所有成员支持同一可复核 CPU 示例"}
        elif name == "Synthesis":
            value = {"content_markdown": content, "selected_candidate_id": context["winner"], "decision": context.get("majority_candidate", {}) if role == "boss" else {}, "minority_views": [], "issues": []}
        elif name == "ExperimentSpec":
            value = {"spec_id": context["spec_id"], "version": context["spec_version"], "question": "演示：CPU 梯度下降拟合合成线性数据", "hypothesis": "在固定数据上 120 步梯度下降可降低均方误差", "fixed": {"seed": 42, "samples": 200, "epochs": 120, "learning_rate": 0.08}, "allowed_adjustments": {"paths": "line workspace only"}, "baseline": "零预测器", "data": {"source": "synthetic", "generator": "y=2*x+1+Gaussian(0,0.02)", "seed": 42}, "metrics": ["mse"], "design": {"comparison": "zero predictor vs fixed gradient descent", "repetitions": 1, "scope": "workflow demonstration only"}, "monitoring": {"signals": ["process_exit", "stdout_progress", "mse"], "window_epochs": 10}, "completion_criteria": "完成 120 个固定训练步并写出 metrics.json", "early_stop_rules": [{"signal": "nonfinite_mse", "action": "stop_and_package"}], "issues": [], "deadline_at": context["deadline_at"]}
        elif name == "Implementation":
            spec = context["spec"]
            value = {"content_markdown": content, "spec_hash": digest(spec), "files": {"experiment.py": DEMO_CODE}, "entrypoint": "experiment.py", "arguments": [], "actual_config": spec["fixed"], "cpu_cores": 1, "memory_mb": 128, "gpu_count": 0, "estimated_seconds": 10, "dependencies": []}
        elif name == "Verification":
            metrics = context["submission"].get("metrics", {})
            ok = context["submission"].get("run", {}).get("state") in {"completed", "succeeded"} and all(metrics.get(k) == v for k, v in {"seed": 42, "samples": 200, "epochs": 120}.items())
            improved = ok and metrics.get("final_mse", float("inf")) < metrics.get("baseline_mse", 0)
            value = {"content_markdown": content + "\n仅根据真实 CPU 运行产物核查演示设置；不构成任何用户题目的科研结论。", "passed": ok, "scientific_observation": "improvement" if improved else "no_improvement" if ok else "execution_failed", "evidence": [{"source_id": context["submission"]["id"], "claim": "CPU 演示运行已完成，实际指标：" + str(metrics) if ok else "执行未正常完成或指标不完整", "status": "observed"}], "limitations": ["固定脚本演示，不含真实多 Agent 推理", "合成数据单次实验，不代表题目的有效科研证据"], "required_changes": []}
        elif name == "BossDecision":
            value = boss_decision()
        elif name == "ResourceAdvice":
            value = {"content_markdown": content, "priority": 0, "reason": "CPU 示例按等待顺序排期，实际容量由资源服务判断"}
        elif name == "MonitorDecision":
            value = {"content_markdown": content, "request_early_stop": False, "rule_id": None, "rationale": "根据真实进程状态继续观测", "evidence_log_hash": context["evidence_log_hash"], "limitations": []}
        elif name == "RunAssessment":
            value = {"content_markdown": content, "action": "package", "rationale": "演示保留真实执行失败并提交 Verify", "proposed_changes": []}
        elif name == "StageReport":
            value = {"title": "CPU 工作流演示报告", "content_markdown": content + "\n\n真实的 CPU 实验、日志和指标见下方冻结证据。讨论为演示脚本，不含真实模型请求。\n\n```json\n" + __import__("json").dumps(context.get("snapshot", {}), ensure_ascii=False, indent=2) + "\n```", "evidence_ids": context.get("evidence_ids", []), "limitations": ["本报告仅验收工作流，不作用户研究题目的科学结论"]}
        else:
            raise RuntimeError(f"Unsupported fixture contract: {name}")
        return schema.model_validate(value).model_dump()

    async def close(self):
        pass
