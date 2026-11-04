"""Receiver-specific handoff requirements used by discussions and finalization."""
from __future__ import annotations

from packages.contracts.research import ExperimentSpec, allowed_next_steps


def discussion_handoff_requirements(role):
    requirements = {step: ["content_markdown（非空正文）"] for step in allowed_next_steps(role)}
    if role == "critic":
        requirements["implementation"] += ["experiment_spec.question", "experiment_spec.fixed（可为 {}）", "experiment_spec.metrics（至少一项）"]
    if role == "boss":
        requirements["dispatch"] += ["operations（操作及目标；首次分派/换方向含 objective）"]
        requirements["finish_lines"] += ["targets（研究线）"]
    return requirements


def validate_discussion_handoff(role, output, context, next_step=None):
    if not isinstance(output, dict) or not isinstance(output.get("content_markdown"), str) or not output["content_markdown"].strip():
        raise ValueError("结束投票必须绑定包含非空 content_markdown 的产物")
    if next_step is not None and next_step not in allowed_next_steps(role):
        raise ValueError("推进方向不在当前角色的目录中")
    # Before routing, only common structure is required. In particular, the
    # presence of an unfinished plan must not prevent a return to Exploration.
    if role == "critic" and next_step == "implementation":
        raw = output.get("experiment_spec", output)
        if not isinstance(raw, dict):
            raise ValueError("转 Implementation 必须提供 experiment_spec 对象")
        missing = [key for key in ("question", "fixed", "metrics") if key not in raw]
        if missing:
            raise ValueError("转 Implementation 缺少 " + "、".join(missing))
        if not isinstance(raw["question"], str) or not raw["question"].strip():
            raise ValueError("experiment_spec.question 必须是非空研究问题")
        if not isinstance(raw["fixed"], dict):
            raise ValueError("experiment_spec.fixed 必须明确给出配置对象，可为 {}")
        if not isinstance(raw["metrics"], list) or not raw["metrics"] or any(not isinstance(item, str) or not item.strip() for item in raw["metrics"]):
            raise ValueError("experiment_spec.metrics 至少给出一个非空评价指标")
        ExperimentSpec.model_validate({**raw, "discussion_protocol": 2,
            "spec_id": context.get("spec_id", "pending-spec"), "version": context.get("spec_version", 1),
            "deadline_at": context["deadline_at"]})
    if role == "boss" and next_step is not None:
        from backend.graphs.boss_operations import normalize_operations
        normalize_operations(output, next_step, context["lines"])
    return output
