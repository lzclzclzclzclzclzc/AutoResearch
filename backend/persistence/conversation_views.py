"""Short, factual conversation labels; task instructions are never option titles."""
from __future__ import annotations

import re


ROLE_LABELS = {"exploration": "探索组", "critic": "评审组", "implementation": "实施",
    "verify": "验证", "boss": "Boss", "organizer": "Organizer", "paper": "Paper",
    "god": "God", "watchdog": "Watch Dog"}


def conversation_metadata(agent_id, role, task_id, turn_id, context):
    """Bind identity and purpose once, at creation; evolving activity comes from records."""
    spec = context.get("spec") or context.get("submission", {}).get("spec") or {}
    cycle = context.get("cycle_id") or spec.get("cycle_id")
    revision = context.get("revision") or context.get("spec_version") or spec.get("version")
    match = re.search(r":revision-(\d+)(?:\D|$)", task_id)
    revision = revision or (int(match[1]) if match else None)
    purpose = ("monitor" if turn_id.startswith("monitor-") else
        "assessment" if turn_id.startswith("assess") else
        "initial_assignment" if context.get("initial") else
        "verification" if role == "verify" else
        "implementation" if role == "implementation" else
        "resource" if role == "organizer" else "report" if role == "paper" else
        "finalization" if role == "god" else "diagnosis" if role == "watchdog" else "discussion")
    return {"scope": agent_id.split("/")[0] if agent_id.startswith("line-") else "global",
        "role": role, "cycle_id": cycle, "revision": revision, "task_type": purpose,
        "task_id": task_id, "initial_turn_id": turn_id}


def _number(value, pattern):
    match = re.search(pattern, str(value or ""))
    return int(match[1]) if match else None


def summarize_conversation(item, messages, executions=(), *, state=None):
    metadata = dict(item.get("summary_metadata") or {})
    cid = item["conversation_id"]
    latest = messages[-1] if messages else {}
    related = [x for x in executions if x.get("conversation_id") == cid]
    agent = next((x.get("agent_id") for x in reversed(related) if x.get("agent_id")), None)
    identities = [p.get("id", "") if isinstance(p, dict) else p for p in item.get("participants", [])]
    identities += [m.get("sender", {}).get("id", "") for m in messages]
    role = metadata.get("role") or next((role for role in ROLE_LABELS if
        any(role in identity.split("/") for identity in [agent or "", *identities]) or cid.endswith(":" + role)), None)
    scope = item.get("line_id") or metadata.get("scope") or next((identity.split("/")[0]
        for identity in identities if identity.startswith("line-")), "global")
    line_no = _number(scope, r"line-(\d+)")
    parts = [f"线 {line_no:02d}" if line_no is not None else "全局"]
    revision = metadata.get("revision") or _number(cid, r":revision-(\d+)")
    cycle = metadata.get("cycle_id") or next((m.get("cycle_id") for m in reversed(messages) if m.get("cycle_id")), None)
    if revision is not None and role in {"exploration", "critic"}:
        parts.append(f"方案修订 {revision}")
    elif cycle and role != "verify":
        # Keep opaque system IDs out of the label; only display a recorded ordinal.
        ordinal = _number(cycle, r"(?:cycle[-:]|cycle-)(\d+)(?:\D|$)")
        if ordinal:
            parts.append(f"周期 {ordinal}")
    role_label = ROLE_LABELS.get(role, "会话")
    purpose = metadata.get("task_type")
    turns = [x.get("turn_id", "") for x in related] + [m.get("turn_id", "") for m in messages]
    if purpose == "monitor" or cid.endswith(":monitor"):
        round_no = max([_number(t, r"monitor-(\d+)") or 0 for t in turns] or [0])
        parts += [role_label, f"实验监控第 {round_no} 次" if round_no else "实验监控"]
    elif role == "verify":
        round_no = metadata.get("verify_round")
        parts.append(f"{role_label}第 {round_no} 轮" if round_no else f"{role_label}报告")
    elif purpose == "assessment" or any(t.startswith("assess") for t in turns):
        parts += [role_label, "实验失败评估"]
    elif purpose == "initial_assignment" or (role == "boss" and ":initial" in cid):
        parts.append("Boss 初始分派")
    elif item.get("kind") == "discussion_finalization":
        parts.append("God 讨论定稿")
    elif role in {"exploration", "critic", "boss"} or item.get("kind") == "group_discussion":
        vote = latest.get("vote") or {}
        mode = (state or {}).get("mode")
        activity = ("结束投票" if mode == "end_vote" or vote.get("type") == "end" else
            "普通投票" if mode == "ordinary_vote" or vote.get("type") == "ordinary" else "讨论")
        speech_round = latest.get("discussion_round")
        parts.append(role_label + (f"发言第 {speech_round} 轮" if activity == "讨论" and speech_round else activity))
    else:
        activity = {"implementation": "编写代码", "resource": "资源调度", "report": "阶段报告",
            "diagnosis": "诊断与修复", "finalization": "定稿"}.get(purpose)
        parts.append(role_label + (f" · {activity}" if activity else ""))
    status = item.get("activity_status")
    if not status:
        status = "running" if any(x.get("status") in {"registered", "running"} for x in related) or (
            state and state.get("mode") != "ended") else "completed" if related or messages else "idle"
    labels = {"running": "进行中", "completed": "已结束", "idle": "待开始"}
    parts.append(labels.get(status, status))
    timestamp = item.get("last_activity_at") or latest.get("updated_at") or latest.get("created_at") or item.get("created_at")
    if timestamp:
        from datetime import datetime, timedelta, timezone
        try:
            parts.append(datetime.fromisoformat(timestamp.replace("Z", "+00:00")).astimezone(timezone(timedelta(hours=8))).strftime("%m-%d %H:%M"))
        except ValueError:
            pass
    return {**item, "summary": " · ".join(parts), "summary_metadata": metadata,
        "activity_status": status, "last_message": latest or None}


def disambiguate_summaries(items):
    counts = {}
    for item in items:
        counts[item["summary"]] = counts.get(item["summary"], 0) + 1
    return [{**item, "summary": item["summary"] + (" · " + item["conversation_id"][-8:]
        if counts[item["summary"]] > 1 else "")} for item in items]
