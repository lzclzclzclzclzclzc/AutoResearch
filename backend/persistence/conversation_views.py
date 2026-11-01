"""Short, factual conversation labels; task instructions are never option titles."""
from __future__ import annotations

import re
import json
from datetime import datetime, timedelta, timezone


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
    turns = [x.get("turn_id") or "" for x in related] + [m.get("turn_id") or "" for m in messages]
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
        activity = {"end_vote": "结束投票", "ordinary_vote": "普通投票", "discussion": "讨论", "god": " · God 定稿"}.get(mode)
        if activity is None:
            activity = "结束投票" if vote.get("type") == "end" else "普通投票" if vote.get("type") == "ordinary" else "讨论"
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


def _time(value):
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except (AttributeError, ValueError):
        return 0


def _involves(participant, identity):
    value = participant.get("id", "") if isinstance(participant, dict) else participant
    return value == identity or value.startswith(identity + "/") or (
        isinstance(participant, dict) and participant.get("type") == "group" and identity.startswith(value + "/"))


def conversation_list(store, research_id, *, identity=None, kind=None, line_id=None, at_seq=None):
    """Filter and order only facts visible at this cursor, including participation.

    A conversation's accumulated participants and latest status are live views.
    Replaying those fields leaks future involvement even when its messages are
    correctly hidden, so historical membership starts at its creation event and
    adds only participants in messages already visible at the requested cursor.
    """
    project = store.project(research_id)
    raw_items = store.list("conversation", research_id)
    raw_executions = store.list("execution", research_id) + store.list("agent_execution", research_id)
    with store.connect() as c:
        c.execute("BEGIN")
        maximum = c.execute("SELECT COALESCE(MAX(seq),0) FROM events WHERE research_id=?", (research_id,)).fetchone()[0]
        cutoff_seq = maximum if at_seq is None else at_seq
        last = c.execute("SELECT data FROM events WHERE research_id=? AND seq<=? ORDER BY seq DESC LIMIT 1",
            (research_id, cutoff_seq)).fetchone()
        cutoff = json.loads(last[0])["occurred_at"] if last else project["created_at"]
        # Do not load model response bodies or tool result payloads for sorting.
        events = [dict(row) for row in c.execute("""SELECT seq,
            json_extract(data,'$.type') AS type, json_extract(data,'$.occurred_at') AS occurred_at,
            json_extract(data,'$.conversation_id') AS conversation_id,
            json_extract(data,'$.execution_id') AS execution_id,
            json_extract(data,'$.task_id') AS task_id
            FROM events WHERE research_id=? AND seq<=? ORDER BY seq""", (research_id, cutoff_seq))]
        creations = {row["conversation_id"]: (row["seq"], json.loads(row["data"])["payload"]["conversation"])
            for row in c.execute("""SELECT seq,json_extract(data,'$.conversation_id') AS conversation_id,data
                FROM events WHERE research_id=? AND seq<=? AND json_extract(data,'$.type')='conversation.created'""",
                (research_id, cutoff_seq))}
        memberships = {}
        for row in c.execute("""SELECT data FROM events WHERE research_id=? AND seq<=?
                AND json_extract(data,'$.type')='conversation.participants_added' ORDER BY seq""", (research_id, cutoff_seq)):
            event = json.loads(row[0])
            memberships.setdefault(event["conversation_id"], []).extend(event["payload"]["participants"])
        message_rows = c.execute("""SELECT cursor,data FROM messages WHERE research_id=?
            AND CAST(COALESCE(json_extract(data,'$.event_seq'),0) AS INTEGER)<=? ORDER BY cursor""",
            (research_id, cutoff_seq)).fetchall()
        messages = store._messages_at(c, message_rows, cutoff_seq) if at_seq is not None else [json.loads(row["data"]) for row in message_rows]
    by_conversation = {}
    for message in messages:
        by_conversation.setdefault(message["conversation_id"], []).append(message)
    events_by_conversation, events_by_execution = {}, {}
    for event in events:
        if event["conversation_id"]:
            events_by_conversation.setdefault(event["conversation_id"], []).append(event)
        if event["execution_id"]:
            key = event["execution_id"]
            events_by_execution.setdefault(key, []).append(event)
            if key.rsplit("-", 1)[-1].isdigit():
                events_by_execution.setdefault(key.rsplit("-", 1)[0], []).append(event)
    executions = []
    for raw in raw_executions:
        if at_seq is None:
            execution = {key: raw.get(key) for key in ("id", "conversation_id", "execution_id", "execution_attempt_id",
                "agent_id", "task_id", "turn_id", "status", "started_at", "created_at")}
        else:
            ids = {raw.get(key) for key in ("id", "execution_id", "execution_attempt_id") if raw.get(key)}
            refs = sorted({event["seq"]: event for key in ids for event in events_by_execution.get(key, [])
                if event["type"].startswith("agent.")}.values(), key=lambda event: event["seq"])
            starts = [event for event in refs if event["type"] == "agent.started"]
            if not starts:
                continue
            started = starts[-1]
            ended = next((event for event in reversed(refs) if event["seq"] > started["seq"] and
                event["type"] in {"agent.completed", "agent.failed", "agent.cancelled"}), None)
            execution = {key: raw.get(key) for key in ("id", "conversation_id", "agent_id", "task_id", "turn_id")}
            execution.update(status=ended["type"].split(".")[1] if ended else "running",
                execution_attempt_id=started["execution_id"], started_at=started["occurred_at"])
        executions.append(execution)
    tasks = store.list("task", research_id) if at_seq is None else []
    worker = store.get("system", "worker") if at_seq is None else None
    worker_live = worker and worker.get("status") == "running" and _time(worker.get("heartbeat_at")) > datetime.now(timezone.utc).timestamp() - 30
    views = []
    for raw in raw_items:
        cid = raw["conversation_id"]
        visible_messages = by_conversation.get(cid, [])
        creation = creations.get(cid)
        if at_seq is not None:
            if raw.get("created_event_seq") and raw["created_event_seq"] > cutoff_seq:
                continue
            if not creation and not visible_messages and _time(raw.get("created_at")) > _time(cutoff):
                continue
            # Old records have no creation snapshot. Only recover known fields;
            # never infer their earlier participants from today's accumulated list.
            item = dict(creation[1]) if creation else {key: raw.get(key) for key in
                ("conversation_id", "kind", "title", "created_at", "discussion_id")}
            participants = list(item.get("participants") or [])
            participants += memberships.get(cid, [])
        else:
            item = dict(raw)
            participants = list(item.get("participants") or [])
        for message in visible_messages:
            participants += [message.get("sender") or {}] + message.get("recipients", [])
        participants = list({json.dumps(p, sort_keys=True): p for p in participants}.values())
        item["participants"] = participants
        item["group_ids"] = sorted({p["id"] for p in participants if isinstance(p, dict) and p.get("type") == "group"})
        if not item.get("line_id"):
            item["line_id"] = next((message.get("line_id") for message in visible_messages if message.get("line_id")), None)
        if (kind and item.get("kind") != kind) or (line_id and item.get("line_id") != line_id):
            continue
        if identity and not any(_involves(p, identity) for p in participants):
            continue
        metadata = item.get("summary_metadata") or {}
        related = [dict(execution, conversation_id=cid) for execution in executions if execution.get("conversation_id") == cid or (
            not execution.get("conversation_id") and execution.get("task_id") and (metadata.get("task_id") == execution["task_id"] or
                cid in {execution["task_id"], research_id + ":" + execution["task_id"]}))]
        # Prefer execution identity. A task can own several conversations, but an
        # unrelated role's call must not make this node's old session look active.
        if identity:
            active = any(execution.get("status") in {"registered", "running"} and
                _involves({"id": execution.get("agent_id") or ""}, identity) for execution in related)
        else:
            active = any(execution.get("status") in {"registered", "running"} for execution in related)
        state = store.get("discussion_state", cid) if at_seq is None else None
        task_id = metadata.get("task_id") or cid
        task = next((task for task in tasks if task_id == task["id"] or task_id.startswith(task["id"] + ":")), None)
        if state and state.get("mode") != "ended" and task and task.get("status") == "running" and worker_live and task.get("owner") == worker.get("owner"):
            active = True
        refs = list({event["seq"]: event for event in [*events_by_conversation.get(cid, []),
            *(event for execution in related for key in (execution.get("execution_attempt_id"), execution.get("execution_id"), execution.get("id"))
                for event in events_by_execution.get(key, []))]}.values())
        refs.sort(key=lambda event: event["seq"])
        activity = refs[-1] if refs else None
        latest = visible_messages[-1] if visible_messages else {}
        item["last_activity_at"] = activity["occurred_at"] if activity else latest.get("updated_at") or latest.get("created_at") or item.get("created_at")
        item["last_activity_seq"] = activity["seq"] if activity else latest.get("latest_event_seq") or latest.get("event_seq") or 0
        item["activity_status"] = "running" if active else "completed" if related or visible_messages else "idle"
        item["is_active"] = bool(active)
        item["last_message_at"] = latest.get("updated_at") or latest.get("created_at")
        item["unread_position"] = 0 if at_seq is not None else item.get("unread_position", 0)
        views.append(summarize_conversation(item, visible_messages, related, state=state))
    views.sort(key=lambda item: (item["is_active"], _time(item["last_activity_at"]), item["last_activity_seq"], item["conversation_id"]), reverse=True)
    return disambiguate_summaries(views)
