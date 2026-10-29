"""Compact activity and tool-call projections over the immutable event log."""
from __future__ import annotations

import json


DETAIL_EVENT_TYPES = (
    "agent.output_delta", "agent.stream_event", "agent.runtime_event",
    "message.started", "message.delta",
)
ACTIVITY_FILTER = "json_extract(data,'$.type') NOT IN (" + ",".join(
    "'" + kind + "'" for kind in DETAIL_EVENT_TYPES) + ")"


def tool_notification(item):
    if item.get("type") != "notification" or item.get("method") != "session.event":
        return None
    event = item.get("payload", {}).get("event", {})
    return event if event.get("type") in {"tool/call", "tool/result"} else None


def tool_event_type(item):
    event = tool_notification(item)
    if not event:
        return "agent.runtime_event"
    if event["type"] == "tool/call":
        return "tool.started"
    return "tool.failed" if event.get("data", {}).get("message", {}).get("isError") else "tool.completed"


def decoded(value):
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (ValueError, TypeError):
            pass
    return value


def project_tool_calls(events, conversations):
    """Accept both old SDK trace events and new explicit tool events.

    Identity includes the execution attempt: providers may reuse a call ID in a
    resumed session. Only supplied events participate, so replay cannot expose
    a result committed after its cursor.
    """
    calls = {}
    for record in events:
        payload = record.get("payload", {})
        event = tool_notification(payload.get("notification", {}))
        if not event:
            continue
        data = event.get("data", {})
        execution = payload.get("execution_attempt_id")
        message = data.get("message", {})
        call_id = data.get("callId") if event["type"] == "tool/call" else message.get("toolCallId") or message.get("source", {}).get("callId")
        if not call_id:
            continue
        identity = (execution, call_id)
        if event["type"] == "tool/call":
            calls.setdefault(identity, {
                "message_id": "tool:" + str(execution) + ":" + call_id,
                "kind": "tool_call", "research_id": record["research_id"],
                "conversation_id": record.get("conversation_id") or conversations.get(execution),
                "sender": {"type": "agent", "id": record.get("agent_id")},
                "execution_id": execution, "call_id": call_id,
                "tool_name": data.get("name", "unknown"), "arguments": decoded(data.get("arguments", {})),
                "content_markdown": data.get("name", "unknown"), "status": "pending",
                "created_at": record["occurred_at"], "event_seq": record["seq"], "latest_event_seq": record["seq"],
            })
        elif identity in calls:
            content = message.get("content", [])
            result = decoded("\n".join(block.get("text", "") for block in content)) if all(
                isinstance(block, dict) and block.get("type") == "text" for block in content) else content
            calls[identity].update(result=result, status="failed" if message.get("isError") else "complete",
                latest_event_seq=record["seq"], completed_at=record["occurred_at"])
    return list(calls.values())
