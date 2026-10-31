"""Versioned discussion state, immutable voting history and durable handoff intent."""
from __future__ import annotations

from copy import deepcopy


class DiscussionStoreMixin:
    def initialize_discussion(self, discussion_id, state):
        with self.connect(True) as c:
            existing = self._get(c, "discussion_state", discussion_id)
            if existing:
                if existing["research_id"] != state["research_id"]:
                    raise ValueError("Discussion belongs to a different research project")
                return existing
            saved = self._put(c, "discussion_state", discussion_id,
                {**deepcopy(state), "revision": 0}, state["research_id"])
            source_id = state.get("context", {}).get("handoff_from_discussion")
            source = self._get(c, "discussion_handoff", source_id) if source_id else None
            if source and source.get("status") == "pending":
                self._put(c, "discussion_handoff", source_id, {**source,
                    "status": "delivered", "delivered_to": discussion_id}, state["research_id"])
            return saved

    def commit_discussion_state(self, previous, state, messages=()):
        """A state step and its visible records commit together, including final delivery intent.

        Revision comparison protects against a stale resumed worker. A caller must reload
        state after every commit; message source IDs make a completed step idempotent.
        """
        did, rid = previous["discussion_id"], previous["research_id"]
        with self.connect(True) as c:
            current = self._get(c, "discussion_state", did)
            if current["revision"] != previous["revision"]:
                return current
            updated = self._put(c, "discussion_state", did,
                {**deepcopy(state), "revision": previous["revision"] + 1}, rid)
            for message in messages:
                fields = dict(message)
                sender = fields.pop("sender", {"type": "system", "id": "discussion"})
                content = fields.pop("content_markdown", "")
                conversation_id = fields.pop("conversation_id", did)
                self._create_message(c, rid, conversation_id, sender, content,
                    source_system="discussion_v2", line_id=state.get("line_id"),
                    discussion_protocol=2, **fields)
            if state.get("mode") == "ended":
                finalization_id = state["result"].get("finalization_id")
                finalization = self._get(c, "discussion_finalization", finalization_id) if finalization_id else None
                if finalization:
                    self._put(c, "discussion_finalization", finalization_id, {**finalization,
                        "status": "completed", "output": {"next_step": state["result"]["next_step"],
                            "final_output": state["result"]["final_output"]}}, rid)
                self._put(c, "discussion", did, {"protocol_version": 2,
                    "members": state["members"], "result": state["result"],
                    "decision_source": state["result"]["decision_source"]}, rid)
                # The worker's durable stage transition consumes this intent. It remains
                # available if the process exits between finalization and stage delivery.
                if not self._get(c, "discussion_handoff", did):
                    self._put(c, "discussion_handoff", did, {"discussion_id": did,
                        "status": "pending", "next_step": state["result"]["next_step"],
                        "final_output": state["result"]["final_output"],
                        "decision_source": state["result"]["decision_source"]}, rid)
                self._emit(c, rid, "discussion.finalized", {"discussion_id": did,
                    "next_step": state["result"]["next_step"],
                    "decision_source": state["result"]["decision_source"]}, conversation_id=did)
            return updated
