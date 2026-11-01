from backend.persistence.conversation_views import (
    conversation_metadata, disambiguate_summaries, summarize_conversation,
)


def test_discussion_revision_and_vote_are_distinct_from_speech_round():
    metadata = conversation_metadata("line-01/critic/member-1", "critic", "c:revision-2:critic", "discussion", {})
    item = {"conversation_id": "c:revision-2:critic", "summary_metadata": metadata,
        "kind": "group_discussion", "title": "x" * 4000}
    view = summarize_conversation(item, [{"discussion_round": 8, "vote": {"type": "end"}}], state={"mode": "end_vote"})
    assert "线 01 · 方案修订 2 · 评审组结束投票 · 进行中" == view["summary"]
    assert "第 8" not in view["summary"]
    assert view["conversation_id"] == item["conversation_id"]


def test_monitor_round_comes_from_latest_execution_in_same_conversation():
    metadata = conversation_metadata("line-02/implementation", "implementation", "run", "monitor-1", {})
    item = {"conversation_id": "run:monitor", "summary_metadata": metadata, "created_at": "2026-11-01T02:40:00Z"}
    view = summarize_conversation(item, [], [
        {"conversation_id": "run:monitor", "turn_id": "monitor-1", "status": "completed"},
        {"conversation_id": "run:monitor", "turn_id": "monitor-3:generation-2", "status": "running"},
        {"conversation_id": "other:monitor", "turn_id": "monitor-99", "status": "running"},
    ])
    assert view["summary"] == "线 02 · 实施 · 实验监控第 3 次 · 进行中 · 11-01 10:40"


def test_global_initial_assignment_and_verification_have_own_names():
    initial = conversation_metadata("boss/member-1", "boss", "initial", "discussion", {"initial": True})
    assert summarize_conversation({"conversation_id": "initial", "summary_metadata": initial}, [])[
        "summary"] == "全局 · Boss 初始分派 · 待开始"
    verify = {**conversation_metadata("line-01/verify", "verify", "submission", "verify", {}), "verify_round": 2}
    assert "验证第 2 轮" in summarize_conversation({"conversation_id": "v", "summary_metadata": verify}, [
        {"sender": {"id": "line-01/verify"}, "discussion_round": 17}])["summary"]


def test_legacy_options_use_known_role_activity_time_and_short_identity():
    views = [summarize_conversation({"conversation_id": cid, "title": "critic 组内讨论",
        "created_at": "2026-11-01T01:10:00Z"}, [{"sender": {"id": "line-01/critic/member-1"}}])
        for cid in ["unknown-conversation-00000001", "unknown-conversation-00000002"]]
    unique = disambiguate_summaries(views)
    assert unique[0]["summary"].endswith("09:10 · 00000001")
    assert unique[1]["summary"].endswith("09:10 · 00000002")
    assert "线 01 · 评审组讨论 · 已结束" in unique[0]["summary"]
