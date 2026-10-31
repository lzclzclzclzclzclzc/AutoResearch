from fastapi.testclient import TestClient

from apps.api.main import create_app
from backend.persistence.store import Store


def test_vote_metadata_is_searchable_and_replay_only_exposes_committed_ballots(tmp_path):
    app = create_app(tmp_path, testing=True)
    store = Store(tmp_path)
    project = store.create_project({"client_request_id": "votes-ui", "topic": "vote records"})
    rid, cid = project["id"], "vote-records:group-v3"
    members = ["boss/member-01", "boss/member-02"]
    vote = {"id": "v1", "type": "ordinary", "phase": "started", "question": "评估方法",
            "members": members, "options": [{"id": "o1", "label": "分组交叉验证"}], "ballots": []}
    first = store.create_message(rid, cid, members[0], "普通投票已发起", kind="discussion_vote", vote=vote)
    ballot = {"member_id": members[1], "option": "o1", "reason": "相同文本不得跨折"}
    last = store.create_message(rid, cid, members[1], "普通投票已收票", kind="discussion_vote",
                                vote={**vote, "phase": "ballot", "ballots": [ballot]})
    with TestClient(app) as client:
        client.get('/api/session')
        live = client.get(f"/api/conversations/{cid}/messages", params={"q": "相同文本不得跨折"}).json()["items"]
        assert [m["message_id"] for m in live] == [last["message_id"]]
        historical = client.get(f"/api/conversations/{cid}/messages", params={"at_seq": first["event_seq"]}).json()["items"]
        assert len(historical) == 1 and historical[0]["vote"]["ballots"] == []
        assert client.get(f"/api/conversations/{cid}/messages", params={"at_seq": first["event_seq"], "q": ballot["reason"]}).json()["items"] == []
        assert client.get(f"/api/research/{rid}/messages/{last['message_id']}", params={"at_seq": first["event_seq"]}).status_code == 404


def test_new_god_finalization_conversation_is_visible_in_historical_chat(tmp_path):
    app = create_app(tmp_path, testing=True)
    store = Store(tmp_path)
    project = store.create_project({"client_request_id": "god-ui", "topic": "god records"})
    rid = project["id"]
    cid = rid + ":god:gfinal-fixture"
    store.create_conversation(rid, cid, kind="discussion_finalization", protocol_version=2,
                              participants=[{"type": "agent", "id": "god/gfinal-fixture"}])
    message = store.create_message(rid, cid, "god/gfinal-fixture", "最终定稿", kind="decision",
                                   structured={"next_step": "implementation", "final_output": {"content_markdown": "完整产物"}})
    with TestClient(app) as client:
        client.get('/api/session')
        result = client.get(f"/api/research/{rid}/conversations", params={"agent_id": "god", "at_seq": message["event_seq"]})
        assert result.status_code == 200
        assert [item["conversation_id"] for item in result.json()["items"]] == [cid]
