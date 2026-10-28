import asyncio

import pytest
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from backend.adapters.dsh import RuntimeBlocked
from backend.adapters.gateway import ToolGateway
from backend.graphs.engine import ResearchEngine
from backend.graphs.tasks import CommunicationGraph
from backend.persistence.store import Store, StoreError
from backend.runtime.control import ControlSupervisor
from backend.services.credentials import CredentialService
from packages.contracts.research import Proposal


@pytest.fixture
def mail(tmp_path):
    store = Store(tmp_path)
    p = store.start_project(store.create_project({"client_request_id": "mail", "topic": "Actual communication fixture", "mode": "demo",
        "team": {"n": 1, "boss_members": 2, "m2": 2}, "max_runtime_seconds": 120})["id"])
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(p["id"], p["deadline_at"])
    context = {"research_id": p["id"], "agent_id": "line-01/exploration/member-01", "role": "exploration",
        "line_id": "line-01", "direction_epoch": 1, "control_generation": 1, "execution_id": "sender-execution",
        "task_id": "sender-task", "conversation_id": "sender-conversation"}
    gateway = ToolGateway.__new__(ToolGateway)
    import threading
    gateway.store, gateway.control, gateway.runner = store, control, None
    gateway.capabilities, gateway.lock = {}, threading.RLock()
    token = gateway.register(**context)
    def send(call="question-1", text="请说明评估阈值应在训练集还是验证集选择？", targets=None):
        return gateway.call(token, {"name": "send_research_message", "call_id": call,
            "arguments": {"recipient_ids": targets or ["boss", "line-01/critic"], "content_markdown": text}})
    yield store, control, context, send
    control.close()


def test_send_commits_per_member_inboxes_and_jobs_once(mail, monkeypatch):
    store, _, ctx, send = mail
    original = store._put
    def fail(c, kind, *args, **kwargs):
        if kind == "inbox":
            raise OSError("Injected mailbox transaction failure")
        return original(c, kind, *args, **kwargs)
    monkeypatch.setattr(store, "_put", fail)
    with pytest.raises(OSError):
        send()
    assert not store.messages(ctx["conversation_id"])["items"]
    assert not store.list("inbox", ctx["research_id"]) and not store.list("message_delivery", ctx["research_id"])
    assert not [t for t in store.pending_tasks() if t["kind"] == "communication"]
    monkeypatch.setattr(store, "_put", original)
    receipt = send()
    assert receipt["delivery_status"] == "delivered" and len(receipt["inbox_ids"]) == 4
    assert {i["recipient_id"] for i in store.list("inbox", ctx["research_id"])} == {
        "boss/member-01", "boss/member-02", "line-01/critic/member-01", "line-01/critic/member-02"}
    before = len(store.events(ctx["research_id"]))
    assert send() == receipt and len(store.events(ctx["research_id"])) == before
    assert len([t for t in store.pending_tasks() if t["kind"] == "communication"]) == 2
    with pytest.raises(StoreError, match="同一消息调用 ID"):
        send(text="different")


def test_mail_consumption_is_frozen_for_replay_and_per_member(mail):
    store, control, ctx, send = mail
    first = send(targets=["line-01/critic"])
    rid, member = ctx["research_id"], "line-01/critic/member-01"
    received = store.consume_agent_inbox(rid, member, "turn-one", control, 1)
    second = send(call="question-2", text="还需要一个固定的测试集吗？", targets=["line-01/critic"])
    assert store.consume_agent_inbox(rid, member, "turn-one", control, 1) == received
    later = store.consume_agent_inbox(rid, member, "turn-two", control, 1)
    assert [i["message_id"] for i in later] == [second["message_id"]]
    other = store.consume_agent_inbox(rid, "line-01/critic/member-02", "other-member", control, 1)
    assert [i["message_id"] for i in other] == [first["message_id"], second["message_id"]]
    assert all(i["status"] == "acknowledged" for i in store.list("message_delivery", rid))


def test_stop_during_delivery_rolls_back_and_obsolete_direction_is_not_consumed(mail, monkeypatch):
    store, control, ctx, send = mail
    receipt = send(targets=["line-01/critic"])
    store.update_line(ctx["research_id"], "line-01", direction_epoch=2)
    assert store.consume_agent_inbox(ctx["research_id"], "line-01/critic/member-01", "new-direction", control, 2) == []
    assert store.get("inbox", receipt["inbox_ids"][0])["status"] == "delivered"
    # Restore the sender's epoch, then inject stop between writing inbox and commit.
    store.update_line(ctx["research_id"], "line-01", direction_epoch=1)
    original = store._put
    def stop(c, kind, *args, **kwargs):
        result = original(c, kind, *args, **kwargs)
        if kind == "inbox":
            control.terminate(ctx["research_id"])
        return result
    monkeypatch.setattr(store, "_put", stop)
    with pytest.raises(StoreError):
        send(call="stop-race", targets=["boss"])
    assert len(store.messages(ctx["conversation_id"])["items"]) == 1
    assert len(store.list("inbox", ctx["research_id"])) == 2


@pytest.mark.asyncio
async def test_idle_boss_actually_reads_and_replies_after_interruption_without_scientific_action(mail, tmp_path):
    store, control, ctx, send = mail
    receipt = send(targets=["boss"])
    task = next(t for t in store.pending_tasks() if t["kind"] == "communication")
    async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "checkpoints.sqlite")) as saver:
        engine = ResearchEngine(store, control, None, CredentialService(tmp_path, store), saver)
        original, contexts, interrupted = engine.demo._produce_turn, [], False
        async def model(**args):
            nonlocal interrupted
            if args["schema"].__name__ == "MessageReply":
                contexts.append(args["context"]["received_messages"])
                assert contexts[-1][0]["message_id"] == receipt["message_id"]
                assert "评估阈值" in contexts[-1][0]["message"]["content_markdown"]
                if not interrupted:
                    interrupted = True
                    raise RuntimeBlocked("fixture_interrupted", "Crash after reading inbox before replying")
            return await original(**args)
        engine.demo._produce_turn = model
        graph = CommunicationGraph(engine.communication, saver)
        with pytest.raises(RuntimeBlocked, match="Crash after reading"):
            await graph.run(task)
        reply = await graph.run(task)
        assert contexts[0] == contexts[1]
        assert "实际投递" in reply["content_markdown"]
        assert await graph.run(task) == reply and len(contexts) == 2
        pending = store.consume_agent_inbox(ctx["research_id"], ctx["agent_id"], "read-boss-reply", control, 1)
        assert pending[0]["message"]["content_markdown"] == reply["content_markdown"]
        assert not store.list("cycle", ctx["research_id"]) and not store.list("memory_node", ctx["research_id"])
        assert not store.list("run", ctx["research_id"])
        assert len([t for t in store.pending_tasks() if t["kind"] == "communication"]) == 1  # No reply-to-reply loop.
        await engine.close()


@pytest.mark.asyncio
async def test_every_group_member_reads_mail_in_regular_turns_and_wakeup_then_skips(mail, tmp_path):
    store, control, ctx, send = mail
    send(targets=["line-01/critic"])
    async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "checkpoints.sqlite")) as saver:
        engine = ResearchEngine(store, control, None, CredentialService(tmp_path, store), saver)
        original, seen = engine.demo._produce_turn, set()
        async def model(**args):
            if args["schema"].__name__ == "Proposal":
                assert "评估阈值" in args["context"]["received_messages"][0]["message"]["content_markdown"]
                seen.add(args["agent_id"])
            return await original(**args)
        engine.demo._produce_turn = model
        await engine.discussions.run(ctx["research_id"], "critic-mail", "critic", engine.members(store.project(ctx["research_id"]), "critic", "line-01"), {}, "line-01")
        assert len(seen) == 2
        task = next(t for t in store.pending_tasks() if t["kind"] == "communication")
        assert (await engine.communication(task))["already_consumed_or_obsolete"]
        await engine.close()


@pytest.mark.asyncio
async def test_interrupted_mail_for_an_old_direction_is_not_replayed_in_the_new_direction(mail, tmp_path):
    store, control, ctx, send = mail
    send(targets=["line-01/critic"])
    task = next(t for t in store.pending_tasks() if t["kind"] == "communication")
    async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "checkpoints.sqlite")) as saver:
        engine = ResearchEngine(store, control, None, CredentialService(tmp_path, store), saver)
        original, replies = engine.demo._produce_turn, []
        async def interrupted(**args):
            if args["schema"].__name__ == "MessageReply":
                replies.append(args["context"]["direction_epoch"])
                raise RuntimeBlocked("fixture_interrupted", "Stop before replying")
            return await original(**args)
        engine.demo._produce_turn = interrupted
        graph = CommunicationGraph(engine.communication, saver)
        with pytest.raises(RuntimeBlocked):
            await graph.run(task)
        store.update_line(ctx["research_id"], "line-01", direction_epoch=2)
        assert (await graph.run(task))["already_consumed_or_obsolete"]
        assert replies == [1]
        await engine.close()


@pytest.mark.asyncio
async def test_reply_to_old_sender_direction_is_not_delivered_into_new_direction(mail, tmp_path):
    store, control, ctx, send = mail
    send(targets=["boss"])
    task = next(t for t in store.pending_tasks() if t["kind"] == "communication")
    store.update_line(ctx["research_id"], "line-01", direction_epoch=2)
    async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "checkpoints.sqlite")) as saver:
        engine = ResearchEngine(store, control, None, CredentialService(tmp_path, store), saver)
        await engine.communication(task)
        entries = store.list("inbox", ctx["research_id"], recipient_id=ctx["agent_id"])
        assert entries[0]["direction_epoch"] == 1
        assert store.consume_agent_inbox(ctx["research_id"], ctx["agent_id"], "new-direction-reply", control, 2) == []
        await engine.close()
