import pytest
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from backend.adapters.dsh import RuntimeBlocked
from backend.graphs.engine import ResearchEngine
from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor
from packages.contracts.research import MessageReply, Willingness


@pytest.mark.asyncio
@pytest.mark.parametrize("private,always_truncated", [(True, False), (False, False), (True, True)])
async def test_truncated_turn_gets_more_room_without_becoming_a_json_repair(tmp_path, private, always_truncated):
    store = Store(tmp_path)
    project = store.start_project(store.create_project({"client_request_id": "limits", "topic": "limits", "mode": "demo"})["id"])
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(project["id"], project["deadline_at"])
    calls = []

    class Adapter:
        async def execute_turn(self, **kwargs):
            calls.append(kwargs)
            if always_truncated or len(calls) == 1:
                raise RuntimeBlocked("model_output_truncated", "truncated")
            return {"score": 75} if private else {"content_markdown": "complete reply"}

    try:
        async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "checkpoints.sqlite")) as saver:
            engine = ResearchEngine(store, control, None, None, saver)
            engine.demo = Adapter()
            args = (project["id"], "boss/member-01", "boss", "limits", "first", Willingness if private else MessageReply, {})
            try:
                if always_truncated:
                    with pytest.raises(RuntimeBlocked, match="131072") as error:
                        await engine.turn(*args, publish_message=not private)
                    assert error.value.code == "model_output_truncated"
                else:
                    result = await engine.turn(*args, publish_message=not private)
                    assert result == ({"score": 75} if private else {"content_markdown": "complete reply"})
                limits = [call["runtime_context"]["max_output_tokens"] for call in calls]
                assert limits == ([16384, 32768, 65536, 131072] if always_truncated else [16384, 32768] if private else [65536, 131072])
                assert len({call["turn_id"] for call in calls}) == len(calls)
                assert all(":repair" not in call["turn_id"] for call in calls)
                if private:
                    assert not any(event["type"] == "message.completed" for event in store.events(project["id"]))
            finally:
                await engine.live.close()
    finally:
        control.close()
