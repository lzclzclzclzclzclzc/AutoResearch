import asyncio

import pytest
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from backend.graphs.engine import ResearchEngine
from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor


@pytest.mark.asyncio
@pytest.mark.parametrize("same_node", [False, True])
async def test_concurrent_boss_memory_merges_disjoint_updates_and_redeliberates_conflicts(tmp_path, same_node):
    store = Store(tmp_path)
    project = store.start_project(store.create_project({"topic": "concurrent Boss", "mode": "demo",
        "client_request_id": "boss-memory", "team": {"boss_members": 1}})["id"])
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(project["id"], project["deadline_at"])
    source = tmp_path / "observation.txt"
    source.write_text("observed", encoding="utf-8")
    artifact = store.register_artifact(project["id"], source)
    try:
        async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "cp.sqlite")) as saver:
            engine = ResearchEngine(store, control, None, None, saver)
            try:
                original_produce = engine.demo._produce_turn
                async def produce(**args):
                    result = await original_produce(**args)
                    if args["schema"].__name__ == "BossProposal":
                        result["candidate"].update(action="report", assignments=[], request_report=False,
                            memory_updates=[{"node_id": "shared" if same_node else args["task_id"],
                                "content_markdown": args["task_id"], "source_ids": [artifact["id"]]}])
                    return result
                engine.demo._produce_turn = produce
                # Both tasks must finish reading the same memory before either
                # deliberates. Model work must not hold a project-wide commit lock.
                original_discussion = engine.discussions.run
                arrived, both_ready = [], asyncio.Event()
                async def discuss(*args, **kwargs):
                    # This fixture exercises historical candidate/memory conflict
                    # semantics; the new protocol has its own operation tests.
                    args[4]["discussion_protocol"] = 1
                    arrived.append(args[4]["memory_index"])
                    if len(arrived) == 2:
                        both_ready.set()
                    await both_ready.wait()
                    return await original_discussion(*args, **kwargs)
                engine.discussions.run = discuss
                tasks = [store.add_task(project["id"], "boss", {"line_id": line}, "boss-" + line)
                    for line in ("line-01", "line-02")]
                async with asyncio.timeout(10):
                    results = await asyncio.gather(*(engine.boss(task) for task in tasks), return_exceptions=True)
                assert arrived[:2] == [[], []]
                failures = [result for result in results if isinstance(result, BaseException)]
                assert not failures, failures
                if same_node:
                    node = store.list("memory_node", project["id"])[0]
                    assert ":memory-review:" in node["boss_decision_id"]
                    assert len(arrived) == 3 and arrived[-1]
                    assert store.project(project["id"])["memory_version"] == 2
                    revision = store.list("boss_reconsideration", project["id"])[0]
                    # Replay across an engine restart follows the same durable
                    # replacement, rather than repeating either memory write.
                    loser = next(t for t in tasks if t["id"] == revision["id"])
                    restarted = ResearchEngine(store, control, None, None, saver)
                    try:
                        await restarted.boss(loser)
                    finally:
                        await restarted.close()
                    assert store.list("memory_node", project["id"]) == [node]
                else:
                    assert not failures, failures
                    assert store.project(project["id"])["memory_version"] == 2
                    assert {node["boss_decision_id"] for node in store.list("memory_node", project["id"])} == {
                        task["id"] for task in tasks}
                    for task in tasks:
                        assert store.get("boss_effects", task["id"])["status"] == "applied"
                        await engine.boss(task)
                    assert store.project(project["id"])["memory_version"] == 2
                    assert len([event for event in store.events(project["id"])
                        if event["type"] == "memory.version_created"]) == 2
            finally:
                await engine.close()
    finally:
        control.close()
