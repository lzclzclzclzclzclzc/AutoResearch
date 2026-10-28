"""Durable Verify transport must feed the next actual role contexts."""
import asyncio
import threading

import pytest
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from backend.graphs.engine import ResearchEngine
from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor
from backend.runtime.runner import RunnerSupervisor
from backend.runtime.worker import dispatch_pending_handoffs
from backend.services.credentials import CredentialService


@pytest.mark.asyncio
async def test_failed_verify_reaches_both_groups_then_boss_once_after_delivery_retry(tmp_path):
    store = Store(tmp_path)
    project = store.create_project({"client_request_id": "handoff-workflow", "topic": "Local handoff fixture", "mode": "demo",
        "team": {"n": 1, "m1": 1, "m2": 1, "boss_members": 1, "k": 3}, "max_runtime_seconds": 120})
    control = ControlSupervisor(tmp_path / "control")
    runner = RunnerSupervisor(tmp_path, control=control)
    seen, verify_count, dispatch_count = [], 0, 0
    worker_deliveries = []
    class RecoveryEngine(ResearchEngine):
        async def route_result(self, state):
            if state["route"] == "boss":
                # Simulate recovery after Verify's commit but before its graph
                # route node: the ordinary Worker drain owns delivery/task creation.
                pending = [item for item in store.list("outbox", project["id"]) if item["payload"]["submission_version_id"] == state["submission_id"] and item["status"] == "pending"]
                if pending:
                    assert not [task for task in store.pending_tasks() if task["kind"] == "boss"]
                    await dispatch_pending_handoffs(store, control)
                    assert store.get("outbox", pending[0]["id"])["status"] == "delivered"
                    worker_deliveries.append(pending[0]["id"])
            return await super().route_result(state)
    original_dispatch = store.dispatch_outbox
    def flaky_dispatch(*args, **kwargs):
        nonlocal dispatch_count
        dispatch_count += 1
        if dispatch_count == 1:
            raise ConnectionError("Injected handoff transport failure before any delivery")
        return original_dispatch(*args, **kwargs)
    store.dispatch_outbox = flaky_dispatch
    try:
        async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "checkpoints.sqlite")) as saver:
            engine = RecoveryEngine(store, control, runner, CredentialService(tmp_path, store), saver)
            original = engine.demo._produce_turn
            async def fixture(**arguments):
                nonlocal verify_count
                output = await original(**arguments)
                seen.append({"role": arguments["role"], "schema": arguments["schema"].__name__, "task_id": arguments["task_id"],
                    "context": arguments.get("context", {})})
                if arguments["schema"].__name__ == "Verification":
                    verify_count += 1
                    if verify_count == 1:
                        output.update(passed=False, required_changes=["HANDOFF_REQUIRED_CHANGE"],
                            content_markdown="演示验证交接测试：HANDOFF_REPORT_MARKER；请补齐证据。")
                return output
            engine.demo._produce_turn = fixture
            supervisor = next(t for t in store.pending_tasks() if t["kind"] == "supervisor")
            await engine.supervisor(supervisor)
            store.finish_task(supervisor["id"])
            line = next(t for t in store.pending_tasks() if t["kind"] == "line")
            await engine.line(line)
            store.finish_task(line["id"])
            assert verify_count == 2 and dispatch_count >= 3
            assert len(worker_deliveries) == 1
            revisions = [entry for entry in seen if ":revision-2:" in entry["task_id"] and entry["schema"] == "Proposal"]
            assert {entry["role"] for entry in revisions} >= {"exploration", "critic"}
            for role in ("exploration", "critic"):
                context = next(entry["context"] for entry in revisions if entry["role"] == role)
                assert len(context["received_handoffs"]) == 1
                handoff = context["received_handoffs"][0]
                assert handoff["verification"]["required_changes"] == ["HANDOFF_REQUIRED_CHANGE"]
                assert "HANDOFF_REPORT_MARKER" in handoff["message"]["content_markdown"]
                assert handoff["payload"]["route"] == "exploration_and_critic"
            verifications = store.list("verification", project["id"])
            passed = next(item for item in verifications if item["passed"])
            expected_boss_id = passed["submission_version_id"] + ":boss"
            bosses = [task for task in store.pending_tasks() if task["kind"] == "boss"]
            assert len(bosses) == 1 and bosses[0]["id"] == expected_boss_id
            inbox_before = store.list("inbox", project["id"])
            await dispatch_pending_handoffs(store, control)
            await engine.route_result({"research_id": project["id"], "line_id": "line-01", "cycle_id": passed["cycle_id"],
                "submission_id": passed["submission_version_id"], "route": "boss", "revision": 2})
            assert store.list("inbox", project["id"]) == inbox_before
            await engine.boss(bosses[0])
            boss_turn = next(entry for entry in reversed(seen) if entry["role"] == "boss" and entry["schema"] == "BossProposal")
            assert len(boss_turn["context"]["received_handoffs"]) == 1
            assert boss_turn["context"]["received_handoffs"][0]["verification"]["passed"] is True
            assert len([task for task in store.list("task", project["id"]) if task["kind"] == "boss"]) == 1
            outboxes = store.list("outbox", project["id"])
            assert len(outboxes) == 2 and all(item["status"] == "delivered" for item in outboxes)
            inboxes = store.list("inbox", project["id"])
            assert len(inboxes) == 3 and len({item["message_id"] for item in inboxes}) == 2
            with store.connect() as connection:
                dispatched = connection.execute("SELECT source_event_id FROM messages WHERE research_id=? AND source_system='verification_dispatch'", (project["id"],)).fetchall()
            assert {row["source_event_id"] for row in dispatched} == {item["submission_version_id"] for item in verifications}
            await engine.close()
    finally:
        runner.close()
        control.close()


@pytest.mark.asyncio
async def test_worker_delivery_failure_remains_retryable_and_does_not_block_other_work():
    entered, release = threading.Event(), threading.Event()
    class PendingStore:
        attempts, pending = 0, True
        def dispatch_outbox(self, control, limit):
            self.attempts += 1
            assert limit == 25
            if self.attempts == 1:
                raise ConnectionError("Injected transient service loss")
            entered.set()
            release.wait(5)
            self.pending = False
            return {"delivered": 1}
    store = PendingStore()
    assert await dispatch_pending_handoffs(store, object()) == {"retry_pending": True}
    assert store.pending
    drain = asyncio.create_task(dispatch_pending_handoffs(store, object()))
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        # The drain awaits its thread; it does not monopolize active role tasks.
        progressed = []
        async def unrelated():
            await asyncio.sleep(0)
            progressed.append("continued")
        await asyncio.wait_for(unrelated(), 1)
        assert progressed == ["continued"] and not drain.done()
    finally:
        release.set()
        await drain
    assert not store.pending and store.attempts == 2
