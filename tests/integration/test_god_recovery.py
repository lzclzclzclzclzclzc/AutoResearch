import pytest
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from backend.adapters.dsh import RuntimeBlocked
from backend.graphs.engine import ResearchEngine, god_prompt_context
from backend.graphs.tasks import PaperGraph, TaskSuspended, current_task
from backend.graphs.decomposition import merge_parts, fragment_contract
from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor
from packages.contracts.research import MessageReply, ExperimentSpec, Implementation, BossDecision, digest


def split_plan(schema=MessageReply):
    return {"target_schema_hash": digest(schema.model_json_schema()), "parts": [
        {"part_id": name, "target_role": "boss", "instructions": "write " + name, "input_refs": [],
         "output_schema": {"type": "object", "properties": {"sections": {"type": "object", "properties": {name: {"type": "string"}},
             "required": [name], "additionalProperties": False}}, "required": ["sections"], "additionalProperties": False},
         "allowed_write_paths": ["/sections/" + name], "depends_on": []} for name in ("first", "second")],
        "merge_plan": {"strategy": "markdown_sections", "order": ["first", "second"], "base_output": {}},
        "invariants": {}, "acceptance_checks": ["both sections and original MessageReply"]}


def test_god_prompt_preserves_candidates_and_exposes_complete_pagination_manifest():
    sections = {"candidates": {"a": {"long": "x" * 20000}}, "issues": [{"issue_id": "fix"}],
        "discussion": [{"message_id": f"m-{i}", "content_markdown": "history " + str(i)} for i in range(60)],
        "diagnostic": {"source_output": "y" * 50000}, "records": {"vote:1:a": {"candidate_id": "a"}}}
    projected = god_prompt_context(sections)
    assert projected["snapshot_excerpt"]["candidates"] == sections["candidates"]
    assert projected["snapshot_excerpt"]["records"]["votes"] == sections["records"]
    history = projected["snapshot_excerpt"]["discussion"]
    assert history["total_messages"] == 60 and len(history["recent_messages"]) == 24 and not history["complete"]
    assert projected["snapshot_excerpt"]["diagnostic"]["source_output"]["complete"] is False
    assert len(projected["available_evidence_refs"]) == 60
    assert len(projected["snapshot_manifest"]) == len(sections)
    assert "a" not in projected["available_evidence_refs"]
    assert len(sections["diagnostic"]["source_output"]) == 50000


@pytest.mark.parametrize("failure", ["fixed", "path", "entrypoint", "case_collision"])
def test_god_implementation_repair_cannot_bypass_original_business_constraints(failure):
    engine = object.__new__(ResearchEngine)
    spec = ExperimentSpec(spec_id="s", version=1, question="q", hypothesis="h", fixed={"seed": 42},
        baseline="b", data={}, metrics=["mse"], design={}, monitoring={}, completion_criteria="done", deadline_at="2030-01-01T00:00:00Z")
    output = Implementation(content_markdown="code", spec_hash=digest(spec.model_dump()), files={"main.py": "pass"},
        entrypoint="main.py", actual_config=spec.fixed).model_dump()
    if failure == "fixed":
        output["actual_config"] = {"seed": 4}
    elif failure == "hash":
        output["spec_hash"] = "wrong"
    elif failure == "path":
        output["files"] = {"../main.py": "pass"}
    elif failure == "entrypoint":
        output["entrypoint"] = "absent.py"
    else:
        output["files"]["MAIN.py"] = "different"
    # Existing normal implementation flow can still return a plan-revision request.
    assert engine.validate_output("r", "implementation", Implementation, output, {"spec": spec.model_dump()}) == output
    with pytest.raises(ValueError):
        engine.validate_recovery_output("r", "implementation", Implementation, output, {"spec": spec.model_dump()})


def test_fragment_validation_and_deterministic_merge():
    plan = split_plan()
    contract = fragment_contract(plan["parts"][0]["output_schema"])
    with pytest.raises(ValueError):
        contract.model_validate({"sections": {"first": 3}})
    with pytest.raises(ValueError, match="Unsupported"):
        fragment_contract({"type": "string", "pattern": "[a-z]+"})
    results = {name: {"sections": {name: name.upper()}} for name in ("first", "second")}
    assert merge_parts(plan, results) == {"content_markdown": "FIRST\n\nSECOND"}
    with pytest.raises(ValueError):
        merge_parts(plan, {**results, "first": {"sections": {"wrong": "bad"}}})


@pytest.mark.asyncio
async def test_truncated_parent_interrupts_and_children_resume_without_worker_slot(tmp_path):
    store = Store(tmp_path)
    p = store.start_project(store.create_project({"client_request_id": "split", "topic": "split", "mode": "demo"})["id"])
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(p["id"], p["deadline_at"])
    try:
        async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "cp.sqlite")) as saver:
            engine = ResearchEngine(store, control, None, None, saver)
            calls = []
            async def produce(**args):
                calls.append((args["role"], args["schema"].__name__, args["turn_id"]))
                if args["schema"] is MessageReply:
                    raise RuntimeBlocked("model_output_truncated", "truncated")
                if args["role"] == "god":
                    req = args["context"]["arbitration_request"]
                    return {"arbitration_id": req["id"], "snapshot_hash": req["snapshot_hash"],
                        "action": "decompose_task", "content_markdown": "split into two parts", "rationale": "bounded output",
                        "evidence_refs": [], "limitations": [], "unresolved_issue_ids": [], "plan": split_plan()}
                path = args["context"]["owned_output_paths"][0].rsplit("/", 1)[-1]
                return {"sections": {path: path.upper()}}
            engine.demo._produce_turn = produce
            async def operation(task):
                return await engine.turn(p["id"], "boss/member-01", "boss", "logical", "reply", MessageReply, {"instruction": "long reply"})
            parent_graph = PaperGraph(operation, saver)
            task = store.add_task(p["id"], "paper", {}, "parent")
            token = current_task.set(task)
            try:
                with pytest.raises(TaskSuspended) as suspended:
                    await parent_graph.run(task)
            finally:
                current_task.reset(token)
            store.patch("task", task["id"], status="waiting_recovery", resume_token=suspended.value.resume_token)
            # A single execution slot is sufficient: the parent has returned.
            for child in store.pending_tasks():
                if child["kind"] == "decomposition_part":
                    result = await engine.decompositions.execute_part(child)
                    store.finish_task(child["id"], result=result)
                    await engine.decompositions.dispatch(p["id"])
            resumed = store.get("task", task["id"])
            assert resumed["status"] == "pending" and resumed["recovery_outcome"]["status"] == "fulfilled"
            token = current_task.set(resumed)
            try:
                result = await parent_graph.run(resumed)
            finally:
                current_task.reset(token)
            assert result == {"content_markdown": "FIRST\n\nSECOND"}
            assert sum(role == "god" for role, _, _ in calls) == 1
            assert sum(schema == "RecoveryFragment" for _, schema, _ in calls) == 2
            before = len(store.list("recovery_resume_outbox"))
            await engine.decompositions.dispatch(p["id"])
            assert len(store.list("recovery_resume_outbox")) == before == 1
            await engine.close()
    finally:
        control.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("first_wait", ["decomposition", "evidence"])
async def test_consecutive_recovery_waits_keep_interrupt_order_across_restarts(tmp_path, first_wait):
    store = Store(tmp_path)
    project = store.start_project(store.create_project({"topic": "two recoveries", "mode": "demo",
        "client_request_id": "two-recoveries"})["id"])
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(project["id"], project["deadline_at"])
    source = tmp_path / "observation.txt"
    source.write_text("observed", encoding="utf-8")
    artifact = store.register_artifact(project["id"], source)
    parent = store.add_task(project["id"], "paper", {}, "consecutive-parent")
    calls, tokens = [], []
    async def produce(**args):
        name = args["schema"].__name__
        calls.append(name)
        if name == "MessageReply":
            raise RuntimeBlocked("model_output_truncated", "truncated")
        if name == "GodDecision":
            request = args["context"]["arbitration_request"]
            output = {"arbitration_id": request["id"], "snapshot_hash": request["snapshot_hash"],
                "content_markdown": "recover", "rationale": "bounded recovery"}
            if "request_evidence" in request["allowed_actions"]:
                return {**output, "action": "request_evidence", "target_role": "boss", "question": "read observation",
                    "source_refs": [artifact["id"]], "steps": ["read registered source"], "result_requirements": ["observed source"]}
            return {**output, "action": "decompose_task", "plan": split_plan()}
        if name == "EvidenceRecoveryOutput":
            return {"content_markdown": "observed", "evidence": [{"source_id": artifact["id"],
                "claim": "observed", "status": "observed"}], "limitations": []}
        part = args["context"]["owned_output_paths"][0].rsplit("/", 1)[-1]
        return {"sections": {part: part.upper()}}
    try:
        # Recreate both the engine and checkpoint connection on every resume;
        # interrupt order must depend on persisted state, not in-memory objects.
        for step in range(3):
            async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "cp.sqlite")) as saver:
                engine = ResearchEngine(store, control, None, None, saver)
                engine.demo._produce_turn = produce
                try:
                    async def operation(task):
                        if first_wait == "evidence":
                            context = {"role": "boss", "evidence": [{"source_id": artifact["id"]}]}
                            decision = await engine.arbitration.resolve(research_id=project["id"], source_task_id=task["id"],
                                scope_kind="discussion", scope_id="first-evidence", reason_code="design_deadlock",
                                origin_node="discussion.collect", context=context, allowed_actions=["request_evidence"])
                            request = store.get("arbitration_request", decision["arbitration_id"])
                            first = await engine.recover_action(request, decision, context=context,
                                agent_id="boss/member-01", role="boss", task_id="first", turn_id="recover")
                        else:
                            first = await engine.turn(project["id"], "boss/member-01", "boss", "first", "reply",
                                MessageReply, {"instruction": "first reply"})
                        second = await engine.turn(project["id"], "boss/member-01", "boss", "second", "reply",
                            MessageReply, {"instruction": "second reply"})
                        return {"first": first, "second": second}
                    graph = PaperGraph(operation, saver)
                    task = store.get("task", parent["id"])
                    token = current_task.set(task)
                    try:
                        if step < 2:
                            with pytest.raises(TaskSuspended) as suspended:
                                await graph.run(task)
                            tokens.append(suspended.value.resume_token)
                            store.patch("task", task["id"], status="waiting_recovery", resume_token=tokens[-1], recovery_outcome=None)
                        else:
                            result = await graph.run(task)
                            assert result["first"]["content_markdown"] == ("observed" if first_wait == "evidence" else "FIRST\n\nSECOND")
                            assert result["second"] == {"content_markdown": "FIRST\n\nSECOND"}
                            store.finish_task(task["id"], result=result)
                    finally:
                        current_task.reset(token)
                    for child in store.pending_tasks():
                        if child["kind"] in {"decomposition_part", "recovery_role"}:
                            execute = engine.decompositions.execute_part if child["kind"] == "decomposition_part" else engine.decompositions.execute_evidence
                            store.finish_task(child["id"], result=await execute(child))
                    await engine.decompositions.dispatch(project["id"])
                finally:
                    await engine.close()
        assert len(set(tokens)) == 2
        assert calls.count("GodDecision") == 2
        assert calls.count("RecoveryFragment") == (2 if first_wait == "evidence" else 4)
        assert len(store.list("recovery_resume_outbox", project["id"], status="delivered")) == 2
        assert all(plan["status"] == "fulfilled" for plan in store.list("decomposition_plan", project["id"]))
        assert store.get("task", parent["id"])["status"] == "completed"
    finally:
        control.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("replace_failed_plan", [False, True])
async def test_real_worker_capacity_one_runs_children_while_parent_waits(tmp_path, monkeypatch, replace_failed_plan):
    import asyncio
    import backend.graphs.engine as engine_module
    import backend.runtime.control as control_module
    import backend.runtime.runner as runner_module
    from backend.runtime.worker import run_worker
    store = Store(tmp_path)
    p = store.start_project(store.create_project({"client_request_id": "worker-split", "topic": "split", "mode": "demo"})["id"])
    for initial in store.pending_tasks():
        store.finish_task(initial["id"], result={"fixture": True})
    root = store.add_task(p["id"], "paper", {}, "worker-parent")
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(p["id"], p["deadline_at"])
    generations = {"god": 0, "first": 0}
    class Engine(ResearchEngine):
        def __init__(self, *args):
            super().__init__(*args)
            self.demo._produce_turn = self.produce
        async def paper(self, task):
            return await self.turn(p["id"], "boss/member-01", "boss", "logical", "reply", MessageReply, {"instruction": "long reply"})
        async def produce(self, **args):
            if args["schema"] is MessageReply:
                raise RuntimeBlocked("model_output_truncated", "truncated")
            if args["role"] == "god":
                req = args["context"]["arbitration_request"]
                generations["god"] += 1
                plan = split_plan()
                if generations["god"] > 1:
                    plan["parts"][1]["instructions"] = "write second recovered"
                return {"arbitration_id": req["id"], "snapshot_hash": req["snapshot_hash"], "action": "decompose_task",
                    "content_markdown": "split", "rationale": "bounded", "plan": plan}
            part = args["context"]["owned_output_paths"][0].rsplit("/", 1)[-1]
            if part == "first":
                generations["first"] += 1
            if replace_failed_plan and part == "second" and "recovered" not in args["context"]["instruction"]:
                return {"sections": {"second": 12}}
            await asyncio.sleep(0.05)
            return {"sections": {part: part.upper()}}
    monkeypatch.setattr(engine_module, "ResearchEngine", Engine)
    monkeypatch.setattr(control_module, "ControlClient", lambda _: control)
    monkeypatch.setattr(runner_module, "RunnerClient", lambda _: None)
    monkeypatch.setenv("AUTORESEARCH_WORKER_CONCURRENCY", "1")
    worker = asyncio.create_task(run_worker(tmp_path, "fixture", "fixture"))
    try:
        async with asyncio.timeout(15):
            while store.get("task", root["id"])["status"] != "completed":
                if worker.done():
                    await worker
                await asyncio.sleep(0.05)
        assert store.get("task", root["id"])["result"] == {"content_markdown": "FIRST\n\nSECOND"}
        children = [t for t in store.list("task", p["id"]) if t["kind"] == "decomposition_part"]
        assert len(children) == (3 if replace_failed_plan else 2)
        assert sum(t["status"] == "completed" for t in children) == 2
        assert any(e["type"] == "task.suspended" for e in store.events(p["id"]))
        assert len(store.list("arbitration_request", p["id"])) == (2 if replace_failed_plan else 1)
        assert generations["first"] == 1
        if replace_failed_plan:
            chains = store.list("recovery_chain", p["id"])
            assert len(chains) == 1 and chains[0]["rounds"] == 2 and chains[0]["status"] == "fulfilled"
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
        control.close()
