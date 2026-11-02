"""Legacy discussions retain every owner's runtime acceptance requirement."""
from copy import deepcopy
from types import SimpleNamespace

import pytest
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from backend.graphs.discussion import GroupDiscussionGraph
from backend.graphs.engine import ResearchEngine
from backend.graphs.issue_requirements import merge_execution_requirements
from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor
from packages.contracts.research import ExperimentSpec, Issue, LegacyVerification


def requirement(description, issue_id="acceptance-1", **fields):
    return Issue(issue_id=issue_id, category="must_fix", description=description,
        acceptance_stage="execution", experiment_mapping=description, **fields).model_dump()


def test_member_collisions_preserve_both_requirements_and_each_owners_latest_transition(tmp_path):
    store = Store(tmp_path)
    rid = store.create_project({"topic": "legacy issues", "client_request_id": "legacy"})["id"]
    graph = GroupDiscussionGraph.__new__(GroupDiscussionGraph)
    graph.engine = SimpleNamespace(store=store)
    first, second = requirement("Measure F1"), requirement("Measure runtime")
    updated = {**first, "resolved": True, "resolution": "Observed the requested F1 result"}
    state = {"research_id": rid, "records": {
        "review:1:critic/member-01": {"issues": [first]},
        "review:1:critic/member-02": {"issues": [second]},
        "review:2:critic/member-01": {"issues": [updated]},
        # A host quoting its peer cannot overwrite its own original requirement.
        "synthesis:2:critic/member-01": {"issues": [second]},
    }}
    before = deepcopy(state)
    result = graph.retain_execution_requirements(state, {"issues": [second]})
    issues = result["issues"]
    assert len(issues) == len({issue["issue_id"] for issue in issues}) == 2
    assert {issue["description"]: issue["resolved"] for issue in issues} == {"Measure F1": True, "Measure runtime": False}
    assert all("raised_by" not in issue for issue in issues)
    assert graph.retain_execution_requirements(state, deepcopy(result)) == result
    assert state == before
    unique = requirement("Unique requirement", issue_id="original-id")
    assert merge_execution_requirements([("owner", unique)]) == [unique]


@pytest.fixture
async def legacy_engine(tmp_path):
    store = Store(tmp_path)
    project = store.start_project(store.create_project({"topic": "legacy requirements", "mode": "demo",
        "client_request_id": "legacy-requirements", "team": {"n": 1, "m1": 1, "m2": 2}})["id"])
    control = ControlSupervisor(tmp_path / "control")
    control.register_project(project["id"], project["deadline_at"])
    try:
        async with AsyncSqliteSaver.from_conn_string(str(tmp_path / "cp.sqlite")) as saver:
            engine = ResearchEngine(store, control, None, None, saver)
            try:
                yield engine, project
            finally:
                await engine.close()
    finally:
        control.close()


@pytest.mark.asyncio
async def test_completed_legacy_checkpoint_recovers_omitted_owner_without_recasting(legacy_engine):
    engine, project = legacy_engine
    rid, did = project["id"], "old-discussion:serial-v2"
    members = engine.members(project, "critic", "line-01")
    first, second = requirement("Measure F1"), requirement("Measure runtime")
    state = {"research_id": rid, "discussion_id": did, "role": "critic", "members": members,
        "line_id": "line-01", "context": {"discussion_protocol": 1},
        "records": {f"review:1:{members[0]}": {"issues": [first]},
            f"review:1:{members[1]}": {"issues": [second]}},
        "result": {"issues": [second], "selected_payload": {"method": "original choice"}}}
    config = {"configurable": {"thread_id": f"research/{rid}/discussion/{did}"}}
    await engine.discussions.graph.aupdate_state(config, state, as_node="synthesize")
    async def forbidden(*args, **kwargs):
        pytest.fail("completed discussion recovery must use the original member records")
    engine.turn = forbidden
    recovered = await engine.discussions.run(rid, "old-discussion", "critic", members,
        {"discussion_protocol": 1}, "line-01")
    assert {issue["description"] for issue in recovered["issues"]} == {"Measure F1", "Measure runtime"}
    assert recovered["selected_payload"] == state["result"]["selected_payload"]
    assert await engine.discussions.run(rid, "old-discussion", "critic", members,
        {"discussion_protocol": 1}, "line-01") == recovered
    snapshot = await engine.discussions.graph.aget_state(config)
    assert snapshot.values["records"] == state["records"]
    assert snapshot.values["result"] == state["result"]


@pytest.mark.asyncio
async def test_cross_group_collisions_reach_frozen_spec_and_need_separate_verify_checks(legacy_engine, tmp_path):
    engine, project = legacy_engine
    store, rid = engine.store, project["id"]
    store.put("cycle", "cycle", {"line_id": "line-01", "objective": "Compare F1 and runtime"}, rid)
    store.update_line(rid, "line-01", cycle_id="cycle")
    groups = {"exploration": {"issues": [requirement("Measure F1")], "selected_payload": {}},
        "critic": {"issues": [requirement("Measure runtime")], "selected_payload": {}}}
    async def discuss(research_id, did, role, *args):
        return deepcopy(groups[role])
    engine.discussions.run = discuss
    original = engine.demo._produce_turn
    async def model(**arguments):
        output = await original(**arguments)
        if arguments["schema"].__name__ == "ExperimentSpec":
            requirements = arguments["context"]["execution_requirements"]
            assert len(requirements) == 2 and len({issue["issue_id"] for issue in requirements}) == 2
            output["issues"] = deepcopy(requirements)
        return output
    engine.demo._produce_turn = model
    state = {"research_id": rid, "line_id": "line-01", "cycle_id": "cycle",
        "direction_epoch": 1, "task_id": "root", "revision": 1}
    result = await engine.design(state)
    spec = engine.spec({**state, **result}).model_dump()
    assert {issue["description"] for issue in spec["issues"]} == {"Measure F1", "Measure runtime"}
    with pytest.raises(ValueError, match="retain execution requirement"):
        engine.validate_output(rid, "critic", ExperimentSpec, {**spec, "issues": spec["issues"][:1]},
            {"execution_requirements": spec["issues"]})
    metrics = tmp_path / "metrics.json"
    metrics.write_text('{"f1":0.8,"runtime_seconds":3}', encoding="utf-8")
    artifact = store.register_artifact(rid, metrics, line_id="line-01")
    report = {"content_markdown": "Measured both requirements", "passed": True, "scientific_observation": "improvement",
        "evidence": [], "limitations": [], "execution_checks": [{"issue_id": issue["issue_id"], "passed": True,
            "evidence": [{"source_id": artifact["id"], "claim": issue["description"], "status": "observed"}]} for issue in spec["issues"]]}
    context = {"submission": {"spec": spec, "artifacts": [artifact], "run_ids": []}}
    with pytest.raises(ValueError, match="cannot pass"):
        engine.validate_output(rid, "line-01/verify", LegacyVerification, {**report, "execution_checks": report["execution_checks"][:1]}, context)
    assert engine.validate_output(rid, "line-01/verify", LegacyVerification, report, context)["passed"]
    frozen = store.get("spec", result["spec_id"])
    async def forbidden(*args, **kwargs):
        pytest.fail("recovery must reuse the already frozen spec")
    engine.discussions.run = engine.turn = forbidden
    assert await engine.design(state) == result
    assert store.get("spec", result["spec_id"]) == frozen
