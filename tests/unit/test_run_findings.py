"""Regression coverage for the real Disaster-Tweets5 failure mechanisms."""
import builtins
from types import SimpleNamespace

import pytest

from backend.graphs.discussion import GroupDiscussionGraph
from backend.graphs.engine import ResearchEngine
from backend.persistence.store import Store
from backend.runtime.light_cpu import validate_numeric_code
from packages.contracts.research import ExperimentSpec, Issue, LegacyVerification as Verification, validate_issue_transition


def execution_issue():
    return Issue(issue_id="baseline-metrics", category="must_fix", description="缺少实测基线",
        acceptance_stage="execution", experiment_mapping="Runner 产生逐折 F1 与 std，Verify 核查同折计算").model_dump()


def plan(issues):
    return dict(spec_id="s", version=1, question="q", hypothesis="h", fixed={}, baseline="b", data={},
        metrics=["f1"], design={}, monitoring={}, completion_criteria="observed metrics", issues=issues,
        deadline_at="2030-01-01T00:00:00Z")


def test_runtime_results_do_not_deadlock_design_and_cannot_be_silently_moved(tmp_path):
    store = Store(tmp_path)
    rid = store.create_project({"topic": "stages", "client_request_id": "stages"})["id"]
    original = {**execution_issue(), "acceptance_stage": "design"}
    with pytest.raises(ValueError, match="stage_review"):
        validate_issue_transition(original, execution_issue())
    source = store.create_message(rid, "design", "line-01/critic/member-01", "基线统计由 Runner 实跑后回填，设计协议已闭合")
    reviewed = {**execution_issue(), "stage_review": {"from_stage": "design", "rationale": "该条件属于运行后的验收",
        "evidence": [{"source_id": source["message_id"], "claim": "已有审查指出协议已完整，数字需运行产生", "status": "observed"}]}}
    validate_issue_transition(original, reviewed, lambda identity: store.source_exists(rid, identity))
    graph = GroupDiscussionGraph.__new__(GroupDiscussionGraph)
    graph.engine = SimpleNamespace(store=store)
    state = {"research_id": rid, "records": {"review:1:member": {"issues": [original]}, "review:2:member": {"issues": [reviewed]}}}
    assert not graph.blockers(state)
    assert graph.issues(state)[0]["resolved"] is False
    assert ExperimentSpec.model_validate(plan([reviewed])).issues[0].acceptance_stage == "execution"
    with pytest.raises(ValueError):
        ExperimentSpec.model_validate(plan([original]))
    reviewed["stage_review"]["evidence"][0]["source_id"] = "invented"
    state["records"]["review:2:member"]["issues"] = [reviewed]
    assert graph.blockers(state)[0].get("acceptance_stage", "design") == "design"


def test_default_stage_and_empty_verify_checks_preserve_existing_frozen_hashes():
    from packages.contracts.research import digest
    legacy = {"issue_id": "legacy", "category": "must_fix", "description": "design rule", "resolved": True,
        "resolution": "protocol fixed", "experiment_mapping": "", "reclassification": None}
    assert Issue.model_validate(legacy).model_dump() == legacy
    frozen = plan([legacy])
    # Existing fields still receive their historical defaults, only new fields must stay absent.
    assert digest(ExperimentSpec.model_validate(frozen).model_dump()["issues"]) == digest(frozen["issues"])
    report = Verification(content_markdown="old report", passed=True, scientific_observation="negative", evidence=[], limitations=[])
    assert "execution_checks" not in report.model_dump()


def test_explicit_retry_of_exhausted_arbitration_refreshes_the_scene(tmp_path):
    store = Store(tmp_path)
    project = store.start_project(store.create_project({"topic": "retry", "client_request_id": "retry"})["id"])
    task = store.add_task(project["id"], "line", {"line_id": "line-01"}, "failed-line")
    store.put("arbitration_request", "old-arbitration", {"status": "exhausted"}, project["id"])
    store.finish_task(task["id"], error={"code": "arbitration_exhausted", "message": "old scene"})
    store.retry_failed_tasks(project["id"])
    refreshed = store.get("task", task["id"])
    assert refreshed["status"] == "pending" and refreshed["recovery_generation"] == 1
    assert store.get("arbitration_request", "old-arbitration")["status"] == "exhausted"
    assert store.events(project["id"])[-1]["type"] == "task.arbitration_retry"


def test_execution_requirements_survive_freezing_and_need_real_verify_evidence(tmp_path):
    store = Store(tmp_path)
    rid = store.create_project({"topic": "verify", "client_request_id": "verify"})["id"]
    engine = ResearchEngine.__new__(ResearchEngine)
    engine.store = store
    issue = execution_issue()
    with pytest.raises(ValueError, match="retain execution requirement"):
        engine.validate_output(rid, "critic", ExperimentSpec, plan([]), {"execution_requirements": [issue]})
    engine.validate_output(rid, "critic", ExperimentSpec, plan([issue]), {"execution_requirements": [issue]})
    report = dict(content_markdown="verified", passed=True, scientific_observation="negative", evidence=[], limitations=[])
    context = {"submission": {"spec": plan([issue]), "run_ids": [], "artifacts": []}}
    with pytest.raises(ValueError, match="cannot pass"):
        engine.validate_output(rid, "verify", Verification, report, context)
    file = tmp_path / "metrics.json"
    file.write_text('{"fold_f1":[0.8,0.79]}', encoding="utf-8")
    artifact = store.register_artifact(rid, file, "application/json")
    context["submission"]["artifacts"] = [artifact]
    report["execution_checks"] = [{"issue_id": issue["issue_id"], "passed": True,
        "evidence": [{"source_id": artifact["id"], "claim": "同折 F1 已实测", "status": "observed"}]}]
    assert engine.validate_output(rid, "verify", Verification, report, context)["passed"]
    report["execution_checks"][0]["evidence"][0]["source_id"] = "invented"
    with pytest.raises(ValueError, match="actual runs or artifacts"):
        engine.validate_output(rid, "verify", Verification, report, context)
    report["passed"] = False
    report["execution_checks"][0]["evidence"][0].update(source_id=artifact["id"], status="unverified")
    with pytest.raises(ValueError, match="observed evidence"):
        engine.validate_output(rid, "verify", Verification, report, context)


def test_csv_and_text_analysis_use_normal_python():
    code = """import csv, io, re, hashlib
rows = list(csv.DictReader(io.StringIO(evidence['input']['data'])))
counts = {}
for row in rows:
    label = row['target'].strip()
    counts[label] = counts.get(label, 0) + 1
result = {'counts': counts, 'ids': [re.match(r'^(\\d+)', r['id']).group(1) for r in rows],
          'digest': hashlib.sha256('observed'.encode('utf-8')).hexdigest(), 'type': str(type(rows))}
"""
    tree, aliases, members = validate_numeric_code(code)
    scope = {"__builtins__": builtins.__dict__, "evidence": {"input": {"data": "id,target\n1,1\n2,0\n3,1\n"}}}
    exec(compile(tree, "<analysis-test>", "exec"), scope, scope)
    assert scope["result"]["counts"] == {"1": 2, "0": 1}
    assert scope["result"]["ids"] == ["1", "2", "3"]
    with pytest.raises(SyntaxError):
        validate_numeric_code("result =")
