from types import SimpleNamespace

import pytest

from backend.graphs.discussion import GroupDiscussionGraph
from backend.persistence.store import Store
from packages.contracts.research import Issue, validate_issue_transition


@pytest.fixture
def issue_history(tmp_path):
    store = Store(tmp_path)
    rid = store.create_project({"client_request_id": "issues", "topic": "fixture", "mode": "demo"})["id"]
    source = store.create_message(rid, "review-evidence", "boss/member-02", "数据已确认不含受试者分组，只需要逐样本独立划分。")
    original = Issue(issue_id="split-leakage", category="must_fix", description="避免组间泄漏").model_dump()
    review = {"from_category": "must_fix", "rationale": "实际数据不存在受试者分组，该要求可降为建议。",
        "evidence": [{"source_id": source["message_id"], "claim": "数据审计说明无分组字段", "status": "observed"}]}
    graph = GroupDiscussionGraph.__new__(GroupDiscussionGraph)
    graph.engine = SimpleNamespace(store=store)
    return store, rid, original, review, graph


@pytest.mark.parametrize("invalid", ["missing", "blank_reason", "no_evidence", "unverified", "wrong_from", "fake_source"])
def test_invalid_reclassification_cannot_remove_original_blocker(issue_history, invalid):
    store, rid, original, review, graph = issue_history
    replacement = {**original, "category": "suggestion", "reclassification": review}
    if invalid == "missing":
        replacement["reclassification"] = None
    elif invalid == "blank_reason":
        review["rationale"] = "  "
    elif invalid == "no_evidence":
        review["evidence"] = []
    elif invalid == "unverified":
        review["evidence"][0]["status"] = "unverified"
    elif invalid == "wrong_from":
        review["from_category"] = "experimental_disagreement"
    else:
        review["evidence"][0]["source_id"] = "invented-artifact"
    with pytest.raises(ValueError):
        validate_issue_transition(original, replacement, lambda source: store.source_exists(rid, source))
    state = {"research_id": rid, "records": {"review:1:boss/member-01": {"issues": [original]},
        "review:2:boss/member-01": {"issues": [replacement]}}}
    assert graph.blockers(state)[0]["issue_id"] == original["issue_id"]
    assert graph.blockers(state)[0]["category"] == "must_fix"


def test_supported_reclassification_preserves_the_review_in_history(issue_history):
    store, rid, original, review, graph = issue_history
    replacement = {**original, "category": "suggestion", "reclassification": review}
    validate_issue_transition(original, replacement, lambda source: store.source_exists(rid, source))
    state = {"research_id": rid, "records": {"review:1:boss/member-01": {"issues": [original]},
        "review:2:boss/member-01": {"issues": [replacement]}}}
    assert not graph.blockers(state)
    assert graph.issues(state)[0]["reclassification"] == review
    assert state["records"]["review:1:boss/member-01"]["issues"][0]["category"] == "must_fix"


def test_another_members_downgrade_cannot_erase_the_owners_blocker(issue_history):
    _, rid, original, _, graph = issue_history
    state = {"research_id": rid, "records": {"review:1:boss/member-01": {"issues": [original]},
        "review:2:boss/member-02": {"issues": [{**original, "category": "suggestion"}]}}}
    assert len(graph.blockers(state)) == 1
