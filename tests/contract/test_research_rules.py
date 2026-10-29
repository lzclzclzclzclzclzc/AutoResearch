from copy import deepcopy

import pytest
from pydantic import ValidationError

from packages.contracts.research import ExperimentSpec, Implementation, Vote, assert_spec_unchanged, digest, majority, validate_vote


def spec():
    return ExperimentSpec(spec_id="spec-a", version=1, question="q", hypothesis="h",
        fixed={"seed": 42, "epochs": 10}, baseline="zero", data={"source": "synthetic"}, metrics=["mse"],
        design={"repetitions": 1}, monitoring={"signal": "mse"}, completion_criteria="ten epochs", deadline_at="2030-01-01T00:00:00Z")


def test_absent_members_do_not_reduce_majority():
    assert majority({"a": "a"}, ["a", "b", "c"], {"a", "b", "c"}) is None
    assert majority({"a": "a", "b": "a"}, ["a", "b", "c"], {"a", "b", "c"}) == "a"
    with pytest.raises(ValueError):
        majority({"outsider": "a"}, ["a", "b"], {"a", "b"})


@pytest.mark.parametrize("candidate", ["missing", "b", "other-group/a", ""])
def test_schema_valid_votes_still_need_a_current_group_candidate(candidate):
    vote = Vote(content_markdown="my vote", candidate_id=candidate, rationale="reason").model_dump()
    with pytest.raises(ValueError, match="current candidate.*allowed IDs: a"):
        validate_vote(vote, "a", ["a", "b"], {"a", "other-group/a"})


def test_vote_validation_keeps_authorship_and_allows_a_valid_minority_vote():
    vote = Vote(content_markdown="my vote", candidate_id="b", rationale="reason").model_dump()
    validate_vote(vote, "a", ["a", "b"], {"a", "b"})
    with pytest.raises(ValueError, match="unregistered member"):
        validate_vote(vote, "outsider", ["a", "b"], {"a", "b"})


def test_unresolved_blocker_and_unmapped_disagreement_cannot_freeze():
    original = spec().model_dump()
    for category in ("must_fix", "experimental_disagreement"):
        value = deepcopy(original)
        value["issues"] = [{"issue_id": "i", "category": category, "description": "missing evidence"}]
        with pytest.raises(ValidationError):
            ExperimentSpec.model_validate(value)


def test_implementation_cannot_silently_change_scientific_configuration():
    plan = spec()
    implementation = Implementation(content_markdown="code", spec_hash=digest(plan.model_dump()), files={"a.py": "pass"}, entrypoint="a.py", actual_config=plan.fixed)
    assert_spec_unchanged(plan, implementation)
    implementation.actual_config = {"seed": 42, "epochs": 5}
    with pytest.raises(ValueError, match="spec_revision_required"):
        assert_spec_unchanged(plan, implementation)


def test_direction_context_and_attempt_keys_are_project_scoped():
    assert digest(["research-a", "line-01/verify", 1, "1"]) != digest(["research-b", "line-01/verify", 1, "1"])
    assert digest(["research-a", "line-01/verify", 1, "1"]) != digest(["research-a", "line-01/verify", 2, "1"])
