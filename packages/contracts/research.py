from __future__ import annotations

import hashlib
import json
import math
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="before")
    @classmethod
    def finite_json_numbers(cls, value):
        def check(item):
            if isinstance(item, float) and not math.isfinite(item):
                raise ValueError("Structured decisions cannot contain NaN or Infinity")
            if isinstance(item, dict):
                for child in item.values():
                    check(child)
            elif isinstance(item, (list, tuple)):
                for child in item:
                    check(child)
        check(value)
        return value


class Role(StrEnum):
    EXPLORATION = "exploration"
    CRITIC = "critic"
    IMPLEMENTATION = "implementation"
    VERIFY = "verify"
    ORGANIZER = "organizer"
    BOSS = "boss"
    PAPER = "paper"


class Evidence(Contract):
    source_id: str
    claim: str
    status: Literal["observed", "inference", "unverified", "failed", "negative"]


class IssueReclassification(Contract):
    from_category: Literal["must_fix", "experimental_disagreement", "suggestion"]
    rationale: str = Field(min_length=1)
    evidence: list[Evidence] = Field(min_length=1)

    @model_validator(mode="after")
    def supported_review(self):
        if not self.rationale.strip() or any(not e.source_id.strip() or not e.claim.strip() or e.status == "unverified" for e in self.evidence):
            raise ValueError("Reclassification needs a reason and traceable, verified evidence")
        return self


class Issue(Contract):
    issue_id: str
    category: Literal["must_fix", "experimental_disagreement", "suggestion"]
    description: str
    resolved: bool = False
    resolution: str = ""
    experiment_mapping: str = ""
    reclassification: IssueReclassification | None = None

    @model_validator(mode="after")
    def resolution_has_evidence(self):
        if self.category == "must_fix" and self.resolved and not self.resolution.strip():
            raise ValueError("Resolving a blocking issue requires an explicit reason and evidence")
        if self.reclassification and self.reclassification.from_category == self.category:
            raise ValueError("Reclassification must name the previous, different category")
        return self


def validate_issue_transition(previous: dict | None, current: dict, source_exists=None):
    issue = Issue.model_validate(current)
    if previous and previous["category"] != issue.category:
        review = issue.reclassification
        if not review or review.from_category != previous["category"]:
            raise ValueError(f"Issue {issue.issue_id}: changing category requires a review of the previous category, a reason and evidence")
        if source_exists and any(not source_exists(e.source_id) for e in review.evidence):
            raise ValueError(f"Issue {issue.issue_id}: reclassification evidence must reference existing evidence or messages in this research")
    return issue.model_dump()


class Proposal(Contract):
    content_markdown: str
    candidate: dict[str, Any]
    evidence: list[Evidence] = Field(default_factory=list)


class MessageReply(Contract):
    content_markdown: str = Field(min_length=1)


class Review(Contract):
    content_markdown: str
    ready_for_vote: bool
    new_evidence: list[str] = Field(default_factory=list)
    issues: list[Issue] = Field(default_factory=list)
    revised_candidate: dict[str, Any] | None = None


class Willingness(Contract):
    score: int = Field(ge=0, le=100, strict=True)


class Vote(Contract):
    content_markdown: str
    candidate_id: str
    rationale: str


class Synthesis(Contract):
    content_markdown: str
    selected_candidate_id: str
    decision: dict[str, Any]
    minority_views: list[str] = Field(default_factory=list)
    issues: list[Issue] = Field(default_factory=list)


class ExperimentSpec(Contract):
    spec_id: str
    version: int = Field(ge=1)
    question: str
    hypothesis: str
    fixed: dict[str, Any]
    allowed_adjustments: dict[str, Any] = Field(default_factory=dict)
    baseline: str
    data: dict[str, Any]
    metrics: list[str] = Field(min_length=1)
    design: dict[str, Any]
    monitoring: dict[str, Any]
    completion_criteria: str
    early_stop_rules: list[dict[str, Any]] = Field(default_factory=list)
    issues: list[Issue] = Field(default_factory=list)
    deadline_at: str

    @model_validator(mode="after")
    def validate_issues(self):
        for issue in self.issues:
            if issue.category == "must_fix" and not issue.resolved:
                raise ValueError(f"Unresolved blocking issue: {issue.issue_id}")
            if issue.category == "experimental_disagreement" and not issue.experiment_mapping:
                raise ValueError(f"Disagreement lacks experiment mapping: {issue.issue_id}")
        return self


class Implementation(Contract):
    content_markdown: str
    spec_hash: str
    files: dict[str, str]
    entrypoint: str
    arguments: list[str] = Field(default_factory=list)
    actual_config: dict[str, Any]
    cpu_cores: int = Field(default=1, ge=1)
    memory_mb: int = Field(default=256, ge=64)
    gpu_count: int = Field(default=0, ge=0)
    estimated_seconds: int = Field(default=60, ge=1)
    dependencies: list[str] = Field(default_factory=list)


class Verification(Contract):
    content_markdown: str
    passed: bool
    scientific_observation: Literal["improvement", "no_improvement", "negative", "inconclusive", "execution_failed"]
    evidence: list[Evidence]
    limitations: list[str]
    required_changes: list[str] = Field(default_factory=list)
    independent_replication: Literal[False] = False


class Assignment(Contract):
    line_id: str
    objective: str
    priority: int = 0


class BossDecision(Contract):
    content_markdown: str
    action: Literal["assign", "continue", "request_evidence", "change_direction", "finish_line", "finish_project", "report"]
    assignments: list[Assignment] = Field(default_factory=list)
    memory_updates: list[dict[str, Any]] = Field(default_factory=list)
    request_report: bool = True
    rationale: str


class BossProposal(Contract):
    content_markdown: str
    candidate: BossDecision
    evidence: list[Evidence] = Field(default_factory=list)


class BossReview(Review):
    revised_candidate: BossDecision | None = None


class ResourceAdvice(Contract):
    content_markdown: str
    priority: int = 0
    reason: str


class MonitorDecision(Contract):
    content_markdown: str
    request_early_stop: bool
    rule_id: str | None = None
    rationale: str
    evidence_log_hash: str
    limitations: list[str] = Field(default_factory=list)


class RunAssessment(Contract):
    content_markdown: str
    action: Literal["package", "repair_implementation", "request_spec_revision"]
    rationale: str
    proposed_changes: list[str] = Field(default_factory=list)


class StageReport(Contract):
    title: str
    content_markdown: str
    evidence_ids: list[str]
    limitations: list[str]


def assert_spec_unchanged(spec: ExperimentSpec, implementation: Implementation) -> None:
    if implementation.spec_hash != digest(spec.model_dump()):
        raise ValueError("spec_revision_required: implementation changed the frozen plan")
    if implementation.actual_config != spec.fixed:
        raise ValueError("spec_revision_required: actual scientific configuration differs from frozen fixed fields")


def majority(votes: dict[str, str], members: list[str], candidates: set[str]) -> str | None:
    """Absent members never reduce the required majority."""
    if set(votes) - set(members):
        raise ValueError("An unregistered member voted")
    if any(candidate not in candidates for candidate in votes.values()):
        raise ValueError("Unknown candidate")
    threshold = len(members) // 2 + 1
    counts = {candidate: sum(v == candidate for v in votes.values()) for candidate in candidates}
    return next((candidate for candidate, count in counts.items() if count >= threshold), None)
