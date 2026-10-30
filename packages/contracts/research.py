from __future__ import annotations

import hashlib
import json
import math
from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, RootModel, model_validator


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()


class Contract(BaseModel):
    model_config = ConfigDict(extra="ignore")

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
    GOD = "god"
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
    model_config = ConfigDict(extra="ignore")
    content_markdown: str = ""
    spec_hash: str = ""
    files: dict[str, str]
    entrypoint: str
    arguments: list[str] = Field(default_factory=list)
    actual_config: dict[str, Any] | None = None
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


ArbitrationAction = Literal["select_candidate", "repair_output", "request_revision", "request_evidence",
                            "decompose_task", "finish_scope", "needs_external_input"]
ArbitrationReason = Literal["decision_blocked", "design_deadlock", "invalid_group_decision",
                            "invalid_boss_assignment", "invalid_output", "model_output_truncated"]


class ArbitrationRequest(Contract):
    """Program-owned recovery identity. Model output cannot change these boundaries."""
    model_config = ConfigDict(extra="forbid", strict=True)
    arbitration_id: str
    schema_version: int = 1
    policy_version: int = 1
    research_id: str
    source_task_id: str
    scope_kind: Literal["discussion", "implementation", "boss_assignment", "turn_output", "decomposition"]
    scope_id: str
    origin_node: str
    origin_checkpoint: str | None = None
    reason_code: ArbitrationReason
    failure_fingerprint: str
    scope_revision: str
    direction_epoch: int | None = None
    control_generation: int
    snapshot_id: str
    snapshot_hash: str
    source_event_seq: int = 0
    target_schema_name: str
    target_schema_hash: str
    allowed_actions: list[ArbitrationAction] = Field(min_length=1)
    resume_point: Literal["discussion.collect", "discussion.synthesize", "discussion.apply_resolution",
                          "boss.validate_assignment", "line.implement", "engine.validate_output", "decomposition.merge"]
    provider_config_id: str
    credential_version: int
    model: str
    recovery_chain_id: str
    created_at: str
    judge_deadline_at: str
    expires_at: str


class DecompositionPart(Contract):
    model_config = ConfigDict(extra="ignore", strict=True)
    part_id: str = Field(min_length=1)
    target_role: str
    instructions: str = Field(min_length=1)
    input_refs: list[str] = Field(default_factory=list)
    output_schema: dict[str, Any]
    allowed_write_paths: list[str] = Field(min_length=1)
    depends_on: list[str] = Field(default_factory=list)


class MergePlan(Contract):
    strategy: Literal["json_fields", "markdown_sections", "files", "file_fragments"]
    order: list[str] = Field(min_length=1)
    base_output: dict[str, Any] = Field(default_factory=dict)


class DecompositionPlan(Contract):
    model_config = ConfigDict(extra="ignore", strict=True)
    target_schema_hash: str
    parts: list[DecompositionPart] = Field(min_length=1, max_length=8)
    merge_plan: MergePlan
    invariants: dict[str, Any] = Field(default_factory=dict)
    acceptance_checks: list[str] = Field(min_length=1)

    @model_validator(mode="after")
    def bounded_dag(self):
        parts = {p.part_id: p for p in self.parts}
        if len(parts) != len(self.parts):
            raise ValueError("Decomposition part IDs must be unique")
        if len(self.merge_plan.order) != len(parts) or set(self.merge_plan.order) != set(parts):
            raise ValueError("Merge order must include every part exactly once")
        visiting, depths = set(), {}
        def depth(identity):
            if identity not in parts:
                raise ValueError("Unknown decomposition dependency")
            if identity in visiting:
                raise ValueError("Decomposition dependencies must be acyclic")
            if identity in depths:
                return depths[identity]
            visiting.add(identity)
            value = 1 + max((depth(other) for other in parts[identity].depends_on), default=0)
            visiting.remove(identity)
            if value > 3:
                raise ValueError("Decomposition dependency depth exceeds 3")
            depths[identity] = value
            return value
        for part in self.parts:
            if len(set(part.depends_on)) != len(part.depends_on):
                raise ValueError("Repeated dependency")
            if part.target_role not in {r.value for r in Role if r != Role.GOD}:
                raise ValueError("Decomposition must use an existing research role")
            depth(part.part_id)
            for path in part.allowed_write_paths:
                if not path.startswith("/") or ".." in path.split("/") or "\\" in path:
                    raise ValueError("Write positions must be bounded JSON Pointers")
                if path.split("/")[1] in {"research_id", "control_generation", "direction_epoch", "spec_id", "spec_hash", "fixed", "deadline_at"}:
                    raise ValueError("Decomposition cannot overwrite frozen identity or settings")
        owners = {}
        for part in self.parts:
            for path in part.allowed_write_paths:
                if path in owners:
                    raise ValueError("Each output position must have a unique owner")
                if any(path.startswith(p + "/") or p.startswith(path + "/") for p in owners):
                    raise ValueError("Overlapping output positions are not allowed")
                owners[path] = part.part_id
        return self


class GodDecisionBase(Contract):
    model_config = ConfigDict(extra="ignore", strict=True)
    arbitration_id: str
    snapshot_hash: str
    content_markdown: str = Field(min_length=1)
    rationale: str = Field(min_length=1)
    evidence_refs: list[str] = Field(default_factory=list)
    limitations: list[str] = Field(default_factory=list)
    unresolved_issue_ids: list[str] = Field(default_factory=list)


class SelectCandidateDecision(GodDecisionBase):
    action: Literal["select_candidate"]
    selected_candidate_id: str
    minority_views: list[str] = Field(default_factory=list)


class RepairOutputDecision(GodDecisionBase):
    action: Literal["repair_output"]
    target_schema_hash: str
    output: dict[str, Any]
    source_fields: list[str] = Field(default_factory=list)


class RequestRevisionDecision(GodDecisionBase):
    action: Literal["request_revision"]
    target_role: str
    instructions: str = Field(min_length=1)
    allowed_changes: list[str] = Field(min_length=1)
    acceptance_checks: list[str] = Field(min_length=1)


class RequestEvidenceDecision(GodDecisionBase):
    action: Literal["request_evidence"]
    target_role: str
    question: str = Field(min_length=1)
    source_refs: list[str] = Field(default_factory=list)
    steps: list[str] = Field(min_length=1)
    result_requirements: list[str] = Field(min_length=1)
    dependency_line_ids: list[str] = Field(default_factory=list)


class DecomposeTaskDecision(GodDecisionBase):
    action: Literal["decompose_task"]
    plan: DecompositionPlan


class FinishScopeDecision(GodDecisionBase):
    action: Literal["finish_scope"]
    line_id: str | None = None
    existing_result_refs: list[str] = Field(default_factory=list)


class NeedsExternalInputDecision(GodDecisionBase):
    action: Literal["needs_external_input"]
    missing_items: list[str] = Field(min_length=1)
    why_unavailable: str = Field(min_length=1)
    operation: str = Field(min_length=1)
    dependency_line_ids: list[str] = Field(default_factory=list)


class GodDecision(RootModel[Annotated[SelectCandidateDecision | RepairOutputDecision | RequestRevisionDecision |
        RequestEvidenceDecision | DecomposeTaskDecision | FinishScopeDecision | NeedsExternalInputDecision,
        Field(discriminator="action")]]):
    """The serialized representation is the flat, discriminated action object."""
    pass


def assert_spec_unchanged(spec: ExperimentSpec, implementation: Implementation) -> None:
    if implementation.actual_config is not None and implementation.actual_config != spec.fixed:
        raise ValueError("spec_revision_required: actual scientific configuration differs from frozen fixed fields")


def validate_vote(vote: dict, voter_id: str, members: list[str], candidates: set[str]) -> None:
    if voter_id not in members:
        raise ValueError("An unregistered member voted")
    eligible = candidates.intersection(members)
    if vote["candidate_id"] not in eligible:
        raise ValueError("Vote candidate_id must name a current candidate in this group; allowed IDs: "
                         + ", ".join(sorted(eligible)))


def majority(votes: dict[str, str], members: list[str], candidates: set[str]) -> str | None:
    """Absent members never reduce the required majority."""
    if set(votes) - set(members):
        raise ValueError("An unregistered member voted")
    if any(candidate not in candidates for candidate in votes.values()):
        raise ValueError("Unknown candidate")
    threshold = len(members) // 2 + 1
    counts = {candidate: sum(v == candidate for v in votes.values()) for candidate in candidates}
    return next((candidate for candidate, count in counts.items() if count >= threshold), None)
