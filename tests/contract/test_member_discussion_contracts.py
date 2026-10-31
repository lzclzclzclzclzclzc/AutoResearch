import pytest
from pydantic import ValidationError

from backend.adapters.output_contract import model_contract
from packages.contracts.research import (
    BossOperation, DiscussionFinal, DiscussionTurn, EndBallot, ExperimentSpec,
    PollBallot, Role, SharedKnowledge, allowed_next_steps,
)
from packages.prompts.roles import discussion_prompt, god_final_prompt, role_prompt


def test_minimal_turns_and_member_defined_poll_are_independent_of_candidates():
    assert DiscussionTurn.model_validate({"content_markdown": "完整讨论意见", "confidence": 0.5}).ordinary_vote is None
    turn = DiscussionTurn.model_validate({"content_markdown": "讨论两种方法", "poll": {
        "question": "采用哪种划分？", "options": ["按组划分", "按时间划分", "两种对照"]}})
    assert turn.model_dump()["ordinary_vote"]["options"] == ["按组划分", "按时间划分", "两种对照"]
    assert PollBallot.model_validate({"option": "按时间划分", "reason": "检查未来泛化", "member_id": "invented"}).model_dump() == {
        "option": "按时间划分", "reason": "检查未来泛化"}


def test_one_turn_cannot_open_two_votes_and_poll_has_distinct_options():
    with pytest.raises(ValidationError):
        DiscussionTurn.model_validate({"content_markdown": "讨论", "ordinary_vote": {
            "question": "选择", "options": ["a", "b"]}, "end_vote": {"final_output": {"content_markdown": "完整方案"}}})
    with pytest.raises(ValidationError):
        DiscussionTurn.model_validate({"content_markdown": "讨论", "ordinary_vote": {
            "question": "选择", "options": ["a", "a"]}})


def test_end_ballot_ignores_accidental_explanations_and_cannot_be_a_string_vote():
    ballot = EndBallot.model_validate({"approve": False, "next_step": "exploration", "reason": "额外说明", "final_output": {"changed": True}})
    assert ballot.model_dump() == {"approve": False, "next_step": "exploration"}
    assert EndBallot.model_validate({"approve": True}).next_step is None
    with pytest.raises(ValidationError):
        EndBallot.model_validate({"approve": "false"})


def test_god_can_create_new_complete_product_without_candidate_ids():
    output = DiscussionFinal.model_validate({"next_step": "exploration", "final_output": {
        "content_markdown": "新的完整研究方案", "new_method": {"arbitrary_scientific_setting": 7}}})
    assert output.final_output["new_method"] == {"arbitrary_scientific_setting": 7}
    assert allowed_next_steps(Role.CRITIC) == ("implementation", "exploration")
    assert allowed_next_steps("exploration") == ("critic",)
    assert allowed_next_steps("boss") == ("dispatch", "finish_lines", "finish_project", "report")


def test_v2_plan_uses_only_needed_data_and_preserves_historical_opinions():
    context = {"discussion_protocol": 2, "spec_id": "s", "spec_version": 3, "deadline_at": "2030-01-01T00:00:00Z"}
    contract = model_contract(ExperimentSpec, context)
    plan = contract.model_validate({"question": "比较方法", "fixed": {"seed": 42}, "metrics": ["mse"],
        "issues": [{"issue_id": "old", "category": "must_fix", "description": "历史意见"}]})
    assert plan.discussion_protocol == 2
    assert plan.spec_id == "s" and plan.version == 3
    assert plan.issues[0].resolved is False
    assert set(contract.model_json_schema()["required"]) == {"question", "fixed", "metrics"}
    assert plan.model_dump()["discussion_protocol"] == 2
    notes = contract.model_validate({"question": "比较方法", "fixed": {}, "metrics": ["f1"], "issues": [
        "还需观察重复文本影响", {"description": "普通风险说明"},
        {"issue_id": "old", "category": "must_fix", "resolved": True, "resolution": "", "description": "旧状态不作为新门禁"}]})
    assert notes.model_dump()["issues"][0] == "还需观察重复文本影响"
    assert notes.model_dump()["issues"][1] == {"description": "普通风险说明"}


def test_boss_combination_uses_only_parameters_for_actual_effects():
    assert BossOperation.model_validate({"operation": "report"}).operation == "report"
    assert BossOperation.model_validate({"operation": "research", "target": "A"}).objective is None
    with pytest.raises(ValidationError):
        BossOperation.model_validate({"operation": "change_direction", "target": "A"})


def test_shared_knowledge_does_not_accept_model_owned_storage_identity():
    knowledge = SharedKnowledge.model_validate({"content_markdown": "一个有出处的发现", "source_ids": ["run-a"],
        "node_id": "invented", "parent_id": "invented-parent", "memory_version": 99})
    assert knowledge.status == "unverified"
    assert "node_id" not in knowledge.model_dump()
    with pytest.raises(ValidationError):
        SharedKnowledge.model_validate({"content_markdown": "发现", "source_ids": []})


def test_new_prompts_separate_finalization_from_legacy_recovery():
    prompt = discussion_prompt("critic", "end_vote")
    assert "所有成员投是且推进方向一致" in prompt
    assert "不输出理由" in prompt and "implementation" in prompt and "exploration" in prompt
    assert "ready_for_vote 不能覆盖" not in prompt
    assert "逐项用真实证据核查" not in role_prompt(Role.VERIFY, discussion_protocol=2)
    assert "request_revision 只能选择" not in god_final_prompt()
    assert "完整新方案" in god_final_prompt()
