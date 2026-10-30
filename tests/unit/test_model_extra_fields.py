import pytest
from packages.contracts.research import Review, BossDecision


def test_valid_model_content_survives_extra_explanation_fields():
    assert Review.model_validate({'content_markdown': 'ready', 'ready_for_vote': True, 'confidence': .95}).ready_for_vote
    assert BossDecision.model_validate({'content_markdown': 'continue', 'action': 'continue', 'rationale': 'valid', 'note': 'extra'}).action == 'continue'
    with pytest.raises(ValueError): Review.model_validate({'content_markdown': 'missing decision', 'confidence': .95})
