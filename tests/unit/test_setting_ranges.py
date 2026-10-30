import pytest
from apps.api.main import Team, Research


def test_team_settings_have_no_unmeasured_maximum():
    team = Team(n=17, m1=9, m2=9, boss_members=10, k=21, experiment_concurrency=32)
    assert team.experiment_concurrency == 32
    assert Research(title='test', topic='test', experiment_concurrency=32).experiment_concurrency == 32
    with pytest.raises(ValueError): Team(experiment_concurrency=0)
