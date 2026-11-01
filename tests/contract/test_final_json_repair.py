import pytest
from pydantic import BaseModel, StrictBool

from backend.adapters.dsh import parse_final_object
from backend.adapters.output_contract import model_contract


class Ballot(BaseModel):
    approve: StrictBool


def parse(raw, reason="completed", schema=Ballot):
    audit = []
    value = parse_final_object(raw, schema, finish_reason=reason, record_repair=audit.append)
    return value, audit


def test_single_missing_terminator_is_repaired_locally():
    result, audit = parse('{"approve":true  \n')
    assert result == {"approve": True}
    assert audit == [{"original_response": '{"approve":true  \n',
                      "method": "append_single_right_brace", "adopted": True}]
    assert parse('{"approve":true}')[1] == []
    assert parse('\n  {"approve":true')[0] == {"approve": True}


@pytest.mark.parametrize("raw", [
    '{"approve":true,"nested":{"x":1', '{"approve":true,"text":"unterminated',
    '{"approve":true,', '{"approve":true}{"approve":false', '[]',
])
def test_other_broken_json_is_not_repaired(raw):
    with pytest.raises(ValueError):
        parse(raw)


@pytest.mark.parametrize("reason", ["max-tokens", "interrupted", None])
def test_only_normally_completed_responses_are_repairable(reason):
    with pytest.raises(ValueError):
        parse('{"approve":true', reason)


def test_braces_and_escapes_in_string_do_not_affect_repair():
    assert parse('{"approve":true,"note":"} \\" {"')[0] == {"approve": True}


def test_schema_and_business_validation_remain_required():
    audit = []
    def reject(value):
        raise ValueError("direction is not allowed")
    schema = model_contract(Ballot, {}, repaired_validator=reject)
    with pytest.raises(ValueError, match="direction is not allowed"):
        parse_final_object('{"approve":true', schema, finish_reason="completed", record_repair=audit.append)
    assert audit[0]["adopted"] is False
    audit.clear()
    with pytest.raises(ValueError):
        parse_final_object('{"other":true', Ballot, finish_reason="completed", record_repair=audit.append)
    assert audit[0]["adopted"] is False
