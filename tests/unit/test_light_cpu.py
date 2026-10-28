import pytest
from backend.runtime.light_cpu import validate_numeric_code


@pytest.mark.parametrize("code",[
    "import os; os.system('echo forbidden')",
    "import socket",
    "import torch",
    "result = open('secret.txt').read()",
    "result = (1).__class__.__mro__",
    "result = getattr(math, 'sin')",
    "exec('print(1)')",
    "import math; math.sin = print",
    "import json; result=json.__dict__",
])
def test_numeric_subset_rejects_general_python_capabilities(code):
    with pytest.raises(ValueError):
        validate_numeric_code(code)


def test_numeric_subset_accepts_evidence_aggregation():
    tree,aliases,members=validate_numeric_code("import statistics as stats\nfrom math import sqrt\nresult={'mean':stats.mean([1,2,3]),'root':sqrt(4)}")
    assert aliases=={"stats":"statistics"}
    assert members=={"sqrt":("math","sqrt")}
    assert tree


def test_scientific_json_preserves_nonfinite_lexemes_as_explicit_markers():
    import json
    from backend.runtime.scientific_json import loads,numeric_status
    value=loads('{"nan":NaN,"positive":Infinity,"negative":-Infinity,"overflow":1e400,"normal":1.25}')
    assert value["nan"]=={"value":"NaN","numeric_status":"non_finite"}
    assert value["positive"]["value"]=="Infinity"
    assert value["negative"]["value"]=="-Infinity"
    assert value["overflow"]["value"]=="1e400"
    assert value["normal"]==1.25
    assert numeric_status(value)=="non_finite"
    json.dumps(value,allow_nan=False)
