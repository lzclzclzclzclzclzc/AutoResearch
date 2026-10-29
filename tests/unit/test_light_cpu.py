import pytest
from backend.runtime.light_cpu import validate_numeric_code


@pytest.mark.parametrize("code", [
    "import os; os.system('echo allowed')", "import socket", "import torch",
    "result = open('data.txt').read()", "result = (1).__class__.__mro__",
    "import math; result = getattr(math, 'sin')(0)", "exec('result=1')",
    "import math; math.sin = print", "import json; result=json.__dict__",
    "import numpy as np; import pandas as pd; result=pd.DataFrame(np.eye(2))",
    "class Sample:\n    def value(self):\n        import os\n        return os.getcwd()\nresult=Sample().value()",
])
def test_python_capabilities_are_not_restricted(code):
    tree, _, _ = validate_numeric_code(code)
    compile(tree, '<test>', 'exec')


@pytest.mark.parametrize("code", ["result =", "return 1", "break"])
def test_invalid_python_is_rejected(code):
    with pytest.raises(SyntaxError):
        validate_numeric_code(code)


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


def test_workspace_rule_covers_all_compute_roles_without_code_whitelist():
    from packages.prompts.roles import role_prompt
    from packages.contracts.research import Role
    for role in (Role.EXPLORATION, Role.CRITIC, Role.IMPLEMENTATION, Role.VERIFY):
        prompt = role_prompt(role)
        assert '工作区约束适用于所有角色' in prompt
        assert '用户提供的只读数据目录' in prompt
        assert 'workspace_dir' in prompt and 'data_dir' in prompt
        assert '不设导入、函数、属性、语法或调用白名单' in prompt
