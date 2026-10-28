"""Strict JSON projections of scientific files without erasing invalid numbers.

Raw evidence remains immutable. Non-finite numeric lexemes are explicit objects
in API/event projections, so browsers never receive nonstandard NaN tokens.
"""
from __future__ import annotations

import json
import math
from typing import Any


def _non_finite(value:str)->dict:
    return {"value":value,"numeric_status":"non_finite"}


def _float(value:str)->float|dict:
    number=float(value)
    return number if math.isfinite(number) else _non_finite(value)


def loads(text:str)->Any:
    return json.loads(text,parse_constant=_non_finite,parse_float=_float)


def numeric_status(value:Any)->str:
    if isinstance(value,dict):
        if value.get("numeric_status")=="non_finite":
            return "non_finite"
        values=value.values()
    elif isinstance(value,list):
        values=value
    else:
        return "finite"
    return "non_finite" if any(numeric_status(item)=="non_finite" for item in values) else "finite"
