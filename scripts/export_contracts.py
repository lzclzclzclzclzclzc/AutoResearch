"""Export the exact validated Agent contracts for external consumers."""
from pathlib import Path
import inspect
import json
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pydantic import BaseModel
from packages.contracts import research

destination = Path(__file__).resolve().parents[1] / "packages" / "contracts" / "schemas"
destination.mkdir(parents=True, exist_ok=True)
for name, contract in inspect.getmembers(research, inspect.isclass):
    if contract.__module__ == research.__name__ and issubclass(contract, BaseModel) and name != "Contract":
        schema = {"$schema": "https://json-schema.org/draft/2020-12/schema", **contract.model_json_schema()}
        (destination / f"{name}.schema.json").write_text(json.dumps(schema, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
print(f"Exported {len(list(destination.glob('*.json')))} contracts")
