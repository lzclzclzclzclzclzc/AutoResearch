"""Freeze only existing evidence explicitly referenced by a Boss report input."""
import re


def report_artifacts(frozen, lookup, research_id):
    ids = set()
    source_keys = {"artifact_id", "artifact_ids", "evidence_ids", "source", "source_id", "source_ids", "source_refs", "evidence_refs"}
    def collect(value, references=False):
        if isinstance(value, dict):
            for key, child in value.items():
                collect(child, references or key in source_keys)
            if isinstance(value.get("id"), str) and value["id"].startswith("artifact-"):
                ids.add(value["id"])
        elif isinstance(value, list):
            for child in value:
                collect(child, references)
        elif references and isinstance(value, str):
            ids.update(re.findall(r"\bartifact-[a-zA-Z0-9_-]+", value))
    collect(frozen)
    result = []
    for identity in sorted(ids):
        artifact = lookup(identity)
        if artifact and artifact.get("research_id") == research_id:
            result.append({**artifact, "artifact_id": identity})
    return result
