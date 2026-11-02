"""Preserve member and group identity when legacy requirements are combined."""
from collections import Counter

from packages.contracts.research import Issue, digest


def merge_execution_requirements(sourced_issues):
    requirements = [(owner, Issue.model_validate({key: value for key, value in issue.items()
        if key != "raised_by"}).model_dump()) for owner, issue in sourced_issues
        if issue.get("acceptance_stage") == "execution"]
    counts = Counter(issue["issue_id"] for _, issue in requirements)
    used = set(counts)
    result = []
    for owner, issue in requirements:
        if counts[issue["issue_id"]] > 1:
            # Unambiguous historical IDs stay unchanged. Collisions receive a
            # stable source suffix before they enter the frozen Verify contract.
            qualified = issue["issue_id"] + "::" + digest([owner, issue["issue_id"]])[:16]
            while qualified in used:
                qualified += ":"
            issue["issue_id"] = qualified
            used.add(qualified)
        result.append(issue)
    return result
