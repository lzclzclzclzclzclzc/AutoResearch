"""Acceptance of evidence derived from a specific frozen submission."""
def submission_sources(store, research_id, submission, line_id):
    allowed = set(submission.get('run_ids', [])) | {
        item.get('artifact_id', item.get('id')) for item in submission.get('artifacts', [])}
    allowed.discard(None)
    artifacts = store.list('artifact', research_id)
    while True:
        extra = {item['id'] for item in artifacts if item.get('line_id') == line_id
                 and item.get('kind') == 'light_cpu_result' and item.get('source_evidence_ids')
                 and set(item['source_evidence_ids']) <= allowed}
        if extra <= allowed: return allowed
        allowed.update(extra)


def validate_execution_checks(store, research_id, submission, result, line_id):
    required = {item['issue_id'] for item in submission.get('spec', {}).get('issues', [])
                if item.get('acceptance_stage') == 'execution' and item['category'] == 'must_fix' and not item['resolved']}
    checks = result.get('execution_checks', [])
    if len({check['issue_id'] for check in checks}) != len(checks):
        raise ValueError('Verify execution_checks must not repeat an issue_id')
    allowed = submission_sources(store, research_id, submission, line_id)
    for check in checks:
        if check['issue_id'] not in required:
            raise ValueError('Verify execution_checks must reference a frozen execution requirement')
        if check['passed'] and any(e['status'] not in {'observed', 'negative'} for e in check['evidence']):
            raise ValueError('Passed execution checks require observed evidence, including observed negative results')
        if any(e['source_id'] not in allowed or not store.source_exists(research_id, e['source_id']) for e in check['evidence']):
            raise ValueError('Verify execution_checks must cite actual runs or artifacts from the submission or its traceable calculations')
    satisfied = {check['issue_id'] for check in checks if check['passed']}
    if result['passed'] and required - satisfied:
        raise ValueError('Verify cannot pass before execution requirements are evidenced: '+', '.join(sorted(required-satisfied)))
