"""Acceptance of evidence derived from a specific frozen submission."""
def validate_report_facts(store, research_id, submission, result, line_id):
    """Check system-owned run facts, never parse scientific prose or citations."""
    if not result.get('passed'):
        return
    if result.get('scientific_observation') == 'execution_failed':
        raise ValueError('An execution failure cannot be marked as a passed scientific result')
    runs = [store.get('run', run_id) for run_id in submission.get('run_ids', [])]
    if not any(run and run.get('research_id') == research_id and run.get('line_id') == line_id
               and run.get('state', run.get('status')) in {'completed', 'succeeded', 'early_stopped'}
               for run in runs):
        raise ValueError('Passed verification requires a real acceptable run in this submission')


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
    if submission.get('spec', {}).get('discussion_protocol') == 2:
        return validate_current_evidence(store, research_id, submission, result, line_id)
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


def validate_current_evidence(store, research_id, submission, result, line_id):
    """New plans own acceptance; old issue identifiers impose no inherited gate."""
    allowed = submission_sources(store, research_id, submission, line_id)
    for evidence in [*result.get('evidence', []), *[e for check in result.get('execution_checks', []) for e in check.get('evidence', [])]]:
        if evidence['source_id'] not in allowed or not store.source_exists(research_id, evidence['source_id']):
            raise ValueError('Verify evidence must cite this submission or its traceable calculations')
    if result.get('passed'):
        runs = submission.get('runs') or ([submission['run']] if submission.get('run') else [])
        if not any(run.get('state', run.get('status')) in {'completed', 'succeeded', 'early_stopped'} for run in runs):
            raise ValueError('An unexecuted or failed experiment cannot become successful by removing review fields')
        if result.get('scientific_observation') == 'execution_failed':
            raise ValueError('An execution failure cannot be marked as a passed scientific result')
        if not any(e.get('status') in {'observed', 'negative'} for e in result.get('evidence', [])):
            raise ValueError('Passed verification requires observed results, including negative results')
