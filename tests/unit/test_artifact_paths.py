from backend.persistence.store import Store


def test_long_run_log_name_keeps_short_content_path_and_original_download_name(tmp_path):
    root = tmp_path / ('x' * max(1, 150 - len(str(tmp_path)) - 1))
    store = Store(root)
    project = store.create_project({'topic': 'long log', 'client_request_id': 'long-log'})
    source = root / ('a' * 64 + '.log')
    source.write_text('immutable experiment output', encoding='utf-8')
    artifact = store.register_artifact(project['id'], source, 'text/plain')
    metadata, backing = store.artifact(artifact['id'])
    assert len(str(root / 'artifacts' / artifact['sha256'][:2] / artifact['sha256'][2:] / source.name)) >= 260
    assert len(str(backing)) < 250
    assert metadata['name'] == source.name
    assert backing.read_text(encoding='utf-8') == 'immutable experiment output'
    assert store.register_artifact(project['id'], source, 'text/plain') == artifact
