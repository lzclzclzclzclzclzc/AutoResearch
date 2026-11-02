from copy import deepcopy
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from apps.api.main import create_app
from backend.services.credentials import CredentialService


@pytest.fixture
def binding_api(tmp_path, monkeypatch):
    app = create_app(tmp_path, control_client=SimpleNamespace(allows_progress=lambda *args: True), testing=True)
    secrets = {"deepseek-default": "sk-SYNTHETIC-OLD-REBIND-FIXTURE"}
    credential_reads = []

    def get_secret(service, provider_id="deepseek-default", version=None):
        credential_reads.append((provider_id, version))
        provider = service.store.get("provider", provider_id)
        assert version is None or version == provider["credential_version"]
        return secrets.get(provider_id)

    # Test only binding policy; the existing DPAPI integration test exercises
    # real encryption/decryption. No provider address is ever contacted.
    monkeypatch.setattr(CredentialService, "get_secret", get_secret)
    with TestClient(app) as client:
        client.headers["X-CSRF-Token"] = client.get("/api/session").json()["csrf_token"]
        assert client.get("/api/settings/providers").status_code == 200
        app.state.store.patch("provider", "deepseek-default", credential_version=1,
            base_url="https://old.example.invalid/anthropic", model="old-model")
        yield client, app.state.store, secrets, credential_reads
    app.state.control_executor.shutdown(wait=True)


def create_project(client):
    response = client.post("/api/research", json={
        "client_request_id": "rebind-project", "title": "Credential rebinding",
        "topic": "Isolated binding fixture", "mode": "demo",
    })
    assert response.status_code == 201, response.text
    return response.json()["id"]


def rebind(client, research_id, **fields):
    return client.post(f"/api/research/{research_id}/commands", json={
        "kind": "rebind_credential", "client_request_id": "rebind-command", **fields,
    })


def assert_rejected_unchanged(client, store, research_id, before, **fields):
    response = rebind(client, research_id, **fields)
    assert response.status_code == 409, response.text
    assert response.json()["code"] == "provider_binding_mismatch"
    assert store.project(research_id) == before
    assert store.get("command", "rebind-command") is None
    assert not [event for event in store.events(research_id) if event["type"] == "credential.rebound"]


@pytest.mark.parametrize("different_transport", [False, True])
def test_rebind_rejects_another_provider_without_reading_its_key(binding_api, different_transport):
    client, store, secrets, reads = binding_api
    research_id = create_project(client)
    before = store.project(research_id)
    # Even aliases with identical transport remain separate provider identities;
    # a credential-only operation cannot silently replace frozen provenance.
    provider = {**before["config_snapshot"]["provider"], "id": "new-provider"}
    if different_transport:
        provider.update(base_url="https://new.example.invalid/anthropic", model="new-model")
    store.put("provider", "new-provider", provider)
    secrets["new-provider"] = "sk-SYNTHETIC-NEW-PROVIDER-FIXTURE"

    assert_rejected_unchanged(client, store, research_id, before, provider_config_id="new-provider")
    assert reads == []


@pytest.mark.parametrize("change", [
    {"base_url": "https://new.example.invalid/anthropic"},
    {"model": "new-model"},
])
def test_rebind_rejects_changed_endpoint_or_model_on_same_provider(binding_api, change):
    client, store, secrets, reads = binding_api
    research_id = create_project(client)
    before = store.project(research_id)
    store.patch("provider", "deepseek-default", credential_version=2, **change)
    secrets["deepseek-default"] = "sk-SYNTHETIC-NEW-TARGET-FIXTURE"

    assert_rejected_unchanged(client, store, research_id, before)
    assert reads == []


def test_rebind_rotates_credentials_and_preserves_frozen_settings(binding_api):
    client, store, secrets, reads = binding_api
    research_id = create_project(client)
    frozen = deepcopy(store.project(research_id)["config_snapshot"])
    store.patch("provider", "deepseek-default", credential_version=2, display_name="Renamed display label")
    secrets["deepseek-default"] = "sk-SYNTHETIC-ROTATED-REBIND-FIXTURE"

    response = rebind(client, research_id)
    assert response.status_code == 202, response.text
    current = store.project(research_id)
    assert current["provider_config_id"] == "deepseek-default"
    assert current["credential_version"] == 2
    assert current["config_snapshot"] == frozen
    assert reads == [("deepseek-default", 2)]
    assert response.json()["credential_version"] == 2
    assert response.json()["provider_config_id"] == "deepseek-default"
    assert rebind(client, research_id).json() == response.json()
    assert reads == [("deepseek-default", 2)]
    assert len([event for event in store.events(research_id) if event["type"] == "credential.rebound"]) == 1


@pytest.mark.parametrize("frozen_url,current_url", [
    ("https://api.deepseek.com", "https://api.deepseek.com/anthropic"),
    ("https://proxy.example.invalid/anthropic", "https://proxy.example.invalid/anthropic/v1/"),
])
def test_rebind_allows_equivalent_messages_endpoints(binding_api, frozen_url, current_url):
    client, store, _, reads = binding_api
    store.patch("provider", "deepseek-default", base_url=frozen_url)
    research_id = create_project(client)
    frozen = deepcopy(store.project(research_id)["config_snapshot"])
    store.patch("provider", "deepseek-default", base_url=current_url, credential_version=2)

    response = rebind(client, research_id)
    assert response.status_code == 202, response.text
    assert store.project(research_id)["config_snapshot"] == frozen
    assert store.project(research_id)["credential_version"] == 2
    assert reads == [("deepseek-default", 2)]


def test_rebind_binding_and_audit_roll_back_if_command_receipt_fails(binding_api, monkeypatch):
    client, store, _, _ = binding_api
    research_id = create_project(client)
    before = store.project(research_id)
    store.patch("provider", "deepseek-default", credential_version=2)
    original = store._put

    def fail_command_receipt(connection, kind, object_id, *args, **kwargs):
        if kind == "command":
            raise OSError("Injected command receipt write failure")
        return original(connection, kind, object_id, *args, **kwargs)

    monkeypatch.setattr(store, "_put", fail_command_receipt)
    response = rebind(client, research_id)
    assert response.status_code == 503, response.text
    assert store.project(research_id) == before
    assert store.get("command", "rebind-command") is None
    assert not [event for event in store.events(research_id) if event["type"] == "credential.rebound"]
