import asyncio
from types import SimpleNamespace

import pytest

from backend.persistence.store import Store, StoreError
from backend.runtime.control import ControlClient, ControlUnavailable
from backend.runtime.recovery import retryable_failure


def test_unknown_control_admission_is_retryable_but_explicit_denial_stays_denied(monkeypatch):
    control = ControlClient()
    def unavailable(*args, **kwargs):
        raise ControlUnavailable("temporary connection loss")
    monkeypatch.setattr(control, "_post", unavailable)
    with pytest.raises(ControlUnavailable) as error:
        control.allows_progress("research", 1)
    assert retryable_failure(error.value)[0] == "local_service_unavailable"
    assert retryable_failure(ControlUnavailable("corrupt journal", "control_journal_incomplete")) is None
    monkeypatch.setattr(control, "_post", lambda *a, **kw: {"allowed": False})
    assert control.allows_progress("research", 1) is False


@pytest.mark.parametrize("status", [409, 503])
def test_rejected_service_operations_are_not_transport_retries(monkeypatch, status):
    import io
    import urllib.error
    import backend.runtime.control as module
    def error(*args, **kwargs):
        raise urllib.error.HTTPError("http://127.0.0.1", status, "fixture", {}, io.BytesIO(b'{}'))
    monkeypatch.setattr(module.urllib.request, "urlopen", error)
    with pytest.raises(ControlUnavailable) as caught:
        ControlClient().allows_progress("research", 1)
    assert bool(retryable_failure(caught.value)) == (status == 503)
