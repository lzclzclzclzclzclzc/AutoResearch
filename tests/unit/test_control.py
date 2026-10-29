import concurrent.futures
import threading
import time
import hashlib
import json
import os

import pytest

from backend.runtime.control import ControlClient, ControlSupervisor, ControlUnavailable, DurableJournal


def _stop_record():
    return {"kind": "stop", "research_id": "old", "record_id": "stop-original", "stop_id": "original",
        "stopped": True, "enabled": False, "control_generation": 2, "stop_seq": 2,
        "stop_requested_at": "2026-10-27T00:00:00+00:00", "reason": "user_requested", "control_epoch": "old-epoch"}


def _journal_file(directory, record, suffix="json"):
    from backend.runtime.control import _canonical
    key = hashlib.sha256(f"{record['research_id']}/{record['record_id']}".encode()).hexdigest()
    path = directory / (f"{key}.json" if suffix == "json" else f"{key}.leftover.tmp")
    wrapper = {"record": record, "sha256": hashlib.sha256(_canonical(record)).hexdigest()}
    path.write_bytes(_canonical(wrapper))
    return path


def test_redundant_stop_temporary_recovers_without_reopening_old_project(tmp_path):
    stop = _stop_record()
    committed = _journal_file(tmp_path, {**stop, "control_epoch": "new-epoch", "previous_control_epoch": "old-epoch"})
    leftover = _journal_file(tmp_path, stop, "tmp")
    original_bytes = leftover.read_bytes()
    control = ControlSupervisor(tmp_path)
    try:
        assert not control.journal_unknown
        assert not control.allows_progress("old")
        assert control.status("old")["stop_id"] == "original"
        assert committed.exists() and not leftover.exists()
        assert (tmp_path / "recovered-temporaries" / leftover.name).read_bytes() == original_bytes
        control.register_project("new", time.time() + 60)
        assert control.allows_progress("new")
    finally:
        control.close()


@pytest.mark.parametrize("problem", ["missing_target", "corrupt", "other_kind", "conflict", "wrong_filename"])
def test_unproven_temporaries_still_close_admission(tmp_path, problem):
    stop = _stop_record()
    if problem != "missing_target":
        _journal_file(tmp_path, stop)
    candidate = {**stop, **({"kind": "confirmation"} if problem == "other_kind" else {}),
        **({"stop_id": "conflicting-stop"} if problem == "conflict" else {})}
    leftover = _journal_file(tmp_path, candidate, "tmp")
    if problem == "corrupt":
        leftover.write_text('{"record":', encoding="utf-8")
    if problem == "wrong_filename":
        leftover = leftover.rename(tmp_path / "wrong-name.tmp")
    control = ControlSupervisor(tmp_path)
    try:
        assert control.journal_unknown and leftover.exists()
        with pytest.raises(ControlUnavailable) as error:
            control.register_project("new", time.time() + 60)
        assert error.value.code == "control_journal_incomplete"
        assert not control.allows_progress("old")
    finally:
        control.close()


def test_windows_sharing_violation_retries_atomic_commit(tmp_path, monkeypatch):
    original = os.replace
    calls = []
    def sharing_then_success(source, target):
        calls.append((source, target))
        if len(calls) <= 2:
            error = PermissionError("temporary Windows sharing violation")
            error.winerror = 32
            raise error
        return original(source, target)
    monkeypatch.setattr(os, "replace", sharing_then_success)
    journal = DurableJournal(tmp_path)
    try:
        journal.append(_stop_record()).result(timeout=2)
        assert len(calls) == 3 and not list(tmp_path.glob("*.tmp"))
        records, unknown = journal.read_all()
        assert not unknown and len(records) == 1
    finally:
        journal.close()


def test_repeated_synced_poll_does_not_rewrite_same_stop(tmp_path, monkeypatch):
    control = ControlSupervisor(tmp_path)
    try:
        stop = control.terminate("old")
        eventually(lambda: control.status("old")["stop_journal_persisted"])
        original, writes = control.journal.append, []
        def count(record):
            writes.append(record)
            return original(record)
        monkeypatch.setattr(control.journal, "append", count)
        for _ in range(10):
            control.mark_db_synced("old", stop["stop_id"], resources_reconciled=True)
        assert len(writes) == 1
        assert control.status("old")["state"] == "terminated"
    finally:
        control.close()


def eventually(predicate, timeout=3):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return
        time.sleep(0.02)
    assert predicate()


def test_stop_is_idempotent_and_receipts_are_ordered(tmp_path):
    control = ControlSupervisor(tmp_path)
    try:
        control.register_project("research", time.time() + 60)
        receipt = control.confirm_operation("research", "verify-1", "abc")
        assert receipt["persisted"]
        assert control.get_confirmation("missing") is None
        assert control.get_confirmation("verify-1")==receipt
        stopped = control.terminate("research")
        assert receipt["control_seq"] < stopped["stop_seq"]
        assert not control.allows_progress("research")
        assert control.terminate("research")["stop_id"] == stopped["stop_id"]
        with pytest.raises(ControlUnavailable):
            control.confirm_operation("research", "verify-2", "def")
        # A valid pre-stop receipt may be recovered for historical application.
        assert control.confirm_operation("research", "verify-1", "abc")["persisted"]
        with pytest.raises(ValueError):
            control.confirm_operation("research", "verify-1", "different")
        eventually(lambda: control.status("research")["stop_journal_persisted"])
    finally:
        control.close()


def test_stop_never_waits_for_confirmation_disk_io(tmp_path):
    class SlowJournal(DurableJournal):
        entered = threading.Event()
        release = threading.Event()

        def _write(self, record):
            if record["kind"] == "confirmation":
                self.entered.set()
                self.release.wait(3)
            return super()._write(record)

    journal = SlowJournal(tmp_path)
    control = ControlSupervisor(journal=journal)
    pool = concurrent.futures.ThreadPoolExecutor()
    try:
        control.register_project("research", time.time() + 60)
        pending = pool.submit(control.confirm_operation, "research", "verify", "hash")
        assert journal.entered.wait(1)
        start = time.monotonic()
        stopped = control.terminate("research")
        assert time.monotonic() - start < 0.2
        assert not control.allows_progress("research")
        assert not stopped["stop_journal_persisted"]
        journal.release.set()
        receipt = pending.result(timeout=2)
        assert receipt["persisted"] and receipt["control_seq"] < stopped["stop_seq"]
    finally:
        journal.release.set()
        pool.shutdown()
        control.close()


def test_deadline_is_independently_enforced(tmp_path):
    control = ControlSupervisor(tmp_path)
    try:
        control.register_project("research", time.time() + 0.15)
        eventually(lambda: control.status("research").get("stop_reason") == "time_limit_reached")
        assert not control.allows_progress("research")
    finally:
        control.close()


def test_control_restart_quarantines_previously_running_project(tmp_path):
    first = ControlSupervisor(tmp_path)
    first.register_project("research", time.time() + 60)
    epoch = first.epoch
    # Simulated hard failure: no graceful shutdown stop record is produced.
    first._shutdown.set()
    first.journal.close()
    second = ControlSupervisor(tmp_path)
    try:
        assert second.epoch != epoch
        assert not second.allows_progress("research")
        assert second.status("research")["reason"] == "control_restarted"
        with pytest.raises(ValueError):
            second.register_project("research", time.time() + 60)
    finally:
        second.close()


def test_corrupt_journal_and_unavailable_client_fail_closed(tmp_path):
    (tmp_path / "broken.json").write_text('{"record":', encoding="utf-8")
    control = ControlSupervisor(tmp_path)
    try:
        assert control.journal_unknown
        with pytest.raises(ControlUnavailable):
            control.register_project("new", time.time() + 60)
        # Unknown transport state still forbids admission, while remaining
        # distinguishable from Control's durable denial/termination decision.
        with pytest.raises(ControlUnavailable) as unavailable:
            ControlClient("http://127.0.0.1:1").allows_progress("new")
        assert unavailable.value.code == "control_unavailable"
    finally:
        control.close()


def test_stop_survives_journal_failure_in_memory(tmp_path):
    control = ControlSupervisor(tmp_path)
    try:
        control.register_project("research", time.time() + 60)
        def fail(record):
            raise OSError("disk full")
        control.journal._write = fail
        control.terminate("research")
        assert not control.allows_progress("research")
        eventually(lambda: bool(control.status("research").get("persistence_error")))
        assert not control.status("research")["stop_journal_persisted"]
    finally:
        control.close()


def test_full_termination_requires_explicit_resource_reconciliation(tmp_path):
    control = ControlSupervisor(tmp_path)
    try:
        control.register_project("research", time.time() + 60)
        stopped = control.terminate("research")
        state = control.mark_db_synced("research", stopped["stop_id"])
        assert state["state"] == "stopped_pending_sync"
        state = control.mark_db_synced("research", stopped["stop_id"], resources_reconciled=True)
        assert state["state"] == "terminated"
    finally:
        control.close()


def test_late_writer_ack_recovers_receipt_after_caller_timeout_and_stop(tmp_path):
    class SlowJournal(DurableJournal):
        entered = threading.Event()
        release = threading.Event()

        def append(self, record):
            future = super().append(record)
            if record["kind"] == "confirmation":
                result = future.result
                # Exercise the same timeout boundary without a five-second test.
                future.result = lambda timeout=None: result(timeout=0.02 if timeout is not None else None)
            return future

        def _write(self, record):
            if record["kind"] == "confirmation":
                self.entered.set()
                self.release.wait(3)
            return super()._write(record)

    journal = SlowJournal(tmp_path)
    control = ControlSupervisor(journal=journal)
    try:
        control.register_project("research", time.time() + 60)
        with pytest.raises(ControlUnavailable):
            control.confirm_operation("research", "verify-late", "hash")
        assert journal.entered.is_set()
        assert control.get_confirmation("verify-late") is None
        stopped = control.terminate("research")
        journal.release.set()
        eventually(lambda: control.get_confirmation("verify-late") is not None)
        receipt = control.get_confirmation("verify-late")
        assert receipt["persisted"] and receipt["control_seq"] < stopped["stop_seq"]
        assert not control.allows_progress("research")
    finally:
        journal.release.set()
        control.close()


@pytest.mark.parametrize("kind", ["agent", "light_cpu", "experiment"])
def test_stop_keeps_pidless_admission_until_supervisor_proves_completion(tmp_path, kind):
    control = ControlSupervisor(tmp_path)
    try:
        control.register_project("research", time.time() + 60)
        control.register_process("research", "pending", kind=kind)
        stopped = control.terminate("research")
        eventually(lambda: control.status("research")["stop_journal_persisted"])
        state = control.mark_db_synced("research", stopped["stop_id"], resources_reconciled=True)
        assert state["state"] == "terminating" and not state["execution_stopped"]
        assert [entry["execution_id"] for entry in state["unfinished"]] == ["pending"]
        control.complete_execution("pending")
        assert control.status("research")["state"] == "terminated"
    finally:
        control.close()
