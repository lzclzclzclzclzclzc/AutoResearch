import asyncio
import subprocess
import sys
from types import SimpleNamespace

import httpx
import pytest

from backend.adapters.dsh import DshRuntimeAdapter, RuntimeBlocked
from backend.graphs.engine import ResearchEngine
from backend.persistence.store import Store
from backend.runtime.control import ControlSupervisor, ControlUnavailable
from backend.runtime.light_cpu import LightCpuService
from backend.runtime.processes import sanitized_environment
from backend.runtime.recovery import retryable_failure
from backend.runtime.runner import ResourceService, ResourceConflict, create_app
from packages.contracts.research import ExperimentSpec, Implementation, digest


def started(store, name):
    return store.start_project(store.create_project({'topic': name, 'client_request_id': name, 'mode': 'demo'})['id'])


def test_unicode_experiment_logs_do_not_crash(tmp_path):
    run = subprocess.run([sys.executable, '-c', 'print(chr(0x1f600))'], env=sanitized_environment(), capture_output=True)
    assert run.returncode == 0
    assert run.stdout.decode('utf-8').strip() == chr(0x1f600)
