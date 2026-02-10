import os
import threading
import time

import pytest

TEST_DB = "test_pybgworker_concurrency.db"
os.environ["PYBGWORKER_DB"] = TEST_DB
os.environ["PYBGWORKER_CONCURRENCY"] = "2"
os.environ["PYBGWORKER_WORKER_NAME"] = "test-worker"

from pybgworker import task, AsyncResult
import pybgworker.task as task_module
import pybgworker.worker as worker
from pybgworker.backends import SQLiteBackend
from pybgworker.sqlite_queue import SQLiteQueue


class DummyProcess:
    def __init__(self, target, args):
        self._thread = threading.Thread(target=target, args=args)

    def start(self):
        self._thread.start()

    def is_alive(self):
        return self._thread.is_alive()

    def terminate(self):
        return None


@pytest.fixture(autouse=True)
def setup_db():
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)

    task_module.queue = SQLiteQueue()
    task_module.backend = SQLiteBackend()
    worker.queue = SQLiteQueue()
    worker.backend = SQLiteBackend()
    worker.shutdown_requested = False
    worker.last_shutdown_signal = 0
    with worker.active_tasks_lock:
        worker.active_tasks.clear()

    yield

    worker.shutdown_requested = True
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)


@task(name="test.sleep")
def sleep_task(duration):
    time.sleep(duration)
    return "done"


def test_single_worker_concurrency(monkeypatch):
    monkeypatch.setattr(worker, "Process", DummyProcess)
    monkeypatch.setattr(worker.signal, "signal", lambda *args, **kwargs: None)

    result_a = sleep_task.delay(0.7)
    result_b = sleep_task.delay(0.7)

    start = time.time()
    thread = threading.Thread(target=worker.run_worker, daemon=True)
    thread.start()

    res_a = AsyncResult(result_a.task_id)
    res_b = AsyncResult(result_b.task_id)

    deadline = time.time() + 5
    while time.time() < deadline:
        if res_a.ready() and res_b.ready():
            break
        time.sleep(0.05)

    worker.shutdown_requested = True
    thread.join(timeout=2)

    assert res_a.ready() is True
    assert res_b.ready() is True
    assert res_a.successful() is True
    assert res_b.successful() is True

    elapsed = time.time() - start
    assert elapsed < 2.0
